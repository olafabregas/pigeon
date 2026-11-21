package io.pigeon.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

/**
 * Public API: call {@link #write} inside the same database transaction as your domain changes.
 * Use {@link #replayDead} / {@link #replayAllDead} to requeue exhausted events after an outage.
 */
public class Outbox {
    private static final Logger log = LoggerFactory.getLogger(Outbox.class);

    private final OutboxRepository repository;
    private final ObjectMapper objectMapper;

    public Outbox(OutboxRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    /**
     * Inserts a PENDING outbox row whose payload is the given <b>JSON text</b>, stored as-is.
     * Must run inside the caller's transaction.
     *
     * <p>The string must already be valid JSON (an object, array, or scalar such as {@code "\"text\""}).
     * It is checked before anything is written, so a mistake fails here with a clear message instead of
     * as a database error that aborts your whole transaction. To have Pigeon serialize a Java object,
     * pass the object itself; to send a plain string as a JSON string, pass it typed as {@code Object}
     * or quote it yourself.
     *
     * @throws IllegalStateException    if no database transaction is active
     * @throws IllegalArgumentException if {@code payloadJson} is null, empty, or not valid JSON
     */
    public UUID write(String aggregateType, String aggregateId, String eventType, String payloadJson) {
        requireTransaction();
        requireValidJson(payloadJson);
        return repository.insert(aggregateType, aggregateId, eventType, payloadJson);
    }

    /**
     * Serializes {@code payload} to JSON with Jackson and inserts a PENDING outbox row.
     * Must run inside the caller's transaction.
     *
     * <p>Note that Java picks {@link #write(String, String, String, String)} when the payload's static
     * type is {@code String}; that overload expects JSON text, not a value to be serialized.
     *
     * @throws IllegalStateException    if no database transaction is active
     * @throws IllegalArgumentException if the payload cannot be serialized
     */
    public UUID write(String aggregateType, String aggregateId, String eventType, Object payload) {
        requireTransaction();
        String json;
        try {
            json = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("failed to serialize outbox payload", e);
        }
        return repository.insert(aggregateType, aggregateId, eventType, json);
    }

    private static void requireTransaction() {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Outbox.write must be called inside an active database transaction");
        }
    }

    /** Rejects text PostgreSQL would refuse as jsonb, without echoing the payload (it may be sensitive). */
    private void requireValidJson(String payloadJson) {
        if (payloadJson == null) {
            throw new IllegalArgumentException("outbox payloadJson must not be null");
        }
        JsonNode node;
        try {
            node = objectMapper.readerFor(JsonNode.class)
                    .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readTree(payloadJson);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "outbox payloadJson is not valid JSON: " + e.getOriginalMessage()
                            + " (to serialize a Java object, pass the object instead of a String)");
        }
        if (node == null || node.isMissingNode()) {
            throw new IllegalArgumentException("outbox payloadJson is empty; expected a JSON value");
        }
    }

    /** Requeue one DEAD event as PENDING (attempts reset). Returns true if a row was updated. */
    public boolean replayDead(UUID id) {
        boolean replayed = repository.replayDead(id) > 0;
        if (replayed) {
            log.info("replayed dead event {}", id);
        }
        return replayed;
    }

    /** Requeue all DEAD events as PENDING. Returns how many rows were updated. */
    public int replayAllDead() {
        int replayed = repository.replayAllDead();
        log.info("replayed {} dead event(s)", replayed);
        return replayed;
    }
}
