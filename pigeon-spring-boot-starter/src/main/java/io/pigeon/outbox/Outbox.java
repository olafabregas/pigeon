package io.pigeon.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.UUID;

/**
 * Public API: call {@link #write} inside the same database transaction as your domain changes.
 * Use {@link #replayDead} / {@link #replayAllDead} to requeue exhausted events after an outage.
 */
public class Outbox {
    private final OutboxRepository repository;
    private final ObjectMapper objectMapper;

    public Outbox(OutboxRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    /** Inserts a PENDING outbox row. Must run inside the caller's transaction. */
    public UUID write(String aggregateType, String aggregateId, String eventType, String payloadJson) {
        return repository.insert(aggregateType, aggregateId, eventType, payloadJson);
    }

    /** Serializes {@code payload} to JSON and inserts a PENDING outbox row. */
    public UUID write(String aggregateType, String aggregateId, String eventType, Object payload) {
        try {
            return write(aggregateType, aggregateId, eventType, objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("failed to serialize outbox payload", e);
        }
    }

    /** Requeue one DEAD event as PENDING (attempts reset). Returns true if a row was updated. */
    public boolean replayDead(UUID id) {
        return repository.replayDead(id) > 0;
    }

    /** Requeue all DEAD events as PENDING. Returns how many rows were updated. */
    public int replayAllDead() {
        return repository.replayAllDead();
    }
}
