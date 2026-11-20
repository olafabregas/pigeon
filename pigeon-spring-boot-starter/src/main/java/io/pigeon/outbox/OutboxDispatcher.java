package io.pigeon.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Claims PENDING events with SELECT ... FOR UPDATE SKIP LOCKED, leases them as PROCESSING,
 * publishes after the claim transaction commits, and finalizes each result independently.
 *
 * <p>Broker-down / transient failures soft-retry without burning {@code maxAttempts}.
 * Only permanent failures (nack, unroutable) count toward DEAD.
 */
public class OutboxDispatcher {
    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final OutboxRepository repository;
    private final OutboxPublisher publisher;
    private final TransactionTemplate tx;
    private final MeterRegistry registry;
    private final OutboxProperties props;
    private final Timer latency;

    public OutboxDispatcher(OutboxRepository repository, OutboxPublisher publisher, TransactionTemplate tx,
                            MeterRegistry registry, OutboxProperties props) {
        this.repository = repository;
        this.publisher = publisher;
        this.tx = tx;
        this.registry = registry;
        this.props = props;
        this.latency = Timer.builder("pigeon.outbox.dispatch.latency")
                .description("Time from outbox event creation to confirmed publish")
                .publishPercentileHistogram()
                .register(registry);
    }

    /** @return number of events claimed in this batch */
    public int dispatchBatch() {
        UUID claimToken = UUID.randomUUID();
        List<OutboxEvent> batch = tx.execute(status -> {
            List<OutboxEvent> claimed = repository.claimBatch(props.batchSize(), claimToken, props.claimLeaseMs());
            return claimed;
        });
        if (batch == null || batch.isEmpty()) {
            return 0;
        }
        PublishBatchResult result;
        try {
            result = publisher.publish(batch);
        } catch (RuntimeException e) {
            log.warn("publish batch failed (treating as transient): {}", e.toString());
            result = PublishBatchResult.all(batch, PublishOutcome.TRANSIENT);
        }
        OffsetDateTime now = OffsetDateTime.now();
        for (OutboxEvent e : batch) {
            PublishOutcome outcome = result.get(e.id());
            tx.executeWithoutResult(status -> finalizeEvent(e, claimToken, outcome, now));
        }
        return batch.size();
    }

    private void finalizeEvent(OutboxEvent e, java.util.UUID claimToken,
                               PublishOutcome outcome, OffsetDateTime now) {
        switch (outcome) {
            case CONFIRMED -> {
                int updated = repository.markSent(e.id(), claimToken);
                if (updated > 0) {
                    latency.record(Duration.between(e.createdAt(), now));
                    count("sent");
                }
            }
            case TRANSIENT -> {
                int updated = repository.markSoftRetry(e.id(), claimToken,
                        now.plus(Duration.ofMillis(props.brokerDownBackoffMs())));
                if (updated > 0) {
                    count("deferred");
                }
            }
            case PERMANENT -> handlePermanentFailure(e, claimToken, now);
        }
    }

    private void handlePermanentFailure(OutboxEvent e, UUID claimToken, OffsetDateTime now) {
        if (e.attempts() + 1 >= props.maxAttempts()) {
            int updated = repository.markDead(e.id(), claimToken);
            if (updated > 0) {
                try {
                    publisher.deadLetter(e);
                } catch (RuntimeException ex) {
                    log.warn("dead-letter publish failed for {}: {}", e.id(), ex.toString());
                }
                log.error("outbox event {} DEAD after {} attempts (permanent publish failure)",
                        e.id(), e.attempts() + 1);
                count("dead");
            }
        } else {
            int updated = repository.markRetry(e.id(), claimToken, now.plus(backoff(e.attempts() + 1)));
            if (updated > 0) {
                count("failed");
            }
        }
    }

    Duration backoff(int attempt) {
        long factor = 1L << Math.min(Math.max(attempt - 1, 0), 30);
        long ms = Math.min(props.baseBackoffMs() * factor, props.maxBackoffMs());
        return Duration.ofMillis(ms);
    }

    private void count(String result) {
        registry.counter("pigeon.outbox.dispatched", "result", result).increment();
    }
}
