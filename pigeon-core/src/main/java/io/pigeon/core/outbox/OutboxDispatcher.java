package io.pigeon.core.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

/**
 * Claims PENDING events with SELECT ... FOR UPDATE SKIP LOCKED inside one transaction, publishes them
 * with confirms and updates their state before commit. Safe to run concurrently in many instances.
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
        Integer n = tx.execute(status -> {
            List<OutboxEvent> batch = repository.claimBatch(props.batchSize());
            if (batch.isEmpty()) {
                return 0;
            }
            Set<java.util.UUID> confirmed;
            try {
                confirmed = publisher.publish(batch);
            } catch (RuntimeException e) {
                log.warn("publish batch failed: {}", e.toString());
                confirmed = Set.of();
            }
            OffsetDateTime now = OffsetDateTime.now();
            for (OutboxEvent e : batch) {
                if (confirmed.contains(e.id())) {
                    repository.markSent(e.id());
                    latency.record(Duration.between(e.createdAt(), now));
                    count("sent");
                } else if (e.attempts() + 1 >= props.maxAttempts()) {
                    repository.markDead(e.id());
                    try {
                        publisher.deadLetter(e);
                    } catch (RuntimeException ex) {
                        log.warn("dead-letter publish failed for {}: {}", e.id(), ex.toString());
                    }
                    log.error("outbox event {} DEAD after {} attempts", e.id(), e.attempts() + 1);
                    count("dead");
                } else {
                    repository.markRetry(e.id(), now.plus(backoff(e.attempts() + 1)));
                    count("failed");
                }
            }
            return batch.size();
        });
        return n == null ? 0 : n;
    }

    /** Exponential backoff: base * 2^(attempt-1), capped. */
    Duration backoff(int attempt) {
        long factor = 1L << Math.min(Math.max(attempt - 1, 0), 30);
        long ms = Math.min(props.baseBackoffMs() * factor, props.maxBackoffMs());
        return Duration.ofMillis(ms);
    }

    private void count(String result) {
        registry.counter("pigeon.outbox.dispatched", "result", result).increment();
    }
}
