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
        OutboxRepository.ClaimResult claim = tx.execute(status ->
                repository.claim(props.batchSize(), claimToken, props.claimLeaseMs()));
        if (claim == null || claim.events().isEmpty()) {
            return 0;
        }
        List<OutboxEvent> batch = claim.events();
        log.debug("claimed {} outbox event(s) token={}", batch.size(), claimToken);
        if (claim.reclaimed() > 0) {
            log.warn("reclaimed {} expired PROCESSING event(s)", claim.reclaimed());
        }
        boolean completed = false;
        try {
            PublishBatchResult result;
            try {
                result = publisher.publish(batch);
            } catch (RuntimeException e) {
                log.warn("publish batch failed (treating as transient): {}", e.toString());
                result = PublishBatchResult.all(batch, PublishOutcome.TRANSIENT, "publish batch failed: " + e);
            }
            OffsetDateTime now = OffsetDateTime.now();
            for (OutboxEvent e : batch) {
                PublishOutcome outcome = result.get(e.id());
                String error = result.error(e.id());
                tx.executeWithoutResult(status -> finalizeEvent(e, claimToken, outcome, error, now));
            }
            completed = true;
            return batch.size();
        } finally {
            if (!completed) {
                releaseUnfinished(claimToken);
            }
        }
    }

    /**
     * A batch that ends early (interrupted on shutdown, or the database failed while finalizing) would
     * otherwise leave its remaining rows PROCESSING until the lease expires. Hand them back right away.
     * Best effort: if this fails too, the lease still makes the rows claimable later.
     */
    private void releaseUnfinished(UUID claimToken) {
        boolean interrupted = Thread.interrupted();
        try {
            Integer released = tx.execute(status -> repository.releaseClaim(claimToken));
            if (released != null && released > 0) {
                log.warn("released {} claimed outbox event(s) back to PENDING after an unfinished batch", released);
            }
        } catch (RuntimeException ex) {
            log.warn("could not release claimed outbox events; they become claimable when the lease expires: {}",
                    ex.toString());
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void finalizeEvent(OutboxEvent e, java.util.UUID claimToken,
                               PublishOutcome outcome, String error, OffsetDateTime now) {
        switch (outcome) {
            case CONFIRMED -> {
                int updated = repository.markSent(e.id(), claimToken);
                if (updated > 0) {
                    Duration elapsed = Duration.between(e.createdAt(), now);
                    latency.record(elapsed);
                    count("sent");
                    log.debug("event {} sent type={} attempts={} latencyMs={}",
                            e.id(), e.eventType(), e.attempts() + 1, elapsed.toMillis());
                }
            }
            case TRANSIENT -> {
                int updated = repository.markSoftRetry(e.id(), claimToken,
                        now.plus(jitter(Duration.ofMillis(props.brokerDownBackoffMs()))),
                        errorText(error, outcome));
                if (updated > 0) {
                    count("deferred");
                }
            }
            case PERMANENT -> handlePermanentFailure(e, claimToken, errorText(error, outcome), now);
        }
    }

    private void handlePermanentFailure(OutboxEvent e, UUID claimToken, String error, OffsetDateTime now) {
        if (e.attempts() + 1 >= props.maxAttempts()) {
            int updated = repository.markDead(e.id(), claimToken, error);
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
            Duration delay = jitter(backoff(e.attempts() + 1));
            int updated = repository.markRetry(e.id(), claimToken, now.plus(delay), error);
            if (updated > 0) {
                count("failed");
                log.warn("event {} publish failed (attempt {}/{}), retrying in {} ms",
                        e.id(), e.attempts() + 1, props.maxAttempts(), delay.toMillis());
            }
        }
    }

    Duration backoff(int attempt) {
        long factor = 1L << Math.min(Math.max(attempt - 1, 0), 30);
        long ms = Math.min(props.baseBackoffMs() * factor, props.maxBackoffMs());
        return Duration.ofMillis(ms);
    }

    /**
     * Shortens {@code delay} by a random fraction of at most {@code backoffJitter}, so events that failed
     * together do not all become claimable at the same instant. Never lengthens the delay, so
     * {@code maxBackoffMs} stays an upper bound.
     */
    Duration jitter(Duration delay) {
        double fraction = Math.min(Math.max(props.backoffJitter(), 0.0), 1.0);
        if (fraction == 0.0) {
            return delay;
        }
        double factor = 1.0 - fraction * java.util.concurrent.ThreadLocalRandom.current().nextDouble();
        return Duration.ofMillis(Math.round(delay.toMillis() * factor));
    }

    private static final int MAX_ERROR_LENGTH = 1000;

    private static String errorText(String error, PublishOutcome outcome) {
        String text = error != null ? error : "publish failed (" + outcome + ")";
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }

    private void count(String result) {
        registry.counter("pigeon.outbox.dispatched", "result", result).increment();
    }
}
