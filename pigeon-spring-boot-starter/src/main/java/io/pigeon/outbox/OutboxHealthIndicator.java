package io.pigeon.outbox;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Actuator health for the outbox: DOWN when the dispatcher is not running, or when the oldest
 * undelivered event is older than {@code pigeon.outbox.health.max-lag-seconds}.
 *
 * <p>Dead events are reported but never make the check fail: they need attention, but the service
 * itself is working. An instance that deliberately does not dispatch
 * ({@code scheduling.enabled=false}) is always UP, since another instance owns publishing.
 */
public final class OutboxHealthIndicator implements HealthIndicator {
    private final OutboxRepository repository;
    private final OutboxProperties props;
    /** Null when this instance does not dispatch. */
    private final OutboxScheduler scheduler;

    OutboxHealthIndicator(OutboxRepository repository, OutboxProperties props, OutboxScheduler scheduler) {
        this.repository = repository;
        this.props = props;
        this.scheduler = scheduler;
    }

    @Override
    public Health health() {
        if (scheduler == null) {
            return Health.up().withDetail("dispatching", "disabled").build();
        }
        try {
            double lagSeconds = repository.oldestPendingAgeSeconds();
            long maxLag = props.health().maxLagSeconds();
            boolean running = scheduler.isRunning();
            boolean lagging = maxLag > 0 && lagSeconds > maxLag;

            Health.Builder builder = running && !lagging ? Health.up() : Health.down();
            builder.withDetail("dispatching", running ? "running" : "stopped")
                    .withDetail("pending", repository.countPending())
                    .withDetail("dead", repository.countByStatus("DEAD"))
                    .withDetail("lagSeconds", Math.round(lagSeconds))
                    .withDetail("maxLagSeconds", maxLag);
            if (!running) {
                builder.withDetail("reason", "the dispatcher is not running");
            } else if (lagging) {
                builder.withDetail("reason", "oldest undelivered event is " + Math.round(lagSeconds)
                        + "s old, over the " + maxLag + "s limit");
            }
            return builder.build();
        } catch (RuntimeException e) {
            return Health.down(e).build();
        }
    }
}
