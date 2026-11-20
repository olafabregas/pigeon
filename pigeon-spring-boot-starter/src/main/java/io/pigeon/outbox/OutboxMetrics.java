package io.pigeon.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/** Registers outbox gauges scraped by Prometheus. */
public final class OutboxMetrics {

    public OutboxMetrics(OutboxRepository repo, MeterRegistry registry) {
        Gauge.builder("pigeon.outbox.pending", () -> safe(() -> repo.countPending()))
                .description("PENDING and PROCESSING outbox events")
                .register(registry);
        Gauge.builder("pigeon.outbox.dead", () -> safe(() -> repo.countByStatus("DEAD")))
                .description("DEAD outbox events")
                .register(registry);
        Gauge.builder("pigeon.outbox.lag.seconds", () -> safe(repo::oldestPendingAgeSeconds))
                .description("Age of oldest PENDING or PROCESSING outbox event")
                .baseUnit("seconds")
                .register(registry);
    }

    private static double safe(java.util.function.Supplier<Number> s) {
        try {
            return s.get().doubleValue();
        } catch (Exception e) {
            return Double.NaN;
        }
    }
}
