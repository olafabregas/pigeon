package io.pigeon.core.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.pigeon.core.outbox.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
public class OutboxConfig {

    @Bean
    OutboxDispatcher outboxDispatcher(OutboxRepository repo, OutboxPublisher publisher,
                                      PlatformTransactionManager tm, MeterRegistry registry, OutboxProperties props) {
        return new OutboxDispatcher(repo, publisher, new TransactionTemplate(tm), registry, props);
    }

    @Bean
    OutboxMetrics outboxMetrics(OutboxRepository repo, MeterRegistry registry) {
        return new OutboxMetrics(repo, registry);
    }

    @Component
    @ConditionalOnProperty(name = "pigeon.outbox.enabled", havingValue = "true", matchIfMissing = true)
    static class OutboxScheduler {
        private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);
        private final OutboxDispatcher dispatcher;
        private final OutboxProperties props;

        OutboxScheduler(OutboxDispatcher dispatcher, OutboxProperties props) {
            this.dispatcher = dispatcher;
            this.props = props;
        }

        @Scheduled(fixedDelayString = "${pigeon.outbox.poll-interval-ms:500}")
        void tick() {
            try {
                // drain while full batches keep coming
                while (dispatcher.dispatchBatch() >= props.batchSize()) {
                    // continue
                }
            } catch (Exception e) {
                log.warn("outbox dispatch tick failed: {}", e.toString());
            }
        }
    }

    static class OutboxMetrics {
        OutboxMetrics(OutboxRepository repo, MeterRegistry registry) {
            Gauge.builder("pigeon.outbox.pending", () -> safe(() -> repo.countByStatus("PENDING")))
                    .description("PENDING outbox events").register(registry);
            Gauge.builder("pigeon.outbox.dead", () -> safe(() -> repo.countByStatus("DEAD")))
                    .description("DEAD outbox events").register(registry);
            Gauge.builder("pigeon.outbox.lag.seconds", () -> safe(repo::oldestPendingAgeSeconds))
                    .description("Age of oldest PENDING outbox event").baseUnit("seconds").register(registry);
        }

        private static double safe(java.util.function.Supplier<Number> s) {
            try {
                return s.get().doubleValue();
            } catch (Exception e) {
                return Double.NaN;
            }
        }
    }
}
