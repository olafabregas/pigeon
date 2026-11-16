package io.pigeon.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.pigeon.outbox.Outbox;
import io.pigeon.outbox.OutboxDispatcher;
import io.pigeon.outbox.OutboxMetrics;
import io.pigeon.outbox.OutboxProperties;
import io.pigeon.outbox.OutboxPublisher;
import io.pigeon.outbox.OutboxRepository;
import io.pigeon.outbox.RabbitOutboxPublisher;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Wires the starter outbox types for the demo app.
 * (Auto-config alone is enough for external apps; this keeps the demo explicit.)
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class DemoOutboxConfig {

    @Bean
    OutboxRepository outboxRepository(JdbcTemplate jdbc) {
        return new OutboxRepository(jdbc);
    }

    @Bean
    Outbox outbox(OutboxRepository repository, ObjectMapper objectMapper) {
        return new Outbox(repository, objectMapper);
    }

    @Bean
    OutboxPublisher outboxPublisher(RabbitTemplate rabbitTemplate, OutboxProperties props) {
        return new RabbitOutboxPublisher(rabbitTemplate, props);
    }

    @Bean
    OutboxDispatcher outboxDispatcher(OutboxRepository repository, OutboxPublisher publisher,
                                      PlatformTransactionManager tm, MeterRegistry registry,
                                      OutboxProperties props) {
        return new OutboxDispatcher(repository, publisher, new TransactionTemplate(tm), registry, props);
    }

    @Bean
    OutboxMetrics outboxMetrics(OutboxRepository repository, MeterRegistry registry) {
        return new OutboxMetrics(repository, registry);
    }

    @Bean
    DemoOutboxScheduler demoOutboxScheduler(OutboxDispatcher dispatcher, OutboxProperties props) {
        return new DemoOutboxScheduler(dispatcher, props);
    }

    public static final class DemoOutboxScheduler {
        private final OutboxDispatcher dispatcher;
        private final OutboxProperties props;

        DemoOutboxScheduler(OutboxDispatcher dispatcher, OutboxProperties props) {
            this.dispatcher = dispatcher;
            this.props = props;
        }

        @Scheduled(fixedDelayString = "${pigeon.outbox.poll-interval-ms:500}")
        public void tick() {
            if (!props.enabled()) {
                return;
            }
            while (dispatcher.dispatchBatch() >= props.batchSize()) {
                // drain
            }
        }
    }
}
