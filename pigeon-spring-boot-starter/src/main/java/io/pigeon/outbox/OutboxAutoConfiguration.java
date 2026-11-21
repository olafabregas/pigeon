package io.pigeon.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "pigeon.outbox.scheduling.enabled", havingValue = "true")
@EnableScheduling
class PigeonSchedulingConfiguration {
}

@AutoConfiguration(after = {
        DataSourceAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class,
        RabbitAutoConfiguration.class
}, afterName = "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration")
@ConditionalOnClass({DataSource.class, JdbcTemplate.class, RabbitTemplate.class})
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(OutboxProperties.class)
@Import(PigeonSchedulingConfiguration.class)
public class OutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    OutboxRepository outboxRepository(JdbcTemplate jdbcTemplate) {
        return new OutboxRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    Outbox outbox(OutboxRepository repository, ObjectProvider<ObjectMapper> objectMapper) {
        return new Outbox(repository, objectMapper.getIfAvailable(ObjectMapper::new));
    }

    @Bean
    @ConditionalOnMissingBean(MeterRegistry.class)
    MeterRegistry pigeonFallbackMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(RabbitTemplate.class)
    OutboxPublisher outboxPublisher(RabbitTemplate rabbitTemplate, OutboxProperties props) {
        return new RabbitOutboxPublisher(rabbitTemplate, props);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(OutboxPublisher.class)
    OutboxDispatcher outboxDispatcher(OutboxRepository repository, OutboxPublisher publisher,
                                      PlatformTransactionManager tm, MeterRegistry registry,
                                      OutboxProperties props) {
        return new OutboxDispatcher(repository, publisher, new TransactionTemplate(tm), registry, props);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxMetrics outboxMetrics(OutboxRepository repository, MeterRegistry registry) {
        return new OutboxMetrics(repository, registry);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxCleanup outboxCleanup(OutboxRepository repository, OutboxProperties props) {
        return new OutboxCleanup(repository, props);
    }

    @Bean
    @ConditionalOnProperty(name = "pigeon.outbox.flyway.enabled", havingValue = "true")
    PigeonFlywayMigration pigeonFlywayMigration(DataSource dataSource, OutboxProperties props) {
        return new PigeonFlywayMigration(dataSource, props);
    }

    @Bean
    @ConditionalOnMissingBean(name = "pigeonEventsExchange")
    TopicExchange pigeonEventsExchange(OutboxProperties props) {
        return ExchangeBuilder.topicExchange(props.exchange()).durable(true).build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "pigeonDeadLetterExchange")
    TopicExchange pigeonDeadLetterExchange(OutboxProperties props) {
        return ExchangeBuilder.topicExchange(props.deadLetterExchange()).durable(true).build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "pigeonDeadLetterQueue")
    Queue pigeonDeadLetterQueue(OutboxProperties props) {
        return QueueBuilder.durable(props.deadLetterQueue()).build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "pigeonDlqBinding")
    Binding pigeonDlqBinding(Queue pigeonDeadLetterQueue, TopicExchange pigeonDeadLetterExchange) {
        return BindingBuilder.bind(pigeonDeadLetterQueue).to(pigeonDeadLetterExchange).with("dead.#");
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "pigeon.outbox.enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnBean(OutboxDispatcher.class)
    OutboxScheduler outboxScheduler(OutboxDispatcher dispatcher, OutboxProperties props,
                                    org.springframework.core.env.Environment environment) {
        boolean schedulingEnabled = environment.getProperty("pigeon.outbox.scheduling.enabled", Boolean.class, false);
        return new OutboxScheduler(dispatcher, props, schedulingEnabled);
    }

    public static final class OutboxScheduler {
        private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);
        private final OutboxDispatcher dispatcher;
        private final OutboxProperties props;

        OutboxScheduler(OutboxDispatcher dispatcher, OutboxProperties props, boolean schedulingEnabled) {
            this.dispatcher = dispatcher;
            this.props = props;
            if (schedulingEnabled) {
                log.info("pigeon outbox started: exchange={} batchSize={} pollIntervalMs={}",
                        props.exchange(), props.batchSize(), props.pollIntervalMs());
            } else {
                log.warn("pigeon outbox is NOT dispatching: set pigeon.outbox.scheduling.enabled=true");
            }
        }

        @Scheduled(fixedDelayString = "${pigeon.outbox.poll-interval-ms:500}")
        public void tick() {
            try {
                while (dispatcher.dispatchBatch() >= props.batchSize()) {
                    // keep draining full batches
                }
            } catch (Exception e) {
                log.warn("outbox dispatch tick failed: {}", e.toString());
            }
        }
    }

}
