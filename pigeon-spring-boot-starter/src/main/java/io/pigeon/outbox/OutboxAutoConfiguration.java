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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

@AutoConfiguration(after = {
        DataSourceAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class,
        RabbitAutoConfiguration.class
}, afterName = "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration")
@ConditionalOnClass({DataSource.class, JdbcTemplate.class, RabbitTemplate.class})
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(OutboxProperties.class)
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

    /**
     * Runs the dispatcher. On by default; set {@code pigeon.outbox.scheduling.enabled=false} on instances
     * that should only write events and leave publishing to other instances.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = {"pigeon.outbox.enabled", "pigeon.outbox.scheduling.enabled"},
            havingValue = "true", matchIfMissing = true)
    @ConditionalOnBean(OutboxDispatcher.class)
    OutboxScheduler outboxScheduler(OutboxDispatcher dispatcher, OutboxCleanup cleanup, OutboxProperties props) {
        return new OutboxScheduler(dispatcher, cleanup, props);
    }

    @Bean
    @ConditionalOnProperty(name = "pigeon.outbox.scheduling.enabled", havingValue = "false")
    SchedulingDisabledNotice pigeonSchedulingDisabledNotice() {
        return new SchedulingDisabledNotice();
    }

    /**
     * Only loaded when Spring Boot Actuator is on the classpath, so the starter still works without it.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    @ConditionalOnProperty(name = "pigeon.outbox.health.enabled", havingValue = "true", matchIfMissing = true)
    static class PigeonHealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "pigeonOutboxHealthIndicator")
        OutboxHealthIndicator pigeonOutboxHealthIndicator(OutboxRepository repository, OutboxProperties props,
                                                          ObjectProvider<OutboxScheduler> scheduler) {
            return new OutboxHealthIndicator(repository, props, scheduler.getIfAvailable());
        }
    }

    /** Makes a deliberately write-only instance visible in the logs. */
    static final class SchedulingDisabledNotice {
        private static final Logger log = LoggerFactory.getLogger(SchedulingDisabledNotice.class);

        SchedulingDisabledNotice() {
            log.info("pigeon outbox dispatching is disabled (pigeon.outbox.scheduling.enabled=false); "
                    + "this instance writes events but does not publish them");
        }
    }
}
