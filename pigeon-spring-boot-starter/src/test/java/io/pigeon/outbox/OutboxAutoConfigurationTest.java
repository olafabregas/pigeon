package io.pigeon.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import javax.sql.DataSource;

class OutboxAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class))
            .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
            .withBean(DataSource.class, () -> mock(DataSource.class))
            .withBean(RabbitTemplate.class, () -> mock(RabbitTemplate.class))
            .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class));

    @Test
    void preservesApplicationMeterRegistry() {
        SimpleMeterRegistry applicationRegistry = new SimpleMeterRegistry();

        contextRunner
                .withBean(MeterRegistry.class, () -> applicationRegistry)
                .run(context -> assertThat(context).hasSingleBean(MeterRegistry.class)
                        .doesNotHaveBean("pigeonFallbackMeterRegistry"));
    }

    @Test
    void createsFallbackOnlyWhenNoRegistryExists() {
        contextRunner.run(context -> assertThat(context).hasBean("pigeonFallbackMeterRegistry"));
    }
}
