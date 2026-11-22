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
    void dispatcherRunsByDefaultWithoutAnySchedulingProperty() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(OutboxScheduler.class);
            assertThat(context.getBean(OutboxScheduler.class).isRunning()).isTrue();
            assertThat(context).doesNotHaveBean(OutboxAutoConfiguration.SchedulingDisabledNotice.class);
        });
    }

    @Test
    void legacyExplicitTrueStillWorks() {
        contextRunner.withPropertyValues("pigeon.outbox.scheduling.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(OutboxScheduler.class));
    }

    @Test
    void schedulingCanBeDisabledForWriteOnlyInstances() {
        contextRunner.withPropertyValues("pigeon.outbox.scheduling.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(OutboxScheduler.class);
                    assertThat(context).hasSingleBean(OutboxAutoConfiguration.SchedulingDisabledNotice.class);
                    assertThat(context).hasSingleBean(Outbox.class);
                });
    }

    @Test
    void masterSwitchDisablesTheDispatcher() {
        contextRunner.withPropertyValues("pigeon.outbox.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(OutboxScheduler.class));
    }

    @Test
    void doesNotEnableSpringSchedulingForTheHostApplication() {
        contextRunner.run(context -> assertThat(context)
                .doesNotHaveBean("org.springframework.context.annotation.internalScheduledAnnotationProcessor"));
    }

    @Test
    void stoppingTheContextStopsTheDispatcher() {
        contextRunner.run(context -> {
            OutboxScheduler scheduler = context.getBean(OutboxScheduler.class);
            assertThat(scheduler.isRunning()).isTrue();
            context.close();
            assertThat(scheduler.isRunning()).isFalse();
        });
    }

    @Test
    void healthIndicatorIsRegisteredByDefault() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(OutboxHealthIndicator.class)
                .hasBean("pigeonOutboxHealthIndicator"));
    }

    @Test
    void healthIndicatorCanBeDisabled() {
        contextRunner.withPropertyValues("pigeon.outbox.health.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(OutboxHealthIndicator.class));
    }

    @Test
    void healthIndicatorOnAWriteOnlyInstanceReportsUp() {
        contextRunner.withPropertyValues("pigeon.outbox.scheduling.enabled=false")
                .run(context -> assertThat(context.getBean(OutboxHealthIndicator.class).health().getStatus())
                        .isEqualTo(org.springframework.boot.actuate.health.Status.UP));
    }

    @Test
    void starterStillWorksWithoutActuatorOnTheClasspath() {
        contextRunner
                .withClassLoader(new org.springframework.boot.test.context.FilteredClassLoader(
                        org.springframework.boot.actuate.health.HealthIndicator.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(Outbox.class);
                    assertThat(context).doesNotHaveBean(OutboxHealthIndicator.class);
                });
    }

    @Test
    void createsFallbackOnlyWhenNoRegistryExists() {
        contextRunner.run(context -> assertThat(context).hasBean("pigeonFallbackMeterRegistry"));
    }
}
