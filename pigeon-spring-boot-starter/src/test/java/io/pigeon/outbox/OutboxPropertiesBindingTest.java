package io.pigeon.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxPropertiesBindingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(Config.class);

    @Test
    void flywayKeysBindToNestedRecord() {
        runner.withPropertyValues(
                        "pigeon.outbox.flyway.enabled=true",
                        "pigeon.outbox.flyway.locations=classpath:custom/pigeon",
                        "pigeon.outbox.flyway.table=custom_history")
                .run(ctx -> {
                    OutboxProperties props = ctx.getBean(OutboxProperties.class);
                    assertThat(props.flyway().enabled()).isTrue();
                    assertThat(props.flyway().locations()).isEqualTo("classpath:custom/pigeon");
                    assertThat(props.flyway().table()).isEqualTo("custom_history");
                });
    }

    @Test
    void deadRetentionBindsAndKeepsDeadRowsForeverByDefault() {
        runner.run(ctx -> assertThat(ctx.getBean(OutboxProperties.class).deadRetentionMs()).isEqualTo(-1L));
        runner.withPropertyValues("pigeon.outbox.dead-retention-ms=86400000")
                .run(ctx -> assertThat(ctx.getBean(OutboxProperties.class).deadRetentionMs()).isEqualTo(86_400_000L));
    }

    @Test
    void flywayIsDisabledByDefault() {
        runner.run(ctx -> assertThat(ctx.getBean(OutboxProperties.class).flyway().enabled()).isFalse());
    }

    @EnableConfigurationProperties(OutboxProperties.class)
    static class Config {
    }
}
