package io.pigeon.outbox;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = OutboxSpringBootTest.Application.class)
@Import(OutboxSpringBootTest.RabbitConfig.class)
@Testcontainers
class OutboxSpringBootTest {
    @Autowired JdbcTemplate jdbc;
    @Autowired Outbox outbox;
    @Autowired Flyway flyway;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("pigeon.outbox.scheduling.enabled", () -> "false");
        registry.add("pigeon.outbox.flyway.enabled", () -> "true");
        registry.add("pigeon.outbox.flyway.locations", () -> "classpath:db/pigeon");
        registry.add("pigeon.outbox.flyway.table", () -> "pigeon_schema_history");
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class Application {
    }

    @TestConfiguration
    static class RabbitConfig {
        @Bean
        RabbitTemplate rabbitTemplate() {
            return mock(RabbitTemplate.class);
        }
    }

    @Test
    void autoConfigAndSeparateFlywayMigrationsCreateAppAndOutboxSchema() {
        assertThat(jdbc.queryForObject(
                "select count(*) from information_schema.tables where table_name='app_marker'",
                Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from information_schema.tables where table_name='outbox_events'",
                Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from information_schema.tables where table_name='pigeon_schema_history'",
                Integer.class)).isEqualTo(1);
        assertThat(outbox).isNotNull();
        assertThat(flyway.getConfiguration().getTable()).isEqualTo("flyway_schema_history");
    }
}
