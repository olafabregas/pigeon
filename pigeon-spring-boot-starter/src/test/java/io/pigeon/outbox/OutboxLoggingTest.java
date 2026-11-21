package io.pigeon.outbox;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the log lines operators rely on: what is logged, at which level, and with which values. */
@Testcontainers
class OutboxLoggingTest {

    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine");

    static JdbcTemplate jdbc;
    static DataSourceTransactionManager tm;

    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private Logger pigeonLogger;
    private Level previousLevel;

    @BeforeAll
    static void init() {
        Flyway.configure()
                .dataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())
                .locations("classpath:db/pigeon")
                .load()
                .migrate();
        DriverManagerDataSource ds = new DriverManagerDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        jdbc = new JdbcTemplate(ds);
        tm = new DataSourceTransactionManager(ds);
    }

    @BeforeEach
    void attachAppender() {
        jdbc.update("DELETE FROM outbox_events");
        pigeonLogger = (Logger) LoggerFactory.getLogger("io.pigeon");
        previousLevel = pigeonLogger.getLevel();
        pigeonLogger.setLevel(Level.DEBUG);
        appender.start();
        pigeonLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        pigeonLogger.detachAppender(appender);
        pigeonLogger.setLevel(previousLevel);
        appender.stop();
        appender.list.clear();
    }

    private OutboxDispatcher dispatcher(PublishOutcome outcome, int maxAttempts) {
        OutboxPublisher publisher = new OutboxPublisher() {
            public PublishBatchResult publish(List<OutboxEvent> events) {
                return PublishBatchResult.all(events, outcome);
            }

            public void deadLetter(OutboxEvent event) {
            }
        };
        OutboxProperties props = new OutboxProperties(true, 10, 100, maxAttempts, 1, 5, 0.0, 1000, 5,
                604_800_000, -1, 3_600_000, 60_000,
                new OutboxProperties.Flyway(false, "classpath:db/pigeon", "pigeon_schema_history"),
                "pigeon.events", "pigeon.dlx", "pigeon.dlq");
        return new OutboxDispatcher(new OutboxRepository(jdbc), publisher, new TransactionTemplate(tm),
                new SimpleMeterRegistry(), props);
    }

    private List<ILoggingEvent> logs() {
        return List.copyOf(appender.list);
    }

    private boolean hasLog(Level level, String contains) {
        return logs().stream().anyMatch(e -> e.getLevel() == level && e.getFormattedMessage().contains(contains));
    }

    @Test
    void successPathLogsClaimAndSentAtDebugOnly() {
        UUID id = new OutboxRepository(jdbc).insert("order", "o1", "order.created", "{}");

        dispatcher(PublishOutcome.CONFIRMED, 3).dispatchBatch();

        assertThat(hasLog(Level.DEBUG, "claimed 1 outbox event(s) token=")).isTrue();
        assertThat(hasLog(Level.DEBUG, "event " + id + " sent type=order.created attempts=1 latencyMs=")).isTrue();
        assertThat(logs()).noneMatch(e -> e.getLevel().isGreaterOrEqual(Level.INFO));
    }

    @Test
    void successPathIsSilentAtInfo() {
        pigeonLogger.setLevel(Level.INFO);
        new OutboxRepository(jdbc).insert("order", "o2", "order.created", "{}");

        dispatcher(PublishOutcome.CONFIRMED, 3).dispatchBatch();

        assertThat(logs()).isEmpty();
    }

    @Test
    void permanentFailureWithBackoffWarnsWithAttemptAndDelay() {
        UUID id = new OutboxRepository(jdbc).insert("order", "o3", "order.created", "{}");

        dispatcher(PublishOutcome.PERMANENT, 3).dispatchBatch();

        assertThat(hasLog(Level.WARN, "event " + id + " publish failed (attempt 1/3), retrying in 1 ms")).isTrue();
    }

    @Test
    void finalPermanentFailureLogsDeadNotRetry() {
        UUID id = new OutboxRepository(jdbc).insert("order", "o4", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET attempts=2 WHERE id=?", id);

        dispatcher(PublishOutcome.PERMANENT, 3).dispatchBatch();

        assertThat(hasLog(Level.ERROR, "outbox event " + id + " DEAD after 3 attempts")).isTrue();
        assertThat(logs()).noneMatch(e -> e.getFormattedMessage().contains("retrying in"));
    }

    @Test
    void expiredLeaseIsReportedAsReclaimed() {
        UUID id = new OutboxRepository(jdbc).insert("order", "o5", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET status='PROCESSING', claim_token=?, "
                + "claimed_at=now() - interval '10 minutes' WHERE id=?", UUID.randomUUID(), id);

        dispatcher(PublishOutcome.CONFIRMED, 3).dispatchBatch();

        assertThat(hasLog(Level.WARN, "reclaimed 1 expired PROCESSING event(s)")).isTrue();
    }

    @Test
    void freshPendingRowsAreNotReportedAsReclaimed() {
        new OutboxRepository(jdbc).insert("order", "o6", "order.created", "{}");

        dispatcher(PublishOutcome.CONFIRMED, 3).dispatchBatch();

        assertThat(logs()).noneMatch(e -> e.getFormattedMessage().contains("reclaimed"));
    }

    @Test
    void replayDeadLogsAtInfo() {
        UUID id = new OutboxRepository(jdbc).insert("order", "o7", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET status='DEAD', attempts=8 WHERE id=?", id);
        Outbox outbox = new Outbox(new OutboxRepository(jdbc), new com.fasterxml.jackson.databind.ObjectMapper());

        assertThat(outbox.replayDead(id)).isTrue();

        assertThat(hasLog(Level.INFO, "replayed dead event " + id)).isTrue();
    }

    @Test
    void replayDeadOfUnknownEventLogsNothing() {
        Outbox outbox = new Outbox(new OutboxRepository(jdbc), new com.fasterxml.jackson.databind.ObjectMapper());

        assertThat(outbox.replayDead(UUID.randomUUID())).isFalse();

        assertThat(logs()).isEmpty();
    }

    @Test
    void replayAllDeadLogsCountAtInfo() {
        OutboxRepository repo = new OutboxRepository(jdbc);
        for (int i = 0; i < 2; i++) {
            UUID id = repo.insert("order", "d" + i, "order.created", "{}");
            jdbc.update("UPDATE outbox_events SET status='DEAD' WHERE id=?", id);
        }
        Outbox outbox = new Outbox(repo, new com.fasterxml.jackson.databind.ObjectMapper());

        assertThat(outbox.replayAllDead()).isEqualTo(2);

        assertThat(hasLog(Level.INFO, "replayed 2 dead event(s)")).isTrue();
    }

    @Test
    void schedulerLogsStartupAndShutdown() {
        OutboxScheduler scheduler = new OutboxScheduler(
                org.mockito.Mockito.mock(OutboxDispatcher.class),
                org.mockito.Mockito.mock(OutboxCleanup.class),
                OutboxProperties.defaults());

        scheduler.start();
        scheduler.stop();

        assertThat(hasLog(Level.INFO,
                "pigeon outbox started: exchange=pigeon.events batchSize=50 pollIntervalMs=500")).isTrue();
        assertThat(hasLog(Level.INFO, "pigeon outbox stopped")).isTrue();
    }
}
