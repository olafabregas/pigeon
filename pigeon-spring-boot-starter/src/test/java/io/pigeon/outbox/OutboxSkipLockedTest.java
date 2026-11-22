package io.pigeon.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class OutboxSkipLockedTest {

    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine");

    static JdbcTemplate jdbc;
    static DataSourceTransactionManager tm;

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

    static class RecordingPublisher implements OutboxPublisher {
        final List<UUID> published = Collections.synchronizedList(new ArrayList<>());

        @Override
        public PublishBatchResult publish(List<OutboxEvent> events) {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            Map<UUID, PublishOutcome> outcomes = new HashMap<>();
            for (OutboxEvent e : events) {
                published.add(e.id());
                outcomes.put(e.id(), PublishOutcome.CONFIRMED);
            }
            return PublishBatchResult.of(outcomes);
        }

        @Override
        public void deadLetter(OutboxEvent event) {
        }
    }

    private OutboxDispatcher dispatcher(OutboxPublisher p, OutboxProperties props) {
        return new OutboxDispatcher(new OutboxRepository(jdbc), p, new TransactionTemplate(tm),
                new SimpleMeterRegistry(), props);
    }

    private static OutboxProperties props(int batch, int maxAttempts) {
        return new OutboxProperties(true, batch, 100, maxAttempts, 1, 5, 0.0, 1000, 5,
                604_800_000, -1, 3_600_000,
                60_000,
                new OutboxProperties.Flyway(false, "classpath:db/pigeon", "pigeon_schema_history"),
                new OutboxProperties.Health(true, 300),
                "pigeon.events", "pigeon.dlx", "pigeon.dlq");
    }

    @Test
    void writeRequiresAnActiveTransaction() {
        Outbox outbox = new Outbox(new OutboxRepository(jdbc), new com.fasterxml.jackson.databind.ObjectMapper());

        assertThat(org.assertj.core.api.Assertions.catchThrowable(
                () -> outbox.write("order", "outside-tx", "order.created", "{}")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("active database transaction");
    }

    @Test
    void twoConcurrentDispatchersNeverClaimSameEvent() throws Exception {
        jdbc.update("DELETE FROM outbox_events");
        OutboxRepository repo = new OutboxRepository(jdbc);
        int total = 200;
        for (int i = 0; i < total; i++) {
            repo.insert("order", "o" + i, "order.created", "{\"n\":" + i + "}");
        }
        RecordingPublisher pub = new RecordingPublisher();
        OutboxProperties cfg = props(10, 5);
        OutboxDispatcher d1 = dispatcher(pub, cfg);
        OutboxDispatcher d2 = dispatcher(pub, cfg);

        ExecutorService ex = Executors.newFixedThreadPool(2);
        CyclicBarrier start = new CyclicBarrier(2);
        List<Future<Integer>> fs = new ArrayList<>();
        for (OutboxDispatcher d : List.of(d1, d2)) {
            fs.add(ex.submit(() -> {
                start.await();
                int n = 0, c;
                while ((c = d.dispatchBatch()) > 0) {
                    n += c;
                }
                return n;
            }));
        }
        int n1 = fs.get(0).get(60, TimeUnit.SECONDS);
        int n2 = fs.get(1).get(60, TimeUnit.SECONDS);
        ex.shutdown();

        assertThat(n1 + n2).isEqualTo(total);
        assertThat(n1).isPositive();
        assertThat(n2).isPositive();
        assertThat(new HashSet<>(pub.published)).hasSize(total);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE status='SENT'", Integer.class))
                .isEqualTo(total);
    }

    @Test
    void permanentFailuresBackOffThenGoDead() {
        jdbc.update("DELETE FROM outbox_events");
        new OutboxRepository(jdbc).insert("order", "x", "order.created", "{}");
        OutboxPublisher failing = new OutboxPublisher() {
            public PublishBatchResult publish(List<OutboxEvent> e) {
                return PublishBatchResult.all(e, PublishOutcome.PERMANENT);
            }

            public void deadLetter(OutboxEvent e) {
            }
        };
        OutboxDispatcher d = dispatcher(failing, props(10, 3));
        for (int i = 0; i < 3; i++) {
            d.dispatchBatch();
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {
            }
        }
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events", String.class)).isEqualTo("DEAD");
        assertThat(jdbc.queryForObject("SELECT attempts FROM outbox_events", Integer.class)).isEqualTo(3);
    }

    @Test
    void transientFailuresDoNotBurnAttemptsOrGoDead() {
        jdbc.update("DELETE FROM outbox_events");
        new OutboxRepository(jdbc).insert("order", "y", "order.created", "{}");
        OutboxPublisher deferred = new OutboxPublisher() {
            public PublishBatchResult publish(List<OutboxEvent> e) {
                return PublishBatchResult.all(e, PublishOutcome.TRANSIENT);
            }

            public void deadLetter(OutboxEvent e) {
            }
        };
        OutboxDispatcher d = dispatcher(deferred, props(10, 3));
        for (int i = 0; i < 5; i++) {
            d.dispatchBatch();
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {
            }
        }
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events", String.class)).isEqualTo("PENDING");
        assertThat(jdbc.queryForObject("SELECT attempts FROM outbox_events", Integer.class)).isEqualTo(0);
    }

    @Test
    void replayDeadResetsToPending() {
        jdbc.update("DELETE FROM outbox_events");
        UUID id = new OutboxRepository(jdbc).insert("order", "z", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET status='DEAD', attempts=8 WHERE id=?", id);
        Outbox outbox = new Outbox(new OutboxRepository(jdbc), new com.fasterxml.jackson.databind.ObjectMapper());
        assertThat(outbox.replayDead(id)).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("PENDING");
        assertThat(jdbc.queryForObject("SELECT attempts FROM outbox_events WHERE id=?", Integer.class, id))
                .isEqualTo(0);
    }

    private static OutboxPublisher failingWith(PublishOutcome outcome, String error) {
        return new OutboxPublisher() {
            public PublishBatchResult publish(List<OutboxEvent> events) {
                return PublishBatchResult.all(events, outcome, error);
            }

            public void deadLetter(OutboxEvent event) {
            }
        };
    }

    private static String lastError(UUID id) {
        return jdbc.queryForObject("SELECT last_error FROM outbox_events WHERE id=?", String.class, id);
    }

    @Test
    void permanentFailureRecordsLastErrorAndKeepsEventPending() {
        jdbc.update("DELETE FROM outbox_events");
        UUID id = new OutboxRepository(jdbc).insert("order", "le1", "order.created", "{}");

        dispatcher(failingWith(PublishOutcome.PERMANENT, "broker nacked the publish"), props(10, 3)).dispatchBatch();

        assertThat(lastError(id)).isEqualTo("broker nacked the publish");
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("PENDING");
    }

    @Test
    void transientFailureRecordsLastErrorWithoutBurningAttempts() {
        jdbc.update("DELETE FROM outbox_events");
        UUID id = new OutboxRepository(jdbc).insert("order", "le2", "order.created", "{}");

        dispatcher(failingWith(PublishOutcome.TRANSIENT, "broker unreachable"), props(10, 3)).dispatchBatch();

        assertThat(lastError(id)).isEqualTo("broker unreachable");
        assertThat(jdbc.queryForObject("SELECT attempts FROM outbox_events WHERE id=?", Integer.class, id))
                .isEqualTo(0);
    }

    @Test
    void deadEventExplainsWhyAndErrorSurvivesReplayUntilDelivered() {
        jdbc.update("DELETE FROM outbox_events");
        OutboxRepository repo = new OutboxRepository(jdbc);
        UUID id = repo.insert("order", "le3", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET attempts=2 WHERE id=?", id);

        dispatcher(failingWith(PublishOutcome.PERMANENT, "unroutable: NO_ROUTE"), props(10, 3)).dispatchBatch();

        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("DEAD");
        assertThat(lastError(id)).isEqualTo("unroutable: NO_ROUTE");

        new Outbox(repo, new com.fasterxml.jackson.databind.ObjectMapper()).replayDead(id);
        assertThat(lastError(id)).isEqualTo("unroutable: NO_ROUTE");

        dispatcher(new RecordingPublisher(), props(10, 3)).dispatchBatch();
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("SENT");
        assertThat(lastError(id)).isNull();
    }

    @Test
    void missingFailureReasonFallsBackToOutcomeName() {
        jdbc.update("DELETE FROM outbox_events");
        UUID id = new OutboxRepository(jdbc).insert("order", "le4", "order.created", "{}");

        dispatcher(failingWith(PublishOutcome.PERMANENT, null), props(10, 3)).dispatchBatch();

        assertThat(lastError(id)).isEqualTo("publish failed (PERMANENT)");
    }

    @Test
    void overlongFailureReasonIsTruncated() {
        jdbc.update("DELETE FROM outbox_events");
        UUID id = new OutboxRepository(jdbc).insert("order", "le5", "order.created", "{}");

        dispatcher(failingWith(PublishOutcome.PERMANENT, "x".repeat(5000)), props(10, 3)).dispatchBatch();

        assertThat(lastError(id)).hasSize(1000);
    }

    @Test
    void failureWhileFinalizingReleasesTheRestOfTheBatchImmediately() {
        jdbc.update("DELETE FROM outbox_events");
        OutboxRepository real = new OutboxRepository(jdbc);
        for (int i = 0; i < 3; i++) {
            real.insert("order", "rel" + i, "order.created", "{}");
        }
        java.util.concurrent.atomic.AtomicInteger sent = new java.util.concurrent.atomic.AtomicInteger();
        OutboxRepository flaky = new OutboxRepository(jdbc) {
            @Override
            public int markSent(UUID id, UUID claimToken) {
                if (sent.incrementAndGet() == 2) {
                    throw new IllegalStateException("database blip");
                }
                return super.markSent(id, claimToken);
            }
        };
        OutboxDispatcher d = new OutboxDispatcher(flaky, new RecordingPublisher(), new TransactionTemplate(tm),
                new SimpleMeterRegistry(), props(10, 5));

        org.assertj.core.api.Assertions.assertThatThrownBy(d::dispatchBatch)
                .isInstanceOf(IllegalStateException.class);

        // Nothing is left leased: one SENT, the other two straight back to PENDING with attempts untouched.
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE status='PROCESSING'", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE status='SENT'", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE status='PENDING' AND attempts=0 AND claim_token IS NULL",
                Integer.class)).isEqualTo(2);
    }

    @Test
    void releasingAnAlreadyFinalizedBatchChangesNothing() {
        jdbc.update("DELETE FROM outbox_events");
        OutboxRepository repo = new OutboxRepository(jdbc);
        UUID id = repo.insert("order", "rel-noop", "order.created", "{}");
        UUID token = UUID.randomUUID();
        assertThat(repo.claimBatch(1, token, 60_000)).hasSize(1);
        repo.markSent(id, token);

        assertThat(repo.releaseClaim(token)).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("SENT");
    }

    @Test
    void stoppingWhileAPublishIsInFlightReleasesTheEventWithoutWaitingForTheLease() throws Exception {
        jdbc.update("DELETE FROM outbox_events");
        UUID id = new OutboxRepository(jdbc).insert("order", "stop1", "order.created", "{}");
        java.util.concurrent.CountDownLatch publishing = new java.util.concurrent.CountDownLatch(1);
        OutboxPublisher blocking = new OutboxPublisher() {
            public PublishBatchResult publish(List<OutboxEvent> events) {
                publishing.countDown();
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // Like the real publisher: an interrupted confirm wait is a transient failure.
                return PublishBatchResult.all(events, PublishOutcome.TRANSIENT, "interrupted");
            }

            public void deadLetter(OutboxEvent event) {
            }
        };
        OutboxScheduler scheduler = new OutboxScheduler(dispatcher(blocking, props(10, 5)),
                org.mockito.Mockito.mock(OutboxCleanup.class), props(10, 5), 200);
        scheduler.start();
        assertThat(publishing.await(10, TimeUnit.SECONDS)).isTrue();

        long started = System.nanoTime();
        scheduler.stop();
        long stopMs = (System.nanoTime() - started) / 1_000_000;

        assertThat(stopMs).isLessThan(5_000);
        assertThat(scheduler.isRunning()).isFalse();
        // Leased for 60s, yet immediately claimable again: it was handed back, not left PROCESSING.
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("PENDING");
        assertThat(jdbc.queryForObject("SELECT claim_token FROM outbox_events WHERE id=?", UUID.class, id))
                .isNull();
        assertThat(jdbc.queryForObject("SELECT attempts FROM outbox_events WHERE id=?", Integer.class, id))
                .isZero();
    }

    private static OutboxPublisher alwaysPermanentFailure() {
        return failingWith(PublishOutcome.PERMANENT, "nack");
    }

    @Test
    void becomingDeadStampsDeadAt() {
        jdbc.update("DELETE FROM outbox_events");
        UUID id = new OutboxRepository(jdbc).insert("order", "dd1", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET attempts=2 WHERE id=?", id);

        dispatcher(alwaysPermanentFailure(), props(10, 3)).dispatchBatch();

        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("DEAD");
        assertThat(jdbc.queryForObject("SELECT dead_at IS NOT NULL FROM outbox_events WHERE id=?", Boolean.class, id))
                .isTrue();
    }

    @Test
    void replayClearsDeadAtSoARedeadedEventStartsItsRetentionAgain() {
        jdbc.update("DELETE FROM outbox_events");
        OutboxRepository repo = new OutboxRepository(jdbc);
        UUID one = repo.insert("order", "dd2", "order.created", "{}");
        UUID two = repo.insert("order", "dd3", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET status='DEAD', dead_at=now() - interval '30 days'");

        repo.replayDead(one);
        assertThat(jdbc.queryForObject("SELECT dead_at FROM outbox_events WHERE id=?", Object.class, one)).isNull();
        assertThat(jdbc.queryForObject("SELECT dead_at FROM outbox_events WHERE id=?", Object.class, two)).isNotNull();

        repo.replayAllDead();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE dead_at IS NOT NULL", Integer.class))
                .isZero();
    }

    @Test
    void deleteDeadBeforeRemovesOnlyDeadRowsThatDiedBeforeTheCutoff() {
        jdbc.update("DELETE FROM outbox_events");
        OutboxRepository repo = new OutboxRepository(jdbc);
        UUID oldDead = repo.insert("order", "old-dead", "order.created", "{}");
        UUID recentDead = repo.insert("order", "recent-dead", "order.created", "{}");
        UUID oldPending = repo.insert("order", "old-pending", "order.created", "{}");
        UUID oldSent = repo.insert("order", "old-sent", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET status='DEAD', dead_at=now() - interval '10 days' WHERE id=?", oldDead);
        jdbc.update("UPDATE outbox_events SET status='DEAD', dead_at=now() - interval '1 hour' WHERE id=?", recentDead);
        jdbc.update("UPDATE outbox_events SET created_at=now() - interval '30 days' WHERE id=?", oldPending);
        jdbc.update("UPDATE outbox_events SET status='SENT', sent_at=now() - interval '30 days' WHERE id=?", oldSent);

        int deleted = repo.deleteDeadBefore(java.time.OffsetDateTime.now().minusDays(7));

        assertThat(deleted).isEqualTo(1);
        assertThat(jdbc.queryForList("SELECT id FROM outbox_events", UUID.class))
                .containsExactlyInAnyOrder(recentDead, oldPending, oldSent);
    }

    @Test
    void deadAtMigrationBackfillsExistingDeadRowsOnly() {
        jdbc.execute("DROP DATABASE IF EXISTS backfill_test");
        jdbc.execute("CREATE DATABASE backfill_test");
        String url = pg.getJdbcUrl().replace("/" + pg.getDatabaseName(), "/backfill_test");
        Flyway.configure().dataSource(url, pg.getUsername(), pg.getPassword())
                .locations("classpath:db/pigeon").target("10001").load().migrate();
        JdbcTemplate old = new JdbcTemplate(new DriverManagerDataSource(url, pg.getUsername(), pg.getPassword()));
        for (String status : List.of("DEAD", "PENDING", "SENT")) {
            old.update("INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status) "
                    + "VALUES (?, 'order', ?, 'order.created', '{}'::jsonb, ?)", UUID.randomUUID(), status, status);
        }

        Flyway.configure().dataSource(url, pg.getUsername(), pg.getPassword())
                .locations("classpath:db/pigeon").load().migrate();

        assertThat(old.queryForObject("SELECT dead_at IS NOT NULL FROM outbox_events WHERE status='DEAD'", Boolean.class))
                .isTrue();
        assertThat(old.queryForObject("SELECT count(*) FROM outbox_events WHERE status<>'DEAD' AND dead_at IS NOT NULL",
                Integer.class)).isZero();
    }

    @Test
    void healthIsDownWhenARealEventHasBeenWaitingTooLong() {
        jdbc.update("DELETE FROM outbox_events");
        OutboxRepository repo = new OutboxRepository(jdbc);
        UUID stuck = repo.insert("order", "health1", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET created_at = now() - interval '1 hour' WHERE id=?", stuck);
        OutboxScheduler running = org.mockito.Mockito.mock(OutboxScheduler.class);
        org.mockito.Mockito.when(running.isRunning()).thenReturn(true);
        OutboxHealthIndicator indicator = new OutboxHealthIndicator(repo, props(10, 5), running);

        org.springframework.boot.actuate.health.Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(org.springframework.boot.actuate.health.Status.DOWN);
        assertThat(health.getDetails()).containsEntry("pending", 1L);
        assertThat((Long) health.getDetails().get("lagSeconds")).isGreaterThanOrEqualTo(3600L);

        jdbc.update("UPDATE outbox_events SET status='SENT', sent_at=now() WHERE id=?", stuck);
        assertThat(indicator.health().getStatus()).isEqualTo(org.springframework.boot.actuate.health.Status.UP);
    }

    @Test
    void cleanupDeletesOnlyExpiredSentRows() {
        OutboxRepository repo = new OutboxRepository(jdbc);
        UUID expired = repo.insert("order", "expired", "order.created", "{}");
        UUID current = repo.insert("order", "current", "order.created", "{}");
        jdbc.update("UPDATE outbox_events SET status='SENT', sent_at=now() - interval '2 days' WHERE id=?", expired);
        jdbc.update("UPDATE outbox_events SET status='SENT', sent_at=now() WHERE id=?", current);

        assertThat(repo.deleteSentBefore(java.time.OffsetDateTime.now().minusDays(1))).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE id=?", Integer.class, expired))
                .isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE id=?", Integer.class, current))
                .isEqualTo(1);
    }

    @Test
    void staleClaimCannotBeMarkedSentByItsOldOwner() {
        jdbc.update("DELETE FROM outbox_events");
        UUID id = new OutboxRepository(jdbc).insert("order", "claim", "order.created", "{}");
        UUID oldOwner = UUID.randomUUID();
        OutboxRepository repo = new OutboxRepository(jdbc);
        assertThat(repo.claimBatch(1, oldOwner, 60_000)).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("PROCESSING");

        jdbc.update("UPDATE outbox_events SET claim_token=? WHERE id=?", UUID.randomUUID(), id);

        assertThat(repo.markSent(id, oldOwner)).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, id))
                .isEqualTo("PROCESSING");
    }

    @Test
    void businessWriteAndOutboxWriteRollBackTogether() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS business_write (id varchar(100) PRIMARY KEY)");
        jdbc.update("DELETE FROM business_write");
        jdbc.update("DELETE FROM outbox_events");
        Outbox outbox = new Outbox(new OutboxRepository(jdbc),
                new com.fasterxml.jackson.databind.ObjectMapper());

        assertThat(org.assertj.core.api.Assertions.catchThrowable(() ->
                new TransactionTemplate(tm).execute(status -> {
                    jdbc.update("INSERT INTO business_write (id) VALUES (?)", "same-tx");
                    outbox.write("order", "same-tx", "order.created", "{}");
                    throw new IllegalStateException("rollback");
                }))).isInstanceOf(IllegalStateException.class);

        assertThat(jdbc.queryForObject("SELECT count(*) FROM business_write", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events", Integer.class)).isZero();
    }
}
