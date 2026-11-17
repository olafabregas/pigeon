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
        return new OutboxProperties(true, batch, 100, maxAttempts, 1, 5, 1000, 5,
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
}
