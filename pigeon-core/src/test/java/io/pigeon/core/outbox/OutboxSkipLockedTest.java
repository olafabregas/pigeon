package io.pigeon.core.outbox;

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

import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class OutboxSkipLockedTest {

    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine");

    static JdbcTemplate jdbc;
    static DataSourceTransactionManager tm;

    @BeforeAll
    static void init() {
        Flyway.configure().dataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword()).load().migrate();
        DriverManagerDataSource ds = new DriverManagerDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        jdbc = new JdbcTemplate(ds);
        tm = new DataSourceTransactionManager(ds);
    }

    /** Fake broker: records every published id and is slow so batches overlap in time. */
    static class RecordingPublisher implements OutboxPublisher {
        final List<UUID> published = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Set<UUID> publish(List<OutboxEvent> events) {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            Set<UUID> ids = new HashSet<>();
            events.forEach(e -> ids.add(e.id()));
            published.addAll(ids);
            return ids;
        }

        @Override
        public void deadLetter(OutboxEvent event) {
        }
    }

    private OutboxDispatcher dispatcher(OutboxPublisher p, OutboxProperties props) {
        return new OutboxDispatcher(new OutboxRepository(jdbc), p, new TransactionTemplate(tm),
                new SimpleMeterRegistry(), props);
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
        OutboxProperties props = new OutboxProperties(true, 10, 100, 5, 10, 100, 1000);
        OutboxDispatcher d1 = dispatcher(pub, props);
        OutboxDispatcher d2 = dispatcher(pub, props);

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
        assertThat(n2).isPositive(); // both really participated
        assertThat(pub.published).hasSize(total).doesNotHaveDuplicates();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE status='SENT'", Integer.class))
                .isEqualTo(total);
    }

    @Test
    void failedEventsBackOffThenGoDead() {
        jdbc.update("DELETE FROM outbox_events");
        new OutboxRepository(jdbc).insert("order", "x", "order.created", "{}");
        OutboxPublisher failing = new OutboxPublisher() {
            public Set<UUID> publish(List<OutboxEvent> e) {
                return Set.of();
            }

            public void deadLetter(OutboxEvent e) {
            }
        };
        OutboxProperties props = new OutboxProperties(true, 10, 100, 3, 1, 5, 1000);
        OutboxDispatcher d = dispatcher(failing, props);
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
}
