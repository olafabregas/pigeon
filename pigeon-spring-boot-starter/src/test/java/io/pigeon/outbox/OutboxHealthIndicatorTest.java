package io.pigeon.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class OutboxHealthIndicatorTest {

    private final OutboxRepository repository = mock(OutboxRepository.class);
    private final OutboxScheduler scheduler = mock(OutboxScheduler.class);

    private static OutboxProperties props(long maxLagSeconds) {
        return new OutboxProperties(true, 50, 500, 8, 1000, 300_000, 0.0, 5000, 5000,
                604_800_000, -1, 3_600_000, 60_000,
                new OutboxProperties.Flyway(false, "classpath:db/pigeon", "pigeon_schema_history"),
                new OutboxProperties.Health(true, maxLagSeconds),
                "pigeon.events", "pigeon.dlx", "pigeon.dlq");
    }

    private void state(boolean running, double lagSeconds, long pending, long dead) {
        when(scheduler.isRunning()).thenReturn(running);
        when(repository.oldestPendingAgeSeconds()).thenReturn(lagSeconds);
        when(repository.countPending()).thenReturn(pending);
        when(repository.countByStatus("DEAD")).thenReturn(dead);
    }

    @Test
    void upWhenRunningAndLagIsWithinTheLimit() {
        state(true, 12.4, 3, 0);

        Health health = new OutboxHealthIndicator(repository, props(300), scheduler).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("dispatching", "running")
                .containsEntry("pending", 3L).containsEntry("dead", 0L)
                .containsEntry("lagSeconds", 12L).containsEntry("maxLagSeconds", 300L)
                .doesNotContainKey("reason");
    }

    @Test
    void downWhenTheOldestEventIsOlderThanTheLimit() {
        state(true, 412.0, 9, 0);

        Health health = new OutboxHealthIndicator(repository, props(300), scheduler).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails().get("reason").toString())
                .contains("412s old").contains("300s limit");
    }

    @Test
    void lagExactlyAtTheLimitIsStillUp() {
        state(true, 300.0, 1, 0);

        assertThat(new OutboxHealthIndicator(repository, props(300), scheduler).health().getStatus())
                .isEqualTo(Status.UP);
    }

    @Test
    void downWhenTheDispatcherIsNotRunning() {
        state(false, 0.0, 0, 0);

        Health health = new OutboxHealthIndicator(repository, props(300), scheduler).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("dispatching", "stopped");
        assertThat(health.getDetails().get("reason").toString()).contains("not running");
    }

    @Test
    void deadEventsAreReportedButDoNotFailTheCheck() {
        state(true, 1.0, 0, 42);

        Health health = new OutboxHealthIndicator(repository, props(300), scheduler).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("dead", 42L);
    }

    @Test
    void zeroMaxLagDisablesTheLagCheck() {
        state(true, 999_999.0, 100, 0);

        assertThat(new OutboxHealthIndicator(repository, props(0), scheduler).health().getStatus())
                .isEqualTo(Status.UP);
    }

    @Test
    void aWriteOnlyInstanceIsUpWithoutTouchingTheDatabase() {
        Health health = new OutboxHealthIndicator(repository, props(300), null).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("dispatching", "disabled");
        verifyNoInteractions(repository);
    }

    @Test
    void downWithTheErrorWhenTheDatabaseCannotBeQueried() {
        when(scheduler.isRunning()).thenReturn(true);
        when(repository.oldestPendingAgeSeconds()).thenThrow(new IllegalStateException("db unreachable"));

        Health health = new OutboxHealthIndicator(repository, props(300), scheduler).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails().get("error").toString()).contains("db unreachable");
    }
}
