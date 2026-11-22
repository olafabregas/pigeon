package io.pigeon.outbox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxSchedulerTest {

    private OutboxScheduler scheduler;

    @AfterEach
    void stopScheduler() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    private static OutboxProperties props(long pollMs, int batchSize) {
        return new OutboxProperties(true, batchSize, pollMs, 8, 1000, 300_000, 0.0, 5000, 5000,
                604_800_000, -1, 3_600_000, 60_000,
                new OutboxProperties.Flyway(false, "classpath:db/pigeon", "pigeon_schema_history"),
                new OutboxProperties.Health(true, 300),
                "pigeon.events", "pigeon.dlx", "pigeon.dlq");
    }

    @Test
    void pollsRepeatedlyOnItsOwnThread() {
        AtomicInteger calls = new AtomicInteger();
        OutboxDispatcher dispatcher = mock(OutboxDispatcher.class);
        when(dispatcher.dispatchBatch()).thenAnswer(i -> {
            calls.incrementAndGet();
            return 0;
        });
        scheduler = new OutboxScheduler(dispatcher, mock(OutboxCleanup.class), props(10, 50));

        scheduler.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(calls.get()).isGreaterThanOrEqualTo(3));
        assertThat(scheduler.isRunning()).isTrue();
    }

    @Test
    void drainsFullBatchesWithoutWaitingForTheNextPoll() {
        AtomicInteger calls = new AtomicInteger();
        OutboxDispatcher dispatcher = mock(OutboxDispatcher.class);
        when(dispatcher.dispatchBatch()).thenAnswer(i -> calls.incrementAndGet() <= 3 ? 50 : 0);
        scheduler = new OutboxScheduler(dispatcher, mock(OutboxCleanup.class), props(60_000, 50));

        scheduler.start();

        // The poll interval is a minute, so four calls this fast can only come from the drain loop.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(calls.get()).isEqualTo(4));
    }

    @Test
    void keepsPollingAfterADispatchFailure() {
        AtomicInteger calls = new AtomicInteger();
        OutboxDispatcher dispatcher = mock(OutboxDispatcher.class);
        when(dispatcher.dispatchBatch()).thenAnswer(i -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("database down");
            }
            return 0;
        });
        scheduler = new OutboxScheduler(dispatcher, mock(OutboxCleanup.class), props(10, 50));

        scheduler.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(calls.get()).isGreaterThanOrEqualTo(3));
    }

    @Test
    void stopHaltsPolling() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        OutboxDispatcher dispatcher = mock(OutboxDispatcher.class);
        when(dispatcher.dispatchBatch()).thenAnswer(i -> {
            calls.incrementAndGet();
            return 0;
        });
        scheduler = new OutboxScheduler(dispatcher, mock(OutboxCleanup.class), props(10, 50));
        scheduler.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() >= 2);

        scheduler.stop();
        int afterStop = calls.get();
        Thread.sleep(100);

        assertThat(scheduler.isRunning()).isFalse();
        assertThat(calls.get()).isEqualTo(afterStop);
    }

    @Test
    void startIsIdempotentAndCanRestartAfterStop() {
        OutboxDispatcher dispatcher = mock(OutboxDispatcher.class);
        scheduler = new OutboxScheduler(dispatcher, mock(OutboxCleanup.class), props(10, 50));

        scheduler.start();
        scheduler.start();
        scheduler.stop();
        scheduler.start();

        assertThat(scheduler.isRunning()).isTrue();
    }

    @Test
    void stopWaitDefaultsToOneConfirmTimeoutPlusHeadroom() throws Exception {
        java.lang.reflect.Field f = OutboxScheduler.class.getDeclaredField("stopWaitMs");
        f.setAccessible(true);
        OutboxScheduler s = new OutboxScheduler(mock(OutboxDispatcher.class), mock(OutboxCleanup.class), props(10, 50));

        // props() uses confirmTimeoutMs = 5000
        assertThat(f.getLong(s)).isEqualTo(5_000 + 5_000);
    }

    @Test
    void stopInterruptsABatchThatOutlivesTheWait() {
        java.util.concurrent.atomic.AtomicBoolean interrupted = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.CountDownLatch inFlight = new java.util.concurrent.CountDownLatch(1);
        OutboxDispatcher dispatcher = mock(OutboxDispatcher.class);
        when(dispatcher.dispatchBatch()).thenAnswer(i -> {
            inFlight.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
            return 0;
        });
        scheduler = new OutboxScheduler(dispatcher, mock(OutboxCleanup.class), props(10, 50), 100);
        scheduler.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> inFlight.getCount() == 0);

        scheduler.stop();

        await().atMost(Duration.ofSeconds(5)).untilTrue(interrupted);
        assertThat(scheduler.isRunning()).isFalse();
    }

    @Test
    void runsSentCleanup() {
        OutboxCleanup cleanup = mock(OutboxCleanup.class);
        scheduler = new OutboxScheduler(mock(OutboxDispatcher.class), cleanup, props(10, 50));

        scheduler.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(cleanup, atLeastOnce()).purge());
    }

    @Test
    void usesDaemonThreadsSoItNeverBlocksJvmExit() {
        java.util.concurrent.atomic.AtomicReference<Thread> seen = new java.util.concurrent.atomic.AtomicReference<>();
        OutboxDispatcher dispatcher = mock(OutboxDispatcher.class);
        when(dispatcher.dispatchBatch()).thenAnswer(i -> {
            seen.set(Thread.currentThread());
            return 0;
        });
        scheduler = new OutboxScheduler(dispatcher, mock(OutboxCleanup.class), props(10, 50));

        scheduler.start();

        await().atMost(Duration.ofSeconds(5)).until(() -> seen.get() != null);
        assertThat(seen.get().isDaemon()).isTrue();
        assertThat(seen.get().getName()).startsWith("pigeon-outbox-");
    }
}
