package io.pigeon.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs the dispatcher poll loop and the SENT-row cleanup on Pigeon's own threads.
 *
 * <p>It deliberately does not use Spring's {@code @Scheduled}: that would require turning on
 * {@code @EnableScheduling} for the whole application, which changes behaviour of the host app's own
 * scheduled jobs. As a {@link SmartLifecycle} it starts after the context is ready and stops before
 * the DataSource and RabbitMQ connections are closed.
 */
public final class OutboxScheduler implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);
    /** Headroom on top of one confirm timeout for finalizing the in-flight batch. */
    private static final long STOP_HEADROOM_MS = 5_000;

    private final OutboxDispatcher dispatcher;
    private final OutboxCleanup cleanup;
    private final OutboxProperties props;
    private final long stopWaitMs;

    private volatile boolean running;
    private ScheduledExecutorService executor;

    OutboxScheduler(OutboxDispatcher dispatcher, OutboxCleanup cleanup, OutboxProperties props) {
        // One confirm wait is the longest a healthy in-flight batch can block: after the first confirm
        // timeout the publisher stops waiting for the rest of the batch.
        this(dispatcher, cleanup, props, props.confirmTimeoutMs() + STOP_HEADROOM_MS);
    }

    OutboxScheduler(OutboxDispatcher dispatcher, OutboxCleanup cleanup, OutboxProperties props, long stopWaitMs) {
        this.dispatcher = dispatcher;
        this.cleanup = cleanup;
        this.props = props;
        this.stopWaitMs = stopWaitMs;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        executor = Executors.newScheduledThreadPool(2, new PigeonThreadFactory());
        running = true;
        long pollMs = Math.max(1, props.pollIntervalMs());
        long cleanupMs = Math.max(1, props.sentCleanupIntervalMs());
        executor.scheduleWithFixedDelay(this::tick, 0, pollMs, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(this::purge, 0, cleanupMs, TimeUnit.MILLISECONDS);
        log.info("pigeon outbox started: exchange={} batchSize={} pollIntervalMs={}",
                props.exchange(), props.batchSize(), props.pollIntervalMs());
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(stopWaitMs, TimeUnit.MILLISECONDS)) {
                log.warn("pigeon outbox did not stop within {} ms; interrupting the in-flight batch", stopWaitMs);
                executor.shutdownNow();
                if (!executor.awaitTermination(stopWaitMs, TimeUnit.MILLISECONDS)) {
                    log.warn("pigeon outbox thread is still busy after interrupt; "
                            + "unfinished events become claimable when their lease expires");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        log.info("pigeon outbox stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Polls once; drains consecutive full batches. Never throws, or the schedule would be cancelled. */
    void tick() {
        try {
            while (running && dispatcher.dispatchBatch() >= props.batchSize()) {
                // keep draining full batches
            }
        } catch (Exception e) {
            log.warn("outbox dispatch tick failed: {}", e.toString());
        }
    }

    private void purge() {
        try {
            cleanup.purge();
        } catch (Exception e) {
            log.warn("outbox cleanup failed: {}", e.toString());
        }
    }

    private static final class PigeonThreadFactory implements ThreadFactory {
        private final AtomicInteger n = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "pigeon-outbox-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
