package io.pigeon.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxBackoffJitterTest {

    private static OutboxDispatcher dispatcher(double jitter) {
        OutboxProperties props = new OutboxProperties(true, 10, 100, 8, 1000, 300_000, jitter, 5000, 5000,
                604_800_000, -1, 3_600_000, 60_000,
                new OutboxProperties.Flyway(false, "classpath:db/pigeon", "pigeon_schema_history"),
                "pigeon.events", "pigeon.dlx", "pigeon.dlq");
        return new OutboxDispatcher(null, null, null, new SimpleMeterRegistry(), props);
    }

    @Test
    void zeroJitterKeepsDelayExact() {
        OutboxDispatcher d = dispatcher(0.0);

        for (int i = 0; i < 100; i++) {
            assertThat(d.jitter(Duration.ofMillis(1000))).isEqualTo(Duration.ofMillis(1000));
        }
    }

    @Test
    void jitterOnlyShortensDelayWithinTheConfiguredFraction() {
        OutboxDispatcher d = dispatcher(0.25);

        for (int i = 0; i < 1000; i++) {
            long ms = d.jitter(Duration.ofMillis(1000)).toMillis();
            assertThat(ms).isBetween(750L, 1000L);
        }
    }

    @Test
    void jitterSpreadsDelaysSoRetriesDoNotSynchronize() {
        OutboxDispatcher d = dispatcher(0.5);
        Set<Long> distinct = new HashSet<>();

        for (int i = 0; i < 200; i++) {
            distinct.add(d.jitter(Duration.ofMillis(1000)).toMillis());
        }

        assertThat(distinct.size()).isGreaterThan(20);
    }

    @Test
    void jitterNeverExceedsMaxBackoff() {
        OutboxDispatcher d = dispatcher(0.2);

        for (int attempt = 1; attempt <= 40; attempt++) {
            assertThat(d.jitter(d.backoff(attempt)).toMillis()).isLessThanOrEqualTo(300_000L);
        }
    }

    @Test
    void outOfRangeJitterIsClamped() {
        assertThat(dispatcher(-1.0).jitter(Duration.ofMillis(1000))).isEqualTo(Duration.ofMillis(1000));
        for (int i = 0; i < 200; i++) {
            assertThat(dispatcher(5.0).jitter(Duration.ofMillis(1000)).toMillis()).isBetween(0L, 1000L);
        }
    }

    @Test
    void jitterBindsFromPropertiesWithDefault() {
        assertThat(OutboxProperties.defaults().backoffJitter()).isEqualTo(0.2);
    }
}
