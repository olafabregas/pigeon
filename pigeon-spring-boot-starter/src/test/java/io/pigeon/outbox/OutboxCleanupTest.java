package io.pigeon.outbox;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxCleanupTest {

    private final OutboxRepository repository = mock(OutboxRepository.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private Logger logger;

    @BeforeEach
    void captureLogs() {
        logger = (Logger) LoggerFactory.getLogger(OutboxCleanup.class);
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void releaseLogs() {
        logger.detachAppender(appender);
        appender.stop();
    }

    private static OutboxProperties props(long sentRetentionMs, long deadRetentionMs) {
        return new OutboxProperties(true, 50, 500, 8, 1000, 300_000, 0.0, 5000, 5000,
                sentRetentionMs, deadRetentionMs, 3_600_000, 60_000,
                new OutboxProperties.Flyway(false, "classpath:db/pigeon", "pigeon_schema_history"),
                "pigeon.events", "pigeon.dlx", "pigeon.dlq");
    }

    @Test
    void deadRowsAreKeptForeverByDefault() {
        new OutboxCleanup(repository, OutboxProperties.defaults()).purge();

        verify(repository, never()).deleteDeadBefore(any());
        assertThat(OutboxProperties.defaults().deadRetentionMs()).isNegative();
    }

    @Test
    void anyNegativeRetentionKeepsDeadRows() {
        new OutboxCleanup(repository, props(1000, -5)).purge();

        verify(repository, never()).deleteDeadBefore(any());
    }

    @Test
    void configuredRetentionPurgesDeadRowsOlderThanTheCutoff() {
        long retention = Duration.ofDays(30).toMillis();
        new OutboxCleanup(repository, props(1000, retention)).purge();

        ArgumentCaptor<OffsetDateTime> cutoff = ArgumentCaptor.forClass(OffsetDateTime.class);
        verify(repository).deleteDeadBefore(cutoff.capture());
        assertThat(cutoff.getValue())
                .isBetween(OffsetDateTime.now().minusDays(30).minusSeconds(5),
                        OffsetDateTime.now().minusDays(30).plusSeconds(5));
    }

    @Test
    void zeroRetentionPurgesDeadRowsImmediately() {
        new OutboxCleanup(repository, props(1000, 0)).purge();

        ArgumentCaptor<OffsetDateTime> cutoff = ArgumentCaptor.forClass(OffsetDateTime.class);
        verify(repository).deleteDeadBefore(cutoff.capture());
        assertThat(cutoff.getValue()).isBetween(OffsetDateTime.now().minusSeconds(5), OffsetDateTime.now().plusSeconds(5));
    }

    @Test
    void purgingDeadRowsIsLoggedAtWarnBecauseTheyCannotBeReplayedAfterwards() {
        when(repository.deleteDeadBefore(any())).thenReturn(3);

        new OutboxCleanup(repository, props(1000, 5000)).purge();

        assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.WARN
                && e.getFormattedMessage().contains("purged 3 DEAD outbox event(s) older than 5000 ms")
                && e.getFormattedMessage().contains("can no longer be replayed"));
    }

    @Test
    void nothingIsLoggedWhenNothingWasPurged() {
        new OutboxCleanup(repository, props(1000, 5000)).purge();

        assertThat(appender.list).isEmpty();
    }

    @Test
    void sentAndDeadPurgesAreIndependent() {
        when(repository.deleteSentBefore(any())).thenReturn(2);

        new OutboxCleanup(repository, OutboxProperties.defaults()).purge();

        verify(repository).deleteSentBefore(any());
        verify(repository, never()).deleteDeadBefore(any());
        assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.INFO
                && e.getFormattedMessage().contains("purged 2 SENT"));
    }
}
