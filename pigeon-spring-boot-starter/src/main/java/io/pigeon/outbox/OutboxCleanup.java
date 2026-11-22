package io.pigeon.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.Duration;

final class OutboxCleanup {
    private static final Logger log = LoggerFactory.getLogger(OutboxCleanup.class);

    private final OutboxRepository repository;
    private final OutboxProperties props;

    OutboxCleanup(OutboxRepository repository, OutboxProperties props) {
        this.repository = repository;
        this.props = props;
    }

    void purge() {
        purgeSent();
        purgeDead();
    }

    void purgeSent() {
        int deleted = repository.deleteSentBefore(
                OffsetDateTime.now().minus(Duration.ofMillis(props.sentRetentionMs())));
        if (deleted > 0) {
            log.info("purged {} SENT outbox event(s) older than {} ms",
                    deleted, props.sentRetentionMs());
        }
    }

    /** Opt-in: a negative {@code dead-retention-ms} keeps DEAD rows forever. */
    void purgeDead() {
        if (props.deadRetentionMs() < 0) {
            return;
        }
        int deleted = repository.deleteDeadBefore(
                OffsetDateTime.now().minus(Duration.ofMillis(props.deadRetentionMs())));
        if (deleted > 0) {
            // WARN, not INFO: these events were never delivered and can no longer be replayed.
            log.warn("purged {} DEAD outbox event(s) older than {} ms; they can no longer be replayed",
                    deleted, props.deadRetentionMs());
        }
    }
}
