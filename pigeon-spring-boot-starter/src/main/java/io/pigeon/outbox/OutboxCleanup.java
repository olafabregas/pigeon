package io.pigeon.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.OffsetDateTime;

final class OutboxCleanup {
    private static final Logger log = LoggerFactory.getLogger(OutboxCleanup.class);

    private final OutboxRepository repository;
    private final OutboxProperties props;

    OutboxCleanup(OutboxRepository repository, OutboxProperties props) {
        this.repository = repository;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${pigeon.outbox.sent-cleanup-interval-ms:3600000}")
    void purgeSent() {
        int deleted = repository.deleteSentBefore(
                OffsetDateTime.now().minusMillis(props.sentRetentionMs()));
        if (deleted > 0) {
            log.info("purged {} SENT outbox event(s) older than {} ms",
                    deleted, props.sentRetentionMs());
        }
    }
}
