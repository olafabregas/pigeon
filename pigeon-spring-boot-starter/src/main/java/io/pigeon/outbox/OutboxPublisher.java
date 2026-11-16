package io.pigeon.outbox;

import java.util.List;

/** Abstraction over the broker so the dispatcher can be tested without RabbitMQ. */
public interface OutboxPublisher {

    /**
     * Publishes all events and waits for confirms.
     * Each event gets a {@link PublishOutcome} (confirmed / transient / permanent).
     */
    PublishBatchResult publish(List<OutboxEvent> events);

    /** Best-effort routing of an exhausted event to the dead-letter exchange. */
    void deadLetter(OutboxEvent event);
}
