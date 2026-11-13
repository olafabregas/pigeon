package io.pigeon.core.outbox;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Abstraction over the broker so the dispatcher can be tested without RabbitMQ. */
public interface OutboxPublisher {

    /** Publishes all events and waits for confirms. Returns ids of events that were confirmed. */
    Set<UUID> publish(List<OutboxEvent> events);

    /** Best-effort routing of an exhausted event to the dead-letter exchange. */
    void deadLetter(OutboxEvent event);
}
