package io.pigeon.outbox;

import java.time.OffsetDateTime;
import java.util.UUID;

public record OutboxEvent(
        UUID id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        int attempts,
        OffsetDateTime createdAt) {
}
