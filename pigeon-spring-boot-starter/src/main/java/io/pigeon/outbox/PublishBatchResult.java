package io.pigeon.outbox;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record PublishBatchResult(Map<UUID, PublishOutcome> outcomes) {

    public PublishOutcome get(UUID id) {
        return outcomes.getOrDefault(id, PublishOutcome.TRANSIENT);
    }

    public static PublishBatchResult all(List<OutboxEvent> events, PublishOutcome outcome) {
        Map<UUID, PublishOutcome> m = new HashMap<>();
        for (OutboxEvent e : events) {
            m.put(e.id(), outcome);
        }
        return new PublishBatchResult(Map.copyOf(m));
    }

    public static PublishBatchResult of(Map<UUID, PublishOutcome> outcomes) {
        return new PublishBatchResult(Map.copyOf(outcomes));
    }
}
