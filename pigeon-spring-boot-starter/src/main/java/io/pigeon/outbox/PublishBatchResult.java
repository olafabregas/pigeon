package io.pigeon.outbox;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Per-event outcomes of one publish attempt, plus an optional human-readable failure reason
 * that is stored in {@code outbox_events.last_error}.
 */
public record PublishBatchResult(Map<UUID, PublishOutcome> outcomes, Map<UUID, String> errors) {

    public PublishBatchResult(Map<UUID, PublishOutcome> outcomes) {
        this(outcomes, Map.of());
    }

    public PublishOutcome get(UUID id) {
        return outcomes.getOrDefault(id, PublishOutcome.TRANSIENT);
    }

    /** @return why the event failed, or {@code null} if no reason was recorded */
    public String error(UUID id) {
        return errors.get(id);
    }

    public static PublishBatchResult all(List<OutboxEvent> events, PublishOutcome outcome) {
        return all(events, outcome, null);
    }

    public static PublishBatchResult all(List<OutboxEvent> events, PublishOutcome outcome, String error) {
        Map<UUID, PublishOutcome> m = new HashMap<>();
        Map<UUID, String> errs = new HashMap<>();
        for (OutboxEvent e : events) {
            m.put(e.id(), outcome);
            if (error != null) {
                errs.put(e.id(), error);
            }
        }
        return new PublishBatchResult(Map.copyOf(m), Map.copyOf(errs));
    }

    public static PublishBatchResult of(Map<UUID, PublishOutcome> outcomes) {
        return new PublishBatchResult(Map.copyOf(outcomes), Map.of());
    }

    public static PublishBatchResult of(Map<UUID, PublishOutcome> outcomes, Map<UUID, String> errors) {
        return new PublishBatchResult(Map.copyOf(outcomes), Map.copyOf(errors));
    }
}
