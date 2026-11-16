package io.pigeon.outbox;

import org.springframework.jdbc.core.JdbcTemplate;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class OutboxRepository {
    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public UUID insert(String aggregateType, String aggregateId, String eventType, String payloadJson) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status, attempts, next_attempt_at)
                VALUES (?, ?, ?, ?, ?::jsonb, 'PENDING', 0, now())
                """, id, aggregateType, aggregateId, eventType, payloadJson);
        return id;
    }

    /** Must be called inside a transaction: row locks are held until it ends. */
    public List<OutboxEvent> claimBatch(int limit) {
        return jdbc.query("""
                SELECT id, aggregate_type, aggregate_id, event_type, payload::text AS payload, attempts, created_at
                FROM outbox_events
                WHERE status = 'PENDING' AND next_attempt_at <= now()
                ORDER BY next_attempt_at, created_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """, (rs, i) -> new OutboxEvent(
                rs.getObject("id", UUID.class),
                rs.getString("aggregate_type"),
                rs.getString("aggregate_id"),
                rs.getString("event_type"),
                rs.getString("payload"),
                rs.getInt("attempts"),
                rs.getObject("created_at", OffsetDateTime.class)), limit);
    }

    public void markSent(UUID id) {
        jdbc.update("UPDATE outbox_events SET status='SENT', attempts=attempts+1, sent_at=now() WHERE id=?", id);
    }

    /** Permanent failure: increment attempts and schedule retry. */
    public void markRetry(UUID id, OffsetDateTime nextAttemptAt) {
        jdbc.update("UPDATE outbox_events SET attempts=attempts+1, next_attempt_at=? WHERE id=?", nextAttemptAt, id);
    }

    /**
     * Transient failure (broker down): delay retry without incrementing attempts,
     * so outages do not push events to DEAD.
     */
    public void markSoftRetry(UUID id, OffsetDateTime nextAttemptAt) {
        jdbc.update("UPDATE outbox_events SET next_attempt_at=? WHERE id=? AND status='PENDING'", nextAttemptAt, id);
    }

    public void markDead(UUID id) {
        jdbc.update("UPDATE outbox_events SET status='DEAD', attempts=attempts+1 WHERE id=?", id);
    }

    /** Requeue a DEAD event for another publish cycle. Returns 1 if replayed, 0 if not found/not DEAD. */
    public int replayDead(UUID id) {
        return jdbc.update("""
                UPDATE outbox_events
                SET status='PENDING', attempts=0, next_attempt_at=now(), sent_at=NULL
                WHERE id=? AND status='DEAD'
                """, id);
    }

    /** Requeue all DEAD events. Returns number of rows updated. */
    public int replayAllDead() {
        return jdbc.update("""
                UPDATE outbox_events
                SET status='PENDING', attempts=0, next_attempt_at=now(), sent_at=NULL
                WHERE status='DEAD'
                """);
    }

    public long countByStatus(String status) {
        Long n = jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE status=?", Long.class, status);
        return n == null ? 0 : n;
    }

    /** Age in seconds of oldest PENDING event, 0 if none. */
    public double oldestPendingAgeSeconds() {
        Double d = jdbc.queryForObject("""
                SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)::float8
                FROM outbox_events WHERE status='PENDING'
                """, Double.class);
        return d == null ? 0 : d;
    }
}
