package io.pigeon.core.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
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

    public void markRetry(UUID id, OffsetDateTime nextAttemptAt) {
        jdbc.update("UPDATE outbox_events SET attempts=attempts+1, next_attempt_at=? WHERE id=?", nextAttemptAt, id);
    }

    public void markDead(UUID id) {
        jdbc.update("UPDATE outbox_events SET status='DEAD', attempts=attempts+1 WHERE id=?", id);
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
