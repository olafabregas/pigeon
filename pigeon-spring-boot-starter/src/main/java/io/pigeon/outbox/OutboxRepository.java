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

    /** Claims rows and releases the row locks when the surrounding transaction commits. */
    public List<OutboxEvent> claimBatch(int limit, UUID claimToken, long claimLeaseMs) {
        return jdbc.query("""
                WITH claimed AS (
                    SELECT id
                    FROM outbox_events
                    WHERE (status = 'PENDING'
                           OR (status = 'PROCESSING'
                               AND claimed_at < now() - (? * interval '1 millisecond')))
                      AND next_attempt_at <= now()
                    ORDER BY next_attempt_at, created_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_events e
                SET status='PROCESSING', claim_token=?, claimed_at=now()
                FROM claimed c
                WHERE e.id=c.id
                RETURNING e.id, e.aggregate_type, e.aggregate_id, e.event_type,
                          e.payload::text AS payload, e.attempts, e.created_at
                """, (rs, i) -> new OutboxEvent(
                rs.getObject("id", UUID.class),
                rs.getString("aggregate_type"),
                rs.getString("aggregate_id"),
                rs.getString("event_type"),
                rs.getString("payload"),
                rs.getInt("attempts"),
                rs.getObject("created_at", OffsetDateTime.class)), claimLeaseMs, limit, claimToken);
    }

    public int markSent(UUID id, UUID claimToken) {
        return jdbc.update("UPDATE outbox_events SET status='SENT', attempts=attempts+1, sent_at=now(), claim_token=NULL, claimed_at=NULL WHERE id=? AND status='PROCESSING' AND claim_token=?", id, claimToken);
    }

    /** Permanent failure: increment attempts and schedule retry. */
    public int markRetry(UUID id, UUID claimToken, OffsetDateTime nextAttemptAt) {
        return jdbc.update("UPDATE outbox_events SET status='PENDING', attempts=attempts+1, next_attempt_at=?, claim_token=NULL, claimed_at=NULL WHERE id=? AND status='PROCESSING' AND claim_token=?", nextAttemptAt, id, claimToken);
    }

    /**
     * Transient failure (broker down): delay retry without incrementing attempts,
     * so outages do not push events to DEAD.
     */
    public void markSoftRetry(UUID id, UUID claimToken, OffsetDateTime nextAttemptAt) {
        jdbc.update("UPDATE outbox_events SET status='PENDING', next_attempt_at=?, claim_token=NULL, claimed_at=NULL WHERE id=? AND status='PROCESSING' AND claim_token=?", nextAttemptAt, id, claimToken);
    }

    public int markDead(UUID id, UUID claimToken) {
        return jdbc.update("UPDATE outbox_events SET status='DEAD', attempts=attempts+1, claim_token=NULL, claimed_at=NULL WHERE id=? AND status='PROCESSING' AND claim_token=?", id, claimToken);
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

    public long countPending() {
        Long n = jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE status IN ('PENDING','PROCESSING')",
                Long.class);
        return n == null ? 0 : n;
    }

    public int deleteSentBefore(OffsetDateTime cutoff) {
        return jdbc.update("DELETE FROM outbox_events WHERE status='SENT' AND sent_at < ?", cutoff);
    }

    /** Age in seconds of oldest PENDING or PROCESSING event, 0 if none. */
    public double oldestPendingAgeSeconds() {
        Double d = jdbc.queryForObject("""
                SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)::float8
                FROM outbox_events WHERE status IN ('PENDING','PROCESSING')
                """, Double.class);
        return d == null ? 0 : d;
    }
}
