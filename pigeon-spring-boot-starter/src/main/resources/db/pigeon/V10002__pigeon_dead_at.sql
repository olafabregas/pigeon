-- When an event became DEAD, so DEAD retention counts from that moment (not from creation).
-- Cleared on replay. Rows already DEAD get a full retention period starting now.
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS dead_at timestamptz;

UPDATE outbox_events SET dead_at = now() WHERE status = 'DEAD' AND dead_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_dead_cleanup ON outbox_events (dead_at) WHERE status = 'DEAD';
