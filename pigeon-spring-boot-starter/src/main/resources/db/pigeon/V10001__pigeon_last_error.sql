-- Most recent publish failure for an event that has not been delivered.
-- Cleared when the event is sent; kept on DEAD rows so the cause survives until a replay succeeds.
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS last_error text;
