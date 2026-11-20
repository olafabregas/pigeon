CREATE TABLE IF NOT EXISTS outbox_events (
    id              uuid PRIMARY KEY,
    aggregate_type  varchar(100) NOT NULL,
    aggregate_id    varchar(100) NOT NULL,
    event_type      varchar(100) NOT NULL,
    payload         jsonb NOT NULL,
    status          varchar(10) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PROCESSING','SENT','DEAD')),
    attempts        int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    sent_at         timestamptz,
    claim_token     uuid,
    claimed_at      timestamptz
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (next_attempt_at) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_outbox_claim_recovery ON outbox_events (status, claimed_at);
CREATE INDEX IF NOT EXISTS idx_outbox_sent_cleanup ON outbox_events (status, sent_at);
