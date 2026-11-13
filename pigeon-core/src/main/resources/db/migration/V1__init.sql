CREATE TABLE orders (
    id          uuid PRIMARY KEY,
    customer_id varchar(100) NOT NULL,
    amount      numeric(19,2) NOT NULL,
    status      varchar(20) NOT NULL DEFAULT 'CREATED',
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
    id              uuid PRIMARY KEY,
    aggregate_type  varchar(100) NOT NULL,
    aggregate_id    varchar(100) NOT NULL,
    event_type      varchar(100) NOT NULL,
    payload         jsonb NOT NULL,
    status          varchar(10) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','SENT','DEAD')),
    attempts        int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    sent_at         timestamptz
);

CREATE INDEX idx_outbox_pending ON outbox_events (next_attempt_at) WHERE status = 'PENDING';

CREATE TABLE processed_messages (
    message_id   varchar(100) PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
