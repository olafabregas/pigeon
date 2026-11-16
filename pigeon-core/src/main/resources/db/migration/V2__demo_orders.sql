CREATE TABLE IF NOT EXISTS orders (
    id          uuid PRIMARY KEY,
    customer_id varchar(100) NOT NULL,
    amount      numeric(19,2) NOT NULL,
    status      varchar(20) NOT NULL DEFAULT 'CREATED',
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id   varchar(100) PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
