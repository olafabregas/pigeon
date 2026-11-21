# Pigeon contract

## Product
- Artifact: `io.pigeon:pigeon-spring-boot-starter`
- Stack: Java 21, Spring Boot 3, PostgreSQL, RabbitMQ

## Outbox table
`outbox_events(id uuid pk, aggregate_type, aggregate_id, event_type, payload jsonb, status [PENDING|PROCESSING|SENT|DEAD], attempts, next_attempt_at, created_at, sent_at, last_error)`

SENT rows are retained for seven days by default and purged hourly. Configure
`pigeon.outbox.sent-retention-ms` and
`pigeon.outbox.sent-cleanup-interval-ms` to change this policy.

## Rabbit soft defaults (starter)
When unset, `RabbitConfirmDefaultsEnvironmentPostProcessor` sets:
`spring.rabbitmq.publisher-confirm-type=correlated`, `publisher-returns=true`, `template.mandatory=true`.
User config overrides.

## API
`Outbox.write(aggregateType, aggregateId, eventType, payloadJson|Object)` — call inside the caller's `@Transactional`.

Every failed publish stores a short reason in `last_error` (truncated to 1000 characters). It is cleared when the event is `SENT` and kept on `DEAD` rows, so a dead event explains itself. Retry delays are shortened by a random fraction of up to `pigeon.outbox.backoff-jitter` (default `0.2`) so events that failed together do not retry at the same instant; `max-backoff-ms` stays an upper bound.

Publish outcomes: **CONFIRMED** → SENT; **TRANSIENT** (broker down/timeout) → soft retry, attempts unchanged; **PERMANENT** (nack/unroutable) → attempts++ → DEAD after `max-attempts`.

Replay methods: `Outbox.replayDead(id)` / `replayAllDead()`. The HTTP replay
endpoints are demo-only: `POST /api/outbox/dead/{id}/replay`,
`POST /api/outbox/dead/replay-all`. Production apps should implement their own
administrative access policy.

## RabbitMQ defaults
- Exchange `pigeon.events` (topic)
- DLX `pigeon.dlx`, DLQ `pigeon.dlq`
- Routing key = `event_type`
- Header / messageId = outbox event id

## Metrics
`pigeon.outbox.pending`, `pigeon.outbox.lag.seconds`, `pigeon.outbox.dead`, `pigeon.outbox.dispatch.latency`, `pigeon.outbox.dispatched` (tag `result`)

## Demo (`pigeon-core`)
- Port `8081`
- `POST /api/orders`, `GET /api/orders/{id}`, `GET /api/ping`
- Demo queue `pigeon.orders` bound to `order.*`
