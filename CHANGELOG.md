# Changelog

## 0.1.0 - 2026-11-20

First release.

### What it is

A Spring Boot starter that implements the transactional outbox pattern for
PostgreSQL and RabbitMQ. `Outbox.write(...)` inserts an event row in the same
database transaction as your domain write. A background poller then publishes
it to RabbitMQ.

### Guarantees

- **Atomic write:** the event row commits or rolls back with your business data.
- **At-least-once delivery:** an event is not lost once committed, even if
  RabbitMQ is down at commit time. Each event carries a `messageId`.
- **Publisher confirms:** a row is marked `SENT` only after the broker confirms.
- **Retries and dead-lettering:** broker outages are soft-retried without
  consuming attempts. Permanent failures back off, then become `DEAD` and are
  routed to a DLQ. DEAD events can be replayed with `Outbox.replayDead(id)` and
  `Outbox.replayAllDead()`.
- **Multi-replica safe:** `SELECT ... FOR UPDATE SKIP LOCKED` with leased
  claims lets replicas take disjoint batches. A crashed claimer's rows become
  claimable again after `pigeon.outbox.claim-lease-ms`.
- **Isolated migrations:** the opt-in Flyway migrations (`V10000` to `V10002`) use their own location
  (`db/pigeon`) and history table (`pigeon_schema_history`).
- **Runs by default, on its own threads:** the dispatcher and the SENT-row cleanup start with the application and stop with it, without enabling `@EnableScheduling` for the host app. Set `pigeon.outbox.scheduling.enabled=false` for write-only instances.
- **Payload validation:** `Outbox.write(..., String)` checks that the text is valid JSON before writing and fails with a clear `IllegalArgumentException`, rather than a database error that aborts your transaction. The message does not echo the payload.
- **DEAD retention (opt-in):** `DEAD` rows are kept forever by default. Set `pigeon.outbox.dead-retention-ms` to purge them; the age is counted from when the event died (`dead_at`), not from creation, so a replayed event that dies again gets a fresh period.
- **Clean shutdown:** on stop, the dispatcher finishes the in-flight batch (waiting up to `confirm-timeout-ms` plus 5 s), then interrupts it. Events from a batch that ends early are handed back to `PENDING` immediately instead of waiting out `claim-lease-ms`.
- **Failure reasons:** `outbox_events.last_error` records why the latest publish failed, and survives on `DEAD` rows.
- **Jittered backoff:** retry delays are randomly shortened by up to `pigeon.outbox.backoff-jitter` (default `0.2`) so failed events do not retry in lockstep.
- **Operational logging:** quiet by default; set `logging.level.io.pigeon=DEBUG` to see claims and sends. Retries, reclaimed leases, DEAD events and replays are always logged.
- **Health indicator:** an optional Actuator `pigeonOutbox` health entry that reports `DOWN` when the dispatcher is stopped or the oldest undelivered event is older than `pigeon.outbox.health.max-lag-seconds` (default 300). Dead events are reported but do not fail it; write-only instances are always `UP`.
- **Metrics:** Micrometer gauges and timers for pending, lag, dead, dispatch
  latency and dispatch outcomes.

### Limits

- **Duplicates are possible.** A crash after the broker confirms but before the
  row is marked `SENT` republishes the event. Consumers must deduplicate by
  `messageId` and be idempotent.
- **No ordering guarantee**, including per aggregate. Consumers that need
  ordering must enforce it.
- **Supported stack only:** Java 21, Spring Boot 3 (built and tested against 3.5.16), PostgreSQL, RabbitMQ. Spring Boot 4 is not supported yet.
- **Demo replay endpoints are unauthenticated** and must not be exposed in
  production.
- No migration path from pre-release demo databases; recreate them.
- Distributed via JitPack; not published to Maven Central.
