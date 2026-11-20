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
- **Isolated migrations:** the opt-in Flyway migration uses its own location
  (`db/pigeon`) and history table (`pigeon_schema_history`).
- **Metrics:** Micrometer gauges and timers for pending, lag, dead, dispatch
  latency and dispatch outcomes.

### Limits

- **Duplicates are possible.** A crash after the broker confirms but before the
  row is marked `SENT` republishes the event. Consumers must deduplicate by
  `messageId` and be idempotent.
- **No ordering guarantee**, including per aggregate. Consumers that need
  ordering must enforce it.
- **Supported stack only:** Java 21, Spring Boot 3, PostgreSQL, RabbitMQ.
- **Demo replay endpoints are unauthenticated** and must not be exposed in
  production.
- No migration path from pre-release demo databases; recreate them.
- Distributed via JitPack; not published to Maven Central.
