# Changelog

## 0.1.1 - 2026-11-21

A hardening release: easier to run, easier to operate, safer at shutdown.

### Upgrade notes

- **The dispatcher now starts by default.** It used to require
  `pigeon.outbox.scheduling.enabled=true`. If an instance should only write
  events and leave publishing to others, set `scheduling.enabled=false`.
- **Two new migrations, `V10001` and `V10002`,** add the `last_error` and
  `dead_at` columns. They run automatically when `pigeon.outbox.flyway.enabled`
  is true. If you manage Pigeon's SQL yourself, apply them before upgrading, or
  the dispatcher will fail.
- **Code that constructs `OutboxProperties` or `PublishBatchResult` by hand**
  needs updating; their constructors gained fields. The property keys you write
  in `application.yml` are unchanged. `OutboxScheduler` is now a top-level
  class instead of nested in `OutboxAutoConfiguration`.

### Added

- **Failure reasons:** `outbox_events.last_error` records why the latest publish
  failed, and survives on `DEAD` rows until a replay succeeds.
- **Jittered backoff:** retry delays are randomly shortened by up to
  `pigeon.outbox.backoff-jitter` (default `0.2`) so failed events do not retry
  in lockstep.
- **Health indicator:** an optional Actuator `pigeonOutbox` entry that reports
  `DOWN` when the dispatcher is stopped or the oldest undelivered event is older
  than `pigeon.outbox.health.max-lag-seconds` (default 300). Dead events are
  reported but do not fail it; write-only instances are always `UP`.
- **DEAD retention (opt-in):** `DEAD` rows are kept forever by default. Set
  `pigeon.outbox.dead-retention-ms` to purge them, counted from when the event
  died (`dead_at`), so a replayed event that dies again gets a fresh period.
- **Operational logging:** quiet by default; set `logging.level.io.pigeon=DEBUG`
  to see claims and sends. Retries, reclaimed leases, released claims, DEAD
  events, purges and replays are always logged.
- **Payload validation:** `Outbox.write(..., String)` checks that the text is
  valid JSON before writing and fails with a clear `IllegalArgumentException`
  instead of a database error that aborts your transaction. The message does not
  echo the payload.

### Changed

- **Runs on its own threads:** the dispatcher and the `SENT`-row cleanup are a
  lifecycle component that starts and stops with the application. It no longer
  needs `@EnableScheduling`, so it does not change how your own scheduled jobs
  behave.
- **Clean shutdown:** on stop, the dispatcher finishes the in-flight batch
  (waiting up to `confirm-timeout-ms` plus 5 s), then interrupts it. Events from
  a batch that ends early go straight back to `PENDING` instead of waiting out
  `claim-lease-ms`.
- **Built and tested against Spring Boot 3.5.16** (was 3.3.5). Spring Boot 4 is
  not supported yet.

### Fixed

- **`pigeon.outbox.flyway.locations` and `flyway.table` were ignored.** The
  nested `flyway.*` keys did not bind to the properties, so custom values
  silently fell back to the defaults.

### Limits

Unchanged from 0.1.0, except the supported stack is now Java 21, Spring Boot 3
(tested on 3.5.16), PostgreSQL and RabbitMQ.

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
