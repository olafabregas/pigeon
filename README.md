# Pigeon

[![](https://jitpack.io/v/olafabregas/pigeon.svg)](https://jitpack.io/#olafabregas/pigeon)
![CI](https://github.com/olafabregas/pigeon/actions/workflows/ci.yml/badge.svg)

**Transactional outbox for Spring Boot + PostgreSQL + RabbitMQ.**

Add the starter, write one line in your database transaction, and Pigeon publishes to RabbitMQ with confirms, retries, and a dead-letter queue — even if the broker is down when you commit.

## Why "Pigeon"?

Carrier pigeons keep trying until the message arrives. Pigeon does the same for domain events: your business row and the outbox row commit together; a background poller delivers to RabbitMQ with backoff until confirmed.

```mermaid
flowchart LR
    App["@Transactional<br/>Business + Outbox.write()"] -->|1. Atomic commit| PG[(PostgreSQL<br/>outbox_events)]
    PG -->|2. SKIP LOCKED poll| Poller["Pigeon Poller"]
    Poller -->|3. Confirmed publish| RMQ["RabbitMQ"]
    RMQ -->|4. Deliver| Listener["Your @RabbitListener"]
```

Full component breakdown, schema, and delivery semantics: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Where it fits

Any time you save something in Postgres and **must** notify another system afterward — without dual-write bugs or silent loss when RabbitMQ is down:

| Domain | Example domain write | Example `eventType` | Typical listener |
|---|---|---|---|
| **Orders / ecommerce** | Create order | `order.created` | Email, fulfillment, inventory |
| **Inventory / rentals** | Reserve or return stock | `rental.reserved`, `stock.adjusted` | Alerts, accounting sync |
| **SaaS signup** | Create user or org | `user.registered` | Welcome email, provision tenant |
| **Partner webhooks** | Status change | `shipment.updated` | Signed outbound webhook |
| **Bookings** | Book appointment | `booking.confirmed` | SMS, calendar |
| **Search / projections** | Update write model | `listing.updated` | Elasticsearch indexer |

Pigeon does **not** send the email, call the webhook, or talk to ChatGPT — you write the `@RabbitListener`. Delivery is **at least once**: if the process crashes after RabbitMQ confirms a message but before Pigeon marks the row `SENT`, the message is published again. Consumers must deduplicate by the event `messageId` and make side effects idempotent.

Pigeon does not guarantee per-aggregate ordering. Multiple replicas and
`SKIP LOCKED` dispatch batches may publish events for the same aggregate in a
different order. Consumers that require ordering must enforce it explicitly.

## Use it in your app

Pigeon is distributed through [JitPack](https://jitpack.io/), which builds
directly from the Git tag. Add the repository and the dependency:

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.github.olafabregas.pigeon</groupId>
  <artifactId>pigeon-spring-boot-starter</artifactId>
  <version>0.1.0</version>
</dependency>
```

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/app
  rabbitmq:
    host: localhost
  flyway:
    locations: classpath:db/migration
pigeon:
  outbox:
    enabled: true
    scheduling:
      enabled: true
    sent-retention-ms: 604800000
    claim-lease-ms: 60000
    flyway:
      enabled: true
      locations: classpath:db/pigeon
      table: pigeon_schema_history
```

> **Required:** `pigeon.outbox.scheduling.enabled: true`. It has no default, and
> without it the dispatcher never starts, so events are written to the outbox
> but never published.

Pigeon soft-defaults `publisher-confirm-type: correlated`, `publisher-returns: true`, and `template.mandatory: true` when you have not set them. Explicit values in your `application.yml` always win.

> **Existing demo databases:** the new Pigeon migration location does not
> migrate historical `flyway_schema_history` state from the old starter `V1`.
> For a throwaway demo database, run `docker compose down -v` before starting
> the updated stack.

The starter migration is opt-in. It runs separately from Spring Boot Flyway
from `classpath:db/pigeon`, using `pigeon_schema_history`, so it does not
compete with an application's `classpath:db/migration/V1__...` migration.
Set `pigeon.outbox.flyway.enabled=false` to manage Pigeon SQL yourself.
Confirmed `SENT` rows are retained for seven days by default and then purged
hourly. Adjust `sent-retention-ms` or set it to `0` for immediate cleanup.

```java
@Service
public class OrderService {
  private final Outbox outbox;
  private final JdbcTemplate jdbc;

  @Transactional
  public UUID create(String customerId, BigDecimal amount) {
    UUID id = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO orders (id, customer_id, amount, status) VALUES (?,?,?,?)",
        id, customerId, amount, "CREATED");
    outbox.write("order", id.toString(), "order.created",
        Map.of("orderId", id.toString(), "customerId", customerId, "amount", amount));
    return id;
  }
}
```

Then add your own `@RabbitListener` for side effects. Bind a queue to exchange `pigeon.events` with the routing keys you use as `eventType` (e.g. `order.*`).

Same pattern for other domains — only the aggregate type, event name, and payload change:

```java
outbox.write("rental", itemId, "rental.reserved", payload);
outbox.write("user", userId, "user.registered", payload);
outbox.write("shipment", shipmentId, "shipment.updated", payload);
```

### Broker down and DEAD events

- **Broker unavailable / confirm timeout** → event stays `PENDING` (soft retry). Attempts are **not** burned.
- **Permanent failure** (nack, unroutable) → attempts increment → `DEAD` after `max-attempts`, plus DLQ.
- **Replay DEAD:** `Outbox.replayDead(id)` / `Outbox.replayAllDead()` (demo HTTP below).

The replay HTTP endpoints are demo-only, are not authenticated, and should
not be exposed in production.

The dispatcher claims rows in a short database transaction, releases the row
locks before waiting for RabbitMQ confirms, and finalizes each event in its own
transaction. A claimed row is leased for `pigeon.outbox.claim-lease-ms`
(default 60 seconds) and becomes claimable again if a process crashes.

## Logging

The success path is quiet by default. Polling runs every 500 ms, so per-batch lines
are `DEBUG`. To see the happy path, set:

```yaml
logging:
  level:
    io.pigeon: DEBUG
```

| Level | Message | Meaning |
|---|---|---|
| INFO | `pigeon outbox started: exchange=… batchSize=… pollIntervalMs=…` | Dispatcher is running |
| WARN | `pigeon outbox is NOT dispatching: set pigeon.outbox.scheduling.enabled=true` | Scheduling is off; nothing will be published |
| DEBUG | `claimed N outbox event(s) token=…` | A batch was claimed |
| DEBUG | `event ID sent type=… attempts=… latencyMs=…` | Broker confirmed the publish |
| WARN | `event ID publish failed (attempt N/MAX), retrying in MS ms` | Permanent failure, backing off |
| ERROR | `outbox event ID DEAD after N attempts …` | Retries exhausted; event is `DEAD` and dead-lettered |
| WARN | `reclaimed N expired PROCESSING event(s)` | An instance crashed or stalled mid-batch; its rows were taken over |
| INFO | `replayed dead event ID` / `replayed N dead event(s)` | Someone replayed DEAD events |

## Try the demo

```bash
docker compose up --build
```

The API port is assigned by Compose. Retrieve it with:

```powershell
docker compose port core 8081
```

| What | URL |
|---|---|
| Demo API | the host/port printed by `docker compose port core 8081` |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| RabbitMQ | http://localhost:15672 (`guest` / `guest`) |

**PowerShell** (recommended on Windows — `curl` is aliased to `Invoke-WebRequest`):

```powershell
Invoke-RestMethod -Method Post http://localhost:<core-port>/api/orders `
  -ContentType application/json `
  -Body '{"customerId":"c1","amount":10.5}'
```

**curl.exe:**

```bash
curl.exe -X POST http://localhost:<core-port>/api/orders \
  -H "Content-Type: application/json" \
  -d "{\"customerId\":\"c1\",\"amount\":10.5}"
```

Replay all DEAD events (demo):

```powershell
Invoke-RestMethod -Method Post http://localhost:<core-port>/api/outbox/dead/replay-all
```

```bash
curl.exe -X POST http://localhost:<core-port>/api/outbox/dead/replay-all
```

### Scale the demo

```bash
docker compose up --build --scale core=3
```

Scaled replicas use Compose-assigned host ports, so all three can start.
Use `docker compose port core 8081` to find a replica's port. `SKIP LOCKED`
lets replicas claim disjoint outbox batches.

Hands-on checklist: [docs/MANUAL_TEST.md](docs/MANUAL_TEST.md).

## What you get

- `Outbox.write(...)` in the same transaction as your domain write
- Flyway migration for `outbox_events` under `db/pigeon`, isolated from app migrations
- In-process poller with leased `SELECT ... FOR UPDATE SKIP LOCKED` claims
- RabbitMQ publisher confirms; broker-down soft-retries; permanent failures → backoff → `DEAD` + DLQ
- At-least-once delivery with an event `messageId`; consumers must be idempotent
- Replay DEAD events via demo-only API / `Outbox.replay*`
- Micrometer metrics: `pending`, `lag`, `dead`, dispatch latency, `dispatched{sent|deferred|failed|dead}`

## Stack

Java 21, Spring Boot 3, PostgreSQL, RabbitMQ. That is the supported surface for v1.

## License

Apache License 2.0 — see [LICENSE](LICENSE). Copyright 2026 Olafabregas.
