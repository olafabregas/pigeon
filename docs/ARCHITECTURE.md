# Architecture

Pigeon is a **Spring Boot transactional outbox** for PostgreSQL → RabbitMQ.

```mermaid
flowchart TB
  subgraph app [Your Spring Boot app]
    Domain["Domain @Transactional"]
    Write["Outbox.write"]
    Poller["SKIP LOCKED poller"]
    Domain --> Write
    Write --> Poller
  end
  PG[(PostgreSQL outbox_events)]
  RMQ[RabbitMQ]
  Consumer["Your @RabbitListener"]

  Write --> PG
  Poller --> PG
  Poller --> RMQ
  RMQ --> Consumer
```

## Components

- **`pigeon-spring-boot-starter`** — product. Auto-configures `Outbox`, repository, dispatcher, Rabbit exchange/DLQ, metrics, Flyway `outbox_events`.
- **`pigeon-core`** — demo orders API that uses the starter. Not required once you depend on the starter yourself.

## Delivery guarantee

1. Domain row + outbox row commit in one ACID transaction.
2. Poller claims batches with `FOR UPDATE SKIP LOCKED` (safe under multiple replicas).
3. Publish with correlated confirms; success → `SENT`; failure → backoff via `next_attempt_at`; max attempts → `DEAD` + DLX.

HTTP gateways / rate limiters are out of scope for this project.
