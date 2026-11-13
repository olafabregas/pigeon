# Pigeon shared contract (all agents must follow)

Repo root: parent `pom.xml` (io.pigeon, Spring Boot 3.3.5, Cloud 2023.0.3, Java 21). Modules: `pigeon-core`, `pigeon-gateway`. Each agent edits ONLY its own paths.

## Ports / hosts (docker network names)
- gateway: 8080 (public). core: 8081. postgres:5432 (db/user/pass `pigeon`). redis:6379. rabbitmq:5672 (mgmt 15672, guest/guest).
- Prometheus 9090, Grafana 3000.

## Core API (pigeon-core)
- `POST /api/orders` body `{"customerId":"c1","amount":10.5}` -> 201 `{"id":"<uuid>",...}`. Writes `orders` row + `outbox_events` row in ONE transaction.
- `GET /api/orders/{id}`, `GET /api/ping` -> 200 `pong`, `GET /api/slow?ms=2000` (for circuit breaker demo), `GET /api/fail` -> 500.

## Outbox
- Table `outbox_events(id uuid pk, aggregate_type, aggregate_id, event_type, payload jsonb, status varchar [PENDING|SENT|DEAD], attempts int, next_attempt_at timestamptz, created_at, sent_at)`.
- Exchange `pigeon.events` (topic), routing key = event_type (e.g. `order.created`), queue `pigeon.orders`, DLX `pigeon.dlx`, DLQ `pigeon.dlq`. Dispatcher publishes with publisher confirms; exponential backoff via `next_attempt_at`; DEAD after max attempts and routed to DLQ.
- Message header `messageId` = outbox event id (consumer idempotency key).

## Metrics (Micrometer names)
- `pigeon.outbox.pending` (gauge), `pigeon.outbox.lag.seconds` (gauge, age of oldest PENDING), `pigeon.outbox.dead` (gauge), `pigeon.outbox.dispatch.latency` (timer), `pigeon.outbox.dispatched` (counter tag result), `pigeon.gateway.rate_limit.rejected` (counter, gateway), Resilience4j circuit breaker metrics default.
- Actuator `/actuator/prometheus` on both services.

## Gateway
- Routes `/api/**` -> `http://core:8081` (env `CORE_URL`, default `http://localhost:8081`). Redis token-bucket via custom atomic Lua script (env `RATE_LIMIT_RPS` default 10, `RATE_LIMIT_BURST` default 20), key per client IP; 429 + `Retry-After`. Resilience4j circuit breaker with fallback `/fallback` returning 503 JSON. Correlation id header `X-Correlation-Id`.

## Env for services
`DB_URL, DB_USER, DB_PASS, REDIS_HOST, RABBIT_HOST, CORE_URL` with localhost defaults.
