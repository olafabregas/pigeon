# Failure Drills

Manual drills to verify Pigeon's reliability claims (transactional outbox, at-least-once delivery, idempotent consumers, no duplicate dispatch across replicas). Record outcomes in [LOAD_TEST_RESULTS.md](LOAD_TEST_RESULTS.md).

Prereqs: `docker compose up --build` is running; k6 is run via `loadtest/run.ps1` / `run.sh` (separate container).

Handy shortcut for SQL:

```bash
docker compose exec -T postgres psql -U pigeon -d pigeon -c "select status,count(*) from outbox_events group by status"
```

## Drill 1: RabbitMQ outage mid-test

Goal: orders keep being accepted while the broker is down; after recovery the outbox drains to 0 and no event is lost.

1. Baseline: record counts (see checklist below) and open Grafana (:3000).
2. Start load: `./loadtest/run.sh spike` (or `smoke` for a gentler run).
3. While running: `docker compose stop rabbitmq`.
4. Observe for ~30-60 s: `pigeon_outbox_pending` and `pigeon_outbox_lag_seconds` climb; order creation (201) continues because writes only touch Postgres. Dispatch attempts back off (`attempts`, `next_attempt_at` grow).
5. `docker compose start rabbitmq` (wait for healthy; mgmt UI at :15672).
6. Let load finish, then wait for the backlog to drain (backoff may delay a few seconds to minutes).
7. Verify:
   - `pigeon_outbox_pending` returns to `0` (Prometheus query or SQL below).
   - `pigeon_outbox_dead` is `0` (or any DEAD rows are explained and present in `pigeon.dlq`).
   - `count(orders) == count(outbox_events where event_type='order.created')`.
   - `outbox_events` with status `SENT` equals orders count.
   - Messages consumed (consumer-side processed count / RabbitMQ `pigeon.orders` delivered+acked totals) equals orders count. Redeliveries are allowed but consumer idempotency (`messageId` header) must keep effects at exactly one per order.

## Drill 2: Kill a core replica

Goal: the system survives losing a core instance without losing or duplicating events.

1. `docker compose up -d --scale core=3` (see Drill 3 for setup).
2. Start load: `./loadtest/run.sh spike`.
3. `docker compose ps core` to get container names; `docker kill <one core container>`.
4. Expect a brief burst of errors/timeouts for requests in flight to that replica; the breaker may open briefly and return 503 via `/fallback`. Gateway should keep serving via remaining replicas (depends on how service discovery / DNS resolution is configured; record what you actually observe).
5. After load: run the verification checklist. Rows left `PENDING` by the dead replica must be picked up by surviving replicas (no row stuck in-flight).
6. `docker compose up -d --scale core=3` to restore.

## Drill 3: Scale core to 3, no duplicate dispatch

Goal: multiple dispatchers do not publish the same outbox event twice.

1. `docker compose up -d --build --scale core=3`.
2. Run `./loadtest/run.sh spike`.
3. After the backlog drains, verify:
   - Each outbox id appears once in what was published/consumed: consumer-side processed count equals orders count; no duplicate `messageId` deliveries beyond what idempotency absorbs. Check RabbitMQ `pigeon.orders` publish-in total equals orders count (publish count > orders count indicates duplicate dispatch).
   - `select count(*) from outbox_events where attempts > 1` should be low/explainable (retries only after real failures).
   - In logs, the same event id is not logged as dispatched by two replicas: `docker compose logs core | grep <event-id>`.

## Verification checklist

SQL (run via `docker compose exec postgres psql -U pigeon -d pigeon`):

```sql
-- Orders vs outbox events (should match 1:1 for order.created)
select (select count(*) from orders) as orders,
       (select count(*) from outbox_events where event_type = 'order.created') as outbox_total;

-- Outbox status breakdown (PENDING should reach 0, DEAD should be 0)
select status, count(*) from outbox_events group by status;

-- Orders with no outbox event (must be 0: transactional atomicity)
select count(*) from orders o
where not exists (select 1 from outbox_events e where e.aggregate_id = o.id::text);

-- Retried or stuck events
select id, status, attempts, next_attempt_at, created_at
from outbox_events where status <> 'SENT' order by created_at limit 20;

-- Oldest pending age (seconds)
select coalesce(extract(epoch from now() - min(created_at)), 0) as lag_s
from outbox_events where status = 'PENDING';
```

(Adjust the `aggregate_id` cast if the column types differ in the final schema.)

curl / HTTP:

```bash
# Gauge values straight from the services
curl -s http://localhost:8081/actuator/prometheus | grep -E '^pigeon_outbox_(pending|lag_seconds|dead)'

# Prometheus query
curl -s 'http://localhost:9090/api/v1/query?query=pigeon_outbox_pending'

# RabbitMQ queue depths (guest/guest)
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/pigeon.orders
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/pigeon.dlq

# Create an order through the gateway
curl -i -X POST http://localhost:8080/api/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c1","amount":10.5}'
```

Note: port 8081 may not be published to the host in the compose file; use `docker compose exec core ...` or Prometheus if so. Metric names are the Micrometer names from the contract with Prometheus naming (`.` becomes `_`).

## Pass criteria

- No order accepted (201) without a matching outbox row.
- `pigeon_outbox_pending == 0` and `pigeon_outbox_dead == 0` after recovery.
- Consumed-effects count equals orders count (duplicates absorbed by idempotency).
- No duplicate dispatch with 3 replicas.
