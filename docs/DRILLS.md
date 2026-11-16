# Failure drills

Run against the demo Compose stack (`core` on http://localhost:8081).

## 1. Stop RabbitMQ mid-flight

1. `docker compose stop rabbitmq`
2. Create several orders via `POST /api/orders`
3. Confirm new `outbox_events` are `PENDING` (DEAD should not climb from a short outage)
4. `docker compose start rabbitmq`
5. Wait; confirm those rows become `SENT` and `pigeon.dlq` stays empty
6. `orders` count should match successful creates; `processed_messages` distinct ids should match consumed events

**Note:** You do **not** need `docker compose down -v` for this drill. An old volume only keeps leftover DEAD/SENT history; it does not block new PENDING → SENT. SENT only advances when publisher confirms are on (`spring.rabbitmq.publisher-confirm-type=correlated`). After a broker restart, the publisher resets the cached connection on confirm timeouts so the backlog drains without a manual `core` restart.

## 2. Scale core to 3

1. `docker compose up -d --scale core=3`
2. Create many orders
3. Assert no duplicate `message_id` in `processed_messages` vs `SENT` outbox rows

## 3. Kill one core replica

1. With `--scale core=3`, `docker compose stop` one core container
2. Traffic to remaining replicas should still accept orders and drain the outbox

## SQL checks

```sql
SELECT status, count(*) FROM outbox_events GROUP BY 1;
SELECT count(*) AS orders FROM orders;
SELECT count(*) AS processed, count(distinct message_id) AS distinct_ids FROM processed_messages;
```
