# Manual test checklist

```powershell
cd C:\Users\ayori\Projects\pigeon
docker compose down -v
docker compose up --build -d
```

Wait until `docker compose ps` shows healthy. API: **http://localhost:8081**

## 1. Happy path

```powershell
Invoke-RestMethod -Method Post http://localhost:8081/api/orders `
  -ContentType application/json -Body '{"customerId":"me","amount":12.5}'
```

Expect **201**. After a couple seconds:

```powershell
docker compose exec -T postgres psql -U pigeon -c "select status, count(*) from outbox_events group by 1"
```

Expect **SENT**. RabbitMQ UI: `pigeon.orders` near 0.

## 2. Broker outage

```powershell
docker compose stop rabbitmq
1..5 | ForEach-Object {
  Invoke-RestMethod -Method Post http://localhost:8081/api/orders `
    -ContentType application/json -Body '{"customerId":"drill","amount":1}'
  Start-Sleep -Seconds 1
}
docker compose exec -T postgres psql -U pigeon -c "select status, count(*) from outbox_events group by 1"
docker compose start rabbitmq
Start-Sleep -Seconds 20
docker compose exec -T postgres psql -U pigeon -c "select status, count(*) from outbox_events group by 1"
```

New rows stay **PENDING** while Rabbit is down (attempts are **not** burned for broker-down). They become **SENT** after it returns.

Grafana: `dispatched{result="deferred"}` rises during the outage; `dead` should not climb from a short stop.

### Replay DEAD (if any from earlier permanent failures)

```powershell
Invoke-RestMethod -Method Post http://localhost:8081/api/outbox/dead/replay-all
```

Or one id: `POST /api/outbox/dead/{id}/replay`

## 3. Scale

```powershell
docker compose up -d --scale core=3
# create orders slowly, then:
docker compose exec -T postgres psql -U pigeon -c "select count(*) from orders; select count(*) from outbox_events where status='SENT'; select count(*) from processed_messages;"
```

## Tear down

```powershell
docker compose down -v
```
