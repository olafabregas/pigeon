# Load Test Results

> **Status: template. No measurements have been taken yet.** Fill each placeholder from real runs; do not commit estimated numbers.

## Environment

| Item | Value |
|---|---|
| Date | `<YYYY-MM-DD>` |
| Commit | `<git sha>` |
| Host (CPU / RAM / OS) | `<fill in>` |
| Docker version / resources | `<fill in>` |
| Core replicas | `<n>` |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | `<fill in>` / `<fill in>` |
| k6 version | `<fill in>` |

## Scenarios

| Script | Command | Peak RPS | Duration |
|---|---|---|---|
| smoke | `./loadtest/run.sh smoke` | `<fill in>` | `<fill in>` |
| spike | `PEAK_RPS=<n> ./loadtest/run.sh spike` | `<fill in>` | `<fill in>` |
| breaker | `./loadtest/run.sh breaker` | `<fill in>` | `<fill in>` |

## Results

| Metric | smoke | spike | breaker |
|---|---|---|---|
| Total requests | `<>` | `<>` | `<>` |
| Achieved RPS | `<>` | `<>` | `<>` |
| 201 count | `<>` | `<>` | n/a |
| 429 count (expected) | `<>` | `<>` | `<>` |
| 5xx count | `<>` | `<>` | `<>` |
| 503 (breaker fallback) | n/a | `<>` | `<>` |
| p50 latency | `<>` | `<>` | `<>` |
| p95 latency | `<>` | `<>` | `<>` |
| p99 latency | `<>` | `<>` | `<>` |
| Thresholds passed | `<yes/no>` | `<yes/no>` | `<yes/no>` |

## Outbox health

| Metric | Value |
|---|---|
| Peak `pigeon_outbox_pending` | `<>` |
| Peak `pigeon_outbox_lag_seconds` | `<>` |
| Time to drain to 0 after load | `<>` |
| Final `pigeon_outbox_dead` | `<>` |
| Orders in DB | `<>` |
| Outbox events SENT | `<>` |
| Messages consumed | `<>` |

## Drills (see [DRILLS.md](DRILLS.md))

| Drill | Outcome | Events lost | Duplicates | Notes |
|---|---|---|---|---|
| RabbitMQ stopped mid-test | `<>` | `<>` | `<>` | `<>` |
| Core replica killed | `<>` | `<>` | `<>` | `<>` |
| Scale core to 3 | `<>` | `<>` | `<>` | `<>` |

## Observations / bottlenecks

`<fill in after runs>`

## Screenshots

`<Grafana dashboard screenshots>`
