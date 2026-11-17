# Load Test Results

No benchmark results are committed yet. Run the current smoke or spike
scenario, then record the generated k6 summary and environment details here.

## Environment

| Item | Value |
|---|---|
| Date | `<YYYY-MM-DD>` |
| Commit | `<git sha>` |
| Host (CPU / RAM / OS) | `<fill in>` |
| Docker version / resources | `<fill in>` |
| Core replicas | `<n>` |
| API base URL | value passed through `BASE_URL` |
| k6 version | `<fill in>` |

## Scenarios

| Script | Command | Peak RPS | Duration |
|---|---|---|---|
| smoke | `./loadtest/run.sh smoke` | low | 30s |
| spike | `PEAK_RPS=<n> ./loadtest/run.sh spike` | configured by `PEAK_RPS` | 2m 40s |

## Results

| Metric | smoke | spike |
|---|---|---|---|
| Total requests | `<>` | `<>` | `<>` |
| Achieved RPS | `<>` | `<>` | `<>` |
| 201 count | `<>` | `<>` |
| 5xx count | `<>` | `<>` |
| p50 latency | `<>` | `<>` |
| p95 latency | `<>` | `<>` |
| p99 latency | `<>` | `<>` |
| Thresholds passed | `<yes/no>` | `<yes/no>` |

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
