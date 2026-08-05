# TaskMQ Operations Notes

## Test / CI extras

| Surface | How |
|---|---|
| Unit + race + **coverage gates** | CI `unit` job; floors live in [`coverage.yaml`](../coverage.yaml) |
| Coverage HTML | `make cover` → `coverage.html` (also CI artifact) |
| Integration (standalone Redis) | CI `integration` job |
| Integration **race** | Nightly / `workflow_dispatch` job `integration-race` |
| **Redis Cluster** smoke | Nightly / manual: `docker compose -f docker-compose.cluster.yml up -d` then `TASKMQ_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test ./tests/integration/ -run ClusterHashTag` |

Cluster is optional; TaskMQ keys are hash-tagged (`taskmq:{queue}:…`) so multi-key Lua stays single-slot.

### Coverage settings (`coverage.yaml`)

| Field | Meaning |
|---|---|
| `mode` | `go test -covermode` (`atomic` for race-safe counting) |
| `packages` | Import path → **minimum** statement coverage % (CI fails if below) |
| `report` | Package patterns for the merged profile |
| `exclude` | Path substrings stripped from the merged profile (generated/proto/mocks) |
| `overall_min` | Optional whole-report floor (`0` = off) |
| `profile` / `html` | Output files (gitignored) |

Raise thresholds only after adding tests. Do not lower gates without an explicit reason in the PR.

## Graceful shutdown

Workers wait for in-flight handlers up to `taskmq.shutdown_timeout` (default **30s**,
aligned with default task timeout). Configure via YAML or `TASKMQ_TASKMQ_SHUTDOWN_TIMEOUT`.

If handlers regularly run longer than this, either raise `shutdown_timeout` or lower
task `timeout_ms`. Stopping too early leaves messages in the PEL until the janitor
reclaims them (at-least-once may re-run the handler).

## Lifecycle / admission (production)

`config.prod.yaml` enables recommended hard limits. Tune to Redis memory and traffic:

| Setting | Effect |
|---|---|
| `enqueue_soft_limit` | Soft signal / metrics when stream grows |
| `enqueue_hard_limit` | Reject new enqueues when `XLEN` >= limit |
| `delayed_max_count` | Cap delayed ZSET cardinality |
| `delayed_overflow` | `reject` (default) or `drop_farthest` |
| `dlq_max_count` / `dlq_max_age` | Cap and age-out dead letters |
| `max_payload_bytes` | Reject oversized payloads |
| `safe_trim_*` | MINID stream trimming janitor |

### Process-local lifecycle counters

| Endpoint | Format |
|---|---|
| `GET /api/lifecycle/metrics` | JSON snapshot map |
| `GET /api/lifecycle/metrics?format=prometheus` | Prometheus text (`taskmq_lifecycle_*`) |
| `GET /api/lifecycle/metrics?format=prometheus&scope=queue` | Queue Redis metrics + process-local |

Notes:

- Lifecycle counters are **process-local** (per server instance).
- Optional `lifecycle.MetricsSink` can mirror increments without a Prometheus client dependency.

### Redis-backed queue metrics (cross-process)

| Endpoint | Format |
|---|---|
| `GET /api/metrics/queues` | JSON counters + depths for all discovered queues |
| `GET /api/metrics/queues/:queue` | Single queue |
| `GET /api/metrics/queues?format=prometheus` | `taskmq_queue_*` counters/gauges |

Fields include `processed_total`, `failed_total` / `dlq_total`, `retried_total`, plus live
`stream_len` / `delayed` / `dlq` gauges.

### Which metrics to scrape

| Source | Scope | Use for |
|---|---|---|
| `GET /api/lifecycle/metrics` | **Process-local** admission/janitor counters | Soft/hard reject rates, safe-trim activity on **this** instance |
| `GET /api/metrics/queues` | **Redis-backed** per-queue HASH + depths | Cluster-wide throughput, DLQ/retry totals, queue backlog |
| `lifecycle.MetricsSink` | In-process mirror of lifecycle counters | Embed Prometheus client without scraping HTTP |

Do **not** sum lifecycle process counters across replicas as if they were global queue truth — use `metricsq` for cross-process totals. Lifecycle metrics answer “is **this** worker rejecting / trimming?”.

### Recommended scrape setup

```text
# Per process (admission / soft-hard reject / safe-trim activity)
scrape  GET /api/lifecycle/metrics?format=prometheus

# Per queue cluster-wide (throughput, DLQ, backlog depths)
scrape  GET /api/metrics/queues?format=prometheus
```

Alert ideas:

- Rising `taskmq_lifecycle_enqueue_hard_rejected` → raise capacity or scale consumers.
- Growing `stream_len` / `delayed` gauges from queue metrics → lag / scheduler pressure.
- Spike of `stalled` events (HTTP `/api/queues/:q/events`) → workers crashing or blocked handlers.

### Observability write budget

Every settled task may touch (best-effort, non-authoritative for correctness):

| Channel | Redis type | Cost control |
|---|---|---|
| Task **meta** | HASH per task id | Success deletes meta when `completed_retention=0` (default). Set retention only when inspect/results are required. |
| **Events** stream | `XADD` ~MAXLEN approx | Default 10k (`events.DefaultMaxLen`). Override with `taskmq.events_max_len` (YAML) / `client.WithEventsMaxLen` / env `TASKMQ_TASKMQ_EVENTS_MAX_LEN`. High-churn queues: treat as a recent feed, not a full audit log. |
| **metricsq** HASH | `HINCRBY` per outcome | Cheap; prefer over scanning streams for dashboards. |
| **Heartbeat** keys | SET + TTL per consumer | ~every 10s / TTL 30s; deleted on stop. |

Settlement (XACK/XDEL, delayed ZADD, DLQ) must succeed even if meta/events/metrics fail. Do not add synchronous external exporters on the hot path — use `MetricsSink` or scrape HTTP.

## Lifecycle defaults (bare topology vs config)

| Construction path | Lifecycle source | When to use |
|---|---|---|
| `taskmq.Module` / `LifecycleFromConfig` | YAML + `DefaultLifecycleConfig` overlay | **Production** (shared client + workers) |
| `BuildWorkerTopologyWithLifecycle(lc)` | Caller-supplied `lc` | Library embed with shared LC |
| `BuildWorkerTopology` (no LC arg) | `DefaultLifecycleConfig()` only | Tests / tools — **not** production |

`lifecycle.FromConfig(nil)` and empty YAML match `DefaultLifecycleConfig` for DLQ cap (1000), SafeTrim on, cancelled TTL 24h, purge cancelled delayed on. Production hard limits (`enqueue_hard_limit`, etc.) only apply when set in config — bare topology will **not** pick them up. Always inject the same `*Lifecycle` into client and workers.

## Redis connection modes

`internal/redis.NewRedisClient` returns `redis.UniversalClient` and supports:

| `redis.mode` | Required settings | Notes |
|---|---|---|
| `standalone` (default) | `redis.addr` or first `redis.addrs` | `redis.db` honored |
| `cluster` | `redis.addrs` seed nodes | `db` ignored by Redis Cluster |
| `sentinel` | `redis.master_name` + `redis.addrs` (sentinels) | Failover client |

Example cluster:

```yaml
redis:
  mode: cluster
  addrs: ["10.0.0.1:6379", "10.0.0.2:6379"]
  password: ""
```

### Cluster key safety

- **Key design**: all queue keys use hash tags `taskmq:{queue}:…` so multi-key Lua
  scripts stay on one slot (see `internal/taskmq/keys` and [LUA_SCRIPTS.md](LUA_SCRIPTS.md)).
- **Schema guard**: `./scripts/check_keys_schema.sh` fails if production code invents
  raw `taskmq:{` key strings outside the `keys` package.
- Prefer integration tests against a multi-node Cluster before production cutover.

## Shared Lifecycle

In production, construct Client and Workers with the **same** `*lifecycle.Lifecycle`
instance (Fx `taskmq.Module` does this). Separate construction uses independent
admission counters and can disagree on limits.

Prefer:

- `taskmq.Module` / `BuildWorkerTopologyWithLifecycle` + `client.WithClientLifecycle(sameLC)`
- Avoid bare `worker.BuildWorkerTopology` in production (it uses `DefaultLifecycleConfig`
  and is not shared with any Client).

## API surfaces (gRPC vs HTTP vs CLI)

| Capability | gRPC (`TaskMQService`) | HTTP admin | CLI |
|---|:---:|:---:|:---:|
| Enqueue / EnqueueIn / EnqueueAt | ✅ | ✅ (test console) | — |
| RegisterCron | ✅ | ✅ | — |
| DLQ list / delete / retry | ✅ | ✅ | ✅ |
| Pause / Resume queue | ✅ `PauseQueue` / `ResumeQueue` / `IsQueuePaused` | ✅ | ✅ |
| Scheduled / active inspect | ✅ `ListScheduledTasks` / `ListActiveTasks` | ✅ | ✅ `task scheduled` / `task active` |
| Cancel task | ✅ `CancelTask` | ✅ | ✅ `task cancel` |
| Get task meta by id | ✅ `GetTask` | ✅ `/api/queues/:q/tasks/:id` | ✅ `task get` |
| Update progress | — | ✅ `POST .../tasks/:id/progress` | — |
| Bulk enqueue | ✅ `EnqueueBulk` | ✅ `POST .../bulk` | — |
| Queue events | — | ✅ `/api/queues/:q/events` | ✅ `events` |
| Live workers | — | ✅ `/api/queues/:q/workers` | ✅ `workers` |
| Lifecycle metrics | — | ✅ `/api/lifecycle/metrics` | — (process-local; scrape HTTP) |
| Queue Redis metrics | — | ✅ `/api/metrics/queues` | ✅ `metrics [queue]` |

## Task metadata & completed retention

- On enqueue, TaskMQ writes `taskmq:{queue}:meta:{id}` (state, retries, errors, …).
- On success: if `taskmq.completed_retention` is **0** (default), meta is **deleted** (Streams fire-and-forget).
- If `completed_retention` > 0, meta moves to `state=completed`, optional `result` is stored, and entries expire / are capped by `completed_max_count`.
- Handlers may set result via `ConsumeContext.SetResult` (or `task.Result` before complete).
- Permanent failures: return `task.SkipRetry(err)` or `task.Unrecoverable(err)` to skip retries and go to DLQ.
- Absolute deadline: set `DeadlineMs` / `WithTaskDeadline`; effective run limit is `min(Timeout, Deadline-now)`.

## Bulk enqueue

```go
res, err := client.EnqueueBulk(ctx, tasks) // pipelines same-queue non-unique tasks
// res.Succeeded, res.FailedIndexes
```

HTTP: `POST /api/queues/:queue/bulk` with `{"tasks":[{"name":"...","payload":"..."}],"fail_fast":false}`.  
gRPC: `EnqueueBulk`.

## Queue events

Lifecycle events are appended to stream `taskmq:{queue}:events` (approx MAXLEN 10000):

| Type | Emitted |
|---|---|
| `enqueued` | Client enqueue paths |
| `active` | Worker starts processing |
| `completed` / `failed` / `retry` / `delayed` | Broker settlement side effects |
| `progress` | Handler / external progress updates |
| `cancelled` | Cancel-before-run gate |
| `stalled` | PEL recovery janitor after `XAutoClaim` reclaims an idle pending message (best-effort; includes `delivery_count` + stream id in error/data field) |

```bash
curl localhost:8080/api/queues/default/events?limit=50
go run cmd/taskmq-cli/main.go events default 20
```

## Worker heartbeats

Each pool refreshes `taskmq:{queue}:heartbeat:{consumer}` every ~10s (TTL 30s).

```bash
curl localhost:8080/api/queues/default/workers
go run cmd/taskmq-cli/main.go workers default
```

## Progress

Handlers with middleware access: `c.UpdateProgress(50, "halfway")`.  
External: `client.UpdateProgress(ctx, queue, id, 50, "halfway")` or HTTP POST.

**API split:** gRPC covers **multi-language producer + DLQ + core ops** (pause,
cancel, scheduled/active list, get task). Dashboard, events feed, worker inventory,
and Prometheus scrape stay on **HTTP** (and CLI mirrors the main ops). Metrics are
intentionally not on gRPC so scrapers keep a single HTTP path.

## Message terminal outcomes & handler errors

See [TERMINAL_OUTCOMES.md](TERMINAL_OUTCOMES.md) for:

- Full outcome matrix (success / retry / DLQ / cancel / rate-limit / corrupt / PEL)
- **Handler control** errors (`task.SkipRetry` / `Unrecoverable`) vs **settlement** (`worker.Handled` / `Abort`)
- Test coverage map and rules for adding new outcomes

Handlers must never XACK/XDEL or call broker settlement APIs.
