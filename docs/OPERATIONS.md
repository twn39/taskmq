# TaskMQ Operations Notes

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

Process-local counters are exposed at **`GET /api/lifecycle/metrics`** (JSON snapshot).
Wire Prometheus scrapers to that endpoint or adapt the snapshot map into your metrics stack.

## Redis Cluster

- **Key design** is Cluster-safe: all queue keys use hash tags `taskmq:{queue}:…`
  so multi-key Lua scripts stay on one slot.
- **Process client** today is `*redis.Client` (standalone). Point `redis.addr` at a
  single primary, Redis Proxy, or cluster-compatible endpoint that presents a
  single-node API if you are not ready to migrate to `UniversalClient` / `ClusterClient`.
- Full in-process Cluster client support is a future change (return type is still
  `*redis.Client` across the engine). Until then, do not set multi-node cluster
  options expecting automatic MOVED handling inside TaskMQ.

## Shared Lifecycle

In production, construct Client and Workers with the **same** `*lifecycle.Lifecycle`
instance (Fx `taskmq.Module` does this). Separate construction uses independent
admission counters and can disagree on limits.
