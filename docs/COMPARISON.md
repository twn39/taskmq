# TaskMQ vs Asynq vs BullMQ (feature map)

Short reference after the 2026-08 improvement pass. Details: repo root analysis in agent chat history; ops: [OPERATIONS.md](OPERATIONS.md).

| Area | TaskMQ | Asynq | BullMQ |
|---|---|---|---|
| Transport | Redis Streams + PEL | Lists/ZSET state machine | Lists/ZSET + locks |
| Connection | UniversalClient: standalone / cluster / sentinel | Redis opts + Sentinel | Multi-mode |
| Unique / dedup | UniqueKey + UniqueScope + watchdog | Unique option | Deduplication |
| Rate limit | Built-in GCRA + group key | External | Global / group |
| Admission | Lifecycle soft/hard/delayed/payload | Weak | removeOnComplete etc. |
| Skip retry | `task.SkipRetry` / `Unrecoverable` | `SkipRetry` | `UnrecoverableError` |
| Deadline | Timeout + absolute DeadlineMs | Timeout + Deadline | Job timeout |
| Task by ID | Meta hash + `GetTaskInfo` / CLI / HTTP | Inspector | getJob |
| Results | Optional via `completed_retention` | ResultWriter + retention | returnvalue + removeOnComplete |
| Metrics | Process-local + Redis queue HASH + Prometheus | metrics exporter | metrics API |
| Flows / parent-child | — | — | FlowProducer |
| Events bus | Queue events stream | partial | QueueEvents |
| Progress | meta + events | — | updateProgress |
| Bulk enqueue | EnqueueBulk (pipeline) | weak | addBulk |
| Worker heartbeats | per-consumer keys | heartbeater | lock renew |
| Cron | RegisterCron + healing | Scheduler / Periodic | Job scheduler |

## Intentionally deferred

- Parent/child Flows, job-level priority inside one queue, sandboxed processes, full event WebSocket fan-out.

## GroupKey note

In TaskMQ, `GroupKey` is for **rate-limit bucketing**, not Asynq-style task aggregation.
