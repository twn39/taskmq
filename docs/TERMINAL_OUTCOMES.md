# Message Terminal Outcomes

This document maps every **terminal outcome** of a Redis Stream message to the
code that settles it. Use it when adding new gates, middleware, or admin actions.

## Two error vocabularies (do not mix)

| Layer | Package | Types | Who uses them |
|---|---|---|---|
| **Handler control** | `task` | `SkipRetry` / `Unrecoverable` / `ErrNoHandler` | Application handlers and retry policy |
| **Stream settlement** | `worker` | `ErrHandled` / `Handled` / `IsHandled` / `Abort` | Middleware & `executeAndSettle` only |

**Rules for application handlers:**

1. Return normal `error` to retry (subject to `MaxRetry` + retry policy).
2. Return `task.SkipRetry(err)` or `task.Unrecoverable(err)` for permanent failure → DLQ.
3. **Never** XACK/XDEL, call broker settlement, or wrap with `worker.Handled` from handlers.
4. Prefer `ConsumeContext.SetResult` / `UpdateProgress` for side channels — they do not settle the stream message.

**Rules for middleware / pipeline authors:**

1. After successful `ScheduleRetry` / `MoveToDLQ` / equivalent broker settle → return `Handled(cause)`.
2. After rate-limit defer → `Abort()` + `nil` (do not Complete).
3. Broker failure → return the raw error **without** `Handled` so the message stays in the PEL.
4. Outer code must not `CompleteTask` when `IsHandled(err)` or `IsAborted()`.

## Pipeline overview

```text
XReadGroup / XAutoClaim
  → MessageProcessor.Process
      1. extract + decode (corrupt → XACK+XDEL discard)
      2. applyDeliveryCount (__delivery_count → task.Retry)
      3. routeExceededMaxRetry  → MoveToDLQ          [gate]
      4. discardIfCancelled     → CompleteTask       [gate]
      5. executeAndSettle
           middleware chain (outer → inner):
             RetryAndDLQ
             UniqueLockWatchdog
             RateLimit
             Recovery
             Handler
           on success only → CompleteTask
```

## Outcome matrix

| Outcome | Where | Broker / Redis effect | Chain return | Completes? |
|---|---|---|---|---|
| **Success** | `executeAndSettle` | `CompleteTask` (XACK+XDEL, unique unlock if needed) | `nil` | Yes (here) |
| **Retry (backoff)** | `RetryAndDLQMiddleware` | `ScheduleRetry` → delayed ZSET | `Handled(err)` | No |
| **Permanent fail → DLQ** | `RetryAndDLQMiddleware` | `MoveToDLQ` | `Handled(err)` | No |
| **SkipRetry / Unrecoverable** | same | `MoveToDLQ` (no further retries) | `Handled(err)` | No |
| **No handler** | same as DLQ | `MoveToDLQ` | `Handled(ErrNoHandler)` | No |
| **Rate-limit defer** | `RateLimitMiddleware` | `DeferRateLimitedTask` | `Abort` + `nil` | No |
| **Cancel before run** | `discardIfCancelled` | `CompleteTask` (drop) | n/a (gate) | Yes (gate) |
| **Crash recovery max-retry** | `routeExceededMaxRetry` | `MoveToDLQ` | n/a (gate) | No |
| **Corrupt payload** | `Process` decode fail | raw XACK+XDEL | n/a | Discarded |
| **Settlement Redis error** | Retry/DLQ/Complete paths | partial / none | **not** `Handled` (or logged) | No — leave PEL |
| **Unique duplicate reject** | Client enqueue (lifecycle) | Unique lock SET NX fails | `lifecycle.ErrDuplicateTask` (enqueue) | n/a (not stream-settled) |
| **Unique unlock on success** | `CompleteTask` Lua | Deletes unique lock if owned | `nil` | Yes (complete) |
| **UniqueUntilStart early unlock** | `UniqueLockWatchdogMiddleware` | `ReleaseUniqueLock` at handler start | continues chain | No (not terminal) |
| **Stalled (PEL reclaim)** | `PELRecoveryJanitor` | XAutoClaim + best-effort `events.TypeStalled` | reprocess via processor | No (reclaimed) |

## `ErrHandled` semantics

```go
// worker.ErrHandled — message outcome already applied.
errors.Is(err, worker.ErrHandled) // or worker.IsHandled(err)
```

Rules:

1. After a **successful** retry schedule or DLQ move, middleware returns `Handled(cause)`.
2. Outer code **must not** call `CompleteTask` when `IsHandled(err)` or `IsAborted()`.
3. If broker settlement fails, return the broker error **without** wrapping as Handled so the message can stay pending for reclaim.
4. Rate-limit success uses `Abort()` + `nil` (equivalent “do not complete”) rather than Handled.

## Dual settlement paths

| Path | Role |
|---|---|
| **Process gates** | Pre-handler terminal decisions (cancel, crash max-retry, corrupt). |
| **Middleware** | Handler failure (retry/DLQ), rate-limit defer, unique-lock lifecycle. |
| **executeAndSettle success** | Only happy-path `CompleteTask`. |

Do **not** XACK/XDEL from application handlers. Always go through `TaskBroker` or documented gates.

## SkipRetry / Unrecoverable

```go
return task.SkipRetry(fmt.Errorf("invalid payload: %w", err))
// or
return task.Unrecoverable(err)
```

Both set `errors.Is(..., task.ErrSkipRetry|ErrUnrecoverable)` and force DLQ without consuming remaining MaxRetry budget for further attempts (Retry counter still increments once on the failure path).

## Test coverage map

| Outcome | Unit | Integration (`tests/integration/settlement_matrix_test.go`) |
|---|---|---|
| Success | `process_message_test` | `TestSettlementMatrix_SuccessAndSkipRetry` |
| SkipRetry / Unrecoverable | middleware tests | SuccessAndSkipRetry + UnrecoverableAlias |
| Retry → delayed | middleware Handled | `TestSettlementMatrix_RetrySchedulesDelayed` |
| MaxRetry → DLQ | middleware | `TestSettlementMatrix_MaxRetryGoesToDLQ` |
| No handler → DLQ | middleware | `TestSettlementMatrix_NoHandlerGoesToDLQ` |
| Cancel before run | process_message_test | `TestSettlementMatrix_CancelBeforeRun` |
| Rate-limit defer | middleware Abort | `TestSettlementMatrix_RateLimitDefers` |
| Corrupt payload | process stages | `TestSettlementMatrix_CorruptPayloadDiscarded` |
| Crash recovery max-retry | `TestProcessMessage_CrashRecoveryMaxRetryGate` | `TestSettlementMatrix_CrashRecoveryMaxRetryGoesToDLQ` |
| Settlement Redis error | `TestRetryAndDLQMiddleware_Outcomes` | `TestSettlementMatrix_SettlementBrokerFailureLeavesPEL` |
| Unique duplicate reject | lifecycle unique tests | `TestSettlementMatrix_UniqueDuplicateRejected` |
| Unique unlock on success | broker unique tests | `TestSettlementMatrix_UniqueUnlockOnSuccess` |
| UniqueUntilStart early unlock | middleware | `TestSettlementMatrix_UniqueUntilStartReleasesEarly` |
| Stalled PEL reclaim event | `TestPELRecoveryJanitor_*` | `TestSettlementMatrix_StalledEventOnPELReclaim` |

## Adding a new terminal outcome

1. Prefer a **named stage** in `process.go` (pre-handler) or a **middleware** (during/after handler).
2. Call a **broker** method for multi-key atomics (hash-tagged keys only under `{queue}`).
3. On success: return `Handled(err)` or `Abort()` so success-path Complete is skipped.
4. Update task **meta** state when durable inspectability is required; keep meta/events best-effort.
5. Add a row to this matrix, a unit test under `internal/taskmq/worker/`, and an integration case in `settlement_matrix_test.go` when the outcome is Redis-visible.
6. If Lua KEYS/ARGV change, update [LUA_SCRIPTS.md](LUA_SCRIPTS.md).
7. Never re-export `worker.ErrHandled` to application-facing packages.
