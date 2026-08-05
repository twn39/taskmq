# Lua Script Ownership

All multi-key Redis scripts that touch a queue **must** use only keys under the same
hash tag `taskmq:{queue}:…` (see `internal/taskmq/keys`). That keeps Cluster slot
affinity correct.

## Active scripts (embedded via `//go:embed`)

| Script | Owner package | Purpose |
|---|---|---|
| `enqueue_stream.lua` | `lifecycle` | Limited stream enqueue (XADD + soft/hard limits) |
| `enqueue_unique_with_limit.lua` | `lifecycle` | Unique + admission-limited immediate enqueue |
| `enqueue_delayed_with_limit.lua` | `lifecycle` | Delayed ZADD with capacity / overflow |
| `enqueue_unique_delayed_with_limit.lua` | `lifecycle` | Unique delayed enqueue with limits |
| `force_promote_member.lua` | `lifecycle` | Promote one delayed member to stream |
| `delete_dead_letter.lua` | `client` | Atomic DLQ ZREM + HDEL |
| `register_cron.lua` | `client` | Register / update cron job config |
| `luaHandleFailure` (inline) | `broker` | XACK/XDEL + DLQ or retry delayed + unique unlock |
| `luaDeferRateLimitedTask` (inline) | `broker` | Rate-limit defer to delayed ZSET |
| `luaUnlock` / `luaRenewUniqueLock` (inline) | `broker` | Unique lock CAS release / renew |
| `luaCompleteTask` (inline) | `broker` | Success path complete + optional unlock |

Inline scripts live in `internal/taskmq/broker/broker.go` (and related broker files).
File-backed scripts live next to their owner under `scripts/`.

## Rules

1. **Do not** reintroduce a top-level `internal/taskmq/scripts/` dump of duplicates.
2. New multi-key scripts go in the package that owns the operation (client / lifecycle / broker).
3. Prefer passing `taskId` and other IDs as ARGV — never parse serialized task bodies in Lua.
4. When changing KEYS/ARGV contracts, update this table and the matching Go `Eval` call sites.
