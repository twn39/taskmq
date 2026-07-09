package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
)

// Lifecycle-related sentinel errors returned by Client admission checks.
var (
	ErrQueueFull       = errors.New("taskmq: queue depth exceeded hard limit")
	ErrDelayedFull     = errors.New("taskmq: delayed queue capacity exceeded")
	ErrDelayTooFar     = errors.New("taskmq: delay exceeds max_delay policy")
	ErrPayloadTooLarge = errors.New("taskmq: task payload exceeds max_payload_bytes")
	// ErrDuplicateTask is returned when a unique task cannot be enqueued because a duplicate already exists.
	ErrDuplicateTask = errors.New("taskmq: duplicate task in queue")
	// ErrMemberGone is returned by ForcePromoteMember when the delayed member was already removed.
	ErrMemberGone = errors.New("taskmq: task already processed or deleted")
)

// DelayedOverflowPolicy controls behavior when delayed queue hits MaxCount.
type DelayedOverflowPolicy string

const (
	// DelayedOverflowReject refuses new delayed enqueues when full (recommended default).
	DelayedOverflowReject DelayedOverflowPolicy = "reject"
	// DelayedOverflowDropFarthest drops the farthest-future delayed tasks to make room.
	DelayedOverflowDropFarthest DelayedOverflowPolicy = "drop_farthest"
)

// LifecycleConfig bounds Redis memory and enforces admission control.
// Zero values mean "disabled" for limits, except DLQMaxCount / CancelledTTL which
// keep production-safe defaults via DefaultLifecycleConfig / Normalize.
type LifecycleConfig struct {
	// StreamMaxLen is an optional catastrophe valve for XADD MAXLEN ~ N.
	// Prefer hard limits + SafeTrim; leave 0 (disabled) unless you accept possible loss.
	StreamMaxLen int64

	// EnqueueSoftLimit logs/metrics when stream length exceeds this (0 = off).
	EnqueueSoftLimit int64
	// EnqueueHardLimit rejects Enqueue when XLEN >= limit (0 = off).
	EnqueueHardLimit int64

	// DelayedMaxCount bounds delayed ZSET cardinality (0 = off).
	DelayedMaxCount int64
	// DelayedMaxDelay rejects EnqueueAt further than now+MaxDelay (0 = off).
	DelayedMaxDelay time.Duration
	// DelayedOverflow is reject (default) or drop_farthest.
	DelayedOverflow DelayedOverflowPolicy

	// DLQMaxCount keeps the newest N dead letters (default 1000). 0 = unlimited.
	DLQMaxCount int64
	// DLQMaxAge purges DLQ entries older than this via RetentionJanitor (0 = off).
	DLQMaxAge time.Duration

	// CancelledTTL is the cancel-marker key TTL (default 24h).
	CancelledTTL time.Duration

	// MaxPayloadBytes rejects tasks whose serialized payload exceeds this (0 = off).
	MaxPayloadBytes int

	// SafeTrimEnabled runs MINID-based stream trimming (default true).
	SafeTrimEnabled bool
	// SafeTrimInterval is how often retention janitor runs (default 30s).
	SafeTrimInterval time.Duration
	// SafeTrimBatchLimit caps entries examined per XTRIM (default 1000).
	SafeTrimBatchLimit int64

	// IdleConsumerTimeout removes consumers with 0 pending idle longer than this (0 = off).
	IdleConsumerTimeout time.Duration

	// PurgeCancelledDelayed removes delayed members whose task is cancelled (default true when janitor runs).
	PurgeCancelledDelayed bool
}

// DefaultLifecycleConfig returns backward-compatible defaults:
// DLQ capped at 1000 (matching previous hardcode), SafeTrim on, no enqueue hard limit.
func DefaultLifecycleConfig() LifecycleConfig {
	return LifecycleConfig{
		DLQMaxCount:           1000,
		CancelledTTL:          24 * time.Hour,
		DelayedOverflow:       DelayedOverflowReject,
		SafeTrimEnabled:       true,
		SafeTrimInterval:      30 * time.Second,
		SafeTrimBatchLimit:    1000,
		PurgeCancelledDelayed: true,
	}
}

// Normalize fills defaults for zero-valued fields that should not mean "disabled".
func (c LifecycleConfig) Normalize() LifecycleConfig {
	out := c
	// DLQMaxCount: <0 treated as unlimited (0); 0 means unlimited; >0 is a cap.
	if out.DLQMaxCount < 0 {
		out.DLQMaxCount = 0
	}
	if out.CancelledTTL <= 0 {
		out.CancelledTTL = 24 * time.Hour
	}
	if out.DelayedOverflow == "" {
		out.DelayedOverflow = DelayedOverflowReject
	}
	if out.SafeTrimInterval <= 0 {
		out.SafeTrimInterval = 30 * time.Second
	}
	if out.SafeTrimBatchLimit <= 0 {
		out.SafeTrimBatchLimit = 1000
	}
	return out
}

// LifecycleMetrics is a lightweight process-local counter set (no Prometheus dependency).
type LifecycleMetrics struct {
	EnqueueRejectedTotal      atomic.Int64
	DelayedRejectedTotal      atomic.Int64
	PayloadRejectedTotal      atomic.Int64
	SafeTrimDeletedTotal      atomic.Int64
	DLQEvictedTotal           atomic.Int64
	CancelledDelayedPurged    atomic.Int64
	IdleConsumersRemovedTotal atomic.Int64
	SoftLimitHitsTotal        atomic.Int64
}

// Snapshot returns a copy of current counters.
func (m *LifecycleMetrics) Snapshot() map[string]int64 {
	if m == nil {
		return map[string]int64{}
	}
	return map[string]int64{
		"enqueue_rejected_total":       m.EnqueueRejectedTotal.Load(),
		"delayed_rejected_total":       m.DelayedRejectedTotal.Load(),
		"payload_rejected_total":       m.PayloadRejectedTotal.Load(),
		"safe_trim_deleted_total":      m.SafeTrimDeletedTotal.Load(),
		"dlq_evicted_total":            m.DLQEvictedTotal.Load(),
		"cancelled_delayed_purged":     m.CancelledDelayedPurged.Load(),
		"idle_consumers_removed_total": m.IdleConsumersRemovedTotal.Load(),
		"soft_limit_hits_total":        m.SoftLimitHitsTotal.Load(),
	}
}

// Lifecycle binds config + metrics for admission and janitor use.
type Lifecycle struct {
	cfg     LifecycleConfig
	metrics *LifecycleMetrics
}

// NewLifecycle creates a Lifecycle with normalized config.
func NewLifecycle(cfg LifecycleConfig) *Lifecycle {
	cfg = cfg.Normalize()
	return &Lifecycle{
		cfg:     cfg,
		metrics: &LifecycleMetrics{},
	}
}

// Config returns a copy of the normalized config.
func (l *Lifecycle) Config() LifecycleConfig {
	if l == nil {
		return DefaultLifecycleConfig().Normalize()
	}
	return l.cfg
}

// Metrics returns the metrics sink (never nil for NewLifecycle).
func (l *Lifecycle) Metrics() *LifecycleMetrics {
	if l == nil {
		return &LifecycleMetrics{}
	}
	return l.metrics
}

// CheckPayloadSize validates MaxPayloadBytes against raw payload length.
func (l *Lifecycle) CheckPayloadSize(payload []byte) error {
	if l == nil {
		return nil
	}
	max := l.cfg.MaxPayloadBytes
	if max <= 0 {
		return nil
	}
	if len(payload) > max {
		l.metrics.PayloadRejectedTotal.Add(1)
		return fmt.Errorf("%w: size=%d max=%d", ErrPayloadTooLarge, len(payload), max)
	}
	return nil
}

// CheckDelayedMaxDelay validates only the max-delay horizon (capacity is enforced atomically in Lua).
func (l *Lifecycle) CheckDelayedMaxDelay(runAt time.Time) error {
	if l == nil {
		return nil
	}
	cfg := l.cfg
	if cfg.DelayedMaxDelay <= 0 {
		return nil
	}
	if runAt.After(time.Now().Add(cfg.DelayedMaxDelay)) {
		l.metrics.DelayedRejectedTotal.Add(1)
		return fmt.Errorf("%w: run_at=%s max_delay=%s", ErrDelayTooFar, runAt.UTC().Format(time.RFC3339), cfg.DelayedMaxDelay)
	}
	return nil
}

// NoteSoftLimitIfNeeded samples XLEN and increments soft-limit metrics (non-blocking).
func (l *Lifecycle) NoteSoftLimitIfNeeded(ctx context.Context, rdb *redis.Client, queue string) {
	if l == nil || l.cfg.EnqueueSoftLimit <= 0 {
		return
	}
	n, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	if err != nil {
		return
	}
	if n >= l.cfg.EnqueueSoftLimit {
		l.metrics.SoftLimitHitsTotal.Add(1)
	}
}

// script return codes for enqueue-with-limit
const (
	enqueueOK          int64 = 1
	enqueueDuplicate   int64 = -1
	enqueueQueueFull   int64 = -2
	enqueueDelayedFull int64 = -3
)

// enqueueStreamLua atomically enforces hard limit then XADD (optional MAXLEN).
// KEYS[1]=stream
// ARGV[1]=hardLimit (0=off), ARGV[2]=streamMaxLen (0=off), ARGV[3]=serialized
const enqueueStreamLua = `
local stream = KEYS[1]
local hard = tonumber(ARGV[1]) or 0
local maxlen = tonumber(ARGV[2]) or 0
local payload = ARGV[3]
if hard > 0 then
  local n = redis.call('XLEN', stream)
  if n >= hard then
    return -2
  end
end
if maxlen > 0 then
  return redis.call('XADD', stream, 'MAXLEN', '~', maxlen, '*', 'task', payload)
end
return redis.call('XADD', stream, '*', 'task', payload)
`

// enqueueUniqueWithLimitLua extends unique enqueue with hard limit / MAXLEN.
// KEYS[1]=lock KEYS[2]=stream
// ARGV[1]=lockVal ARGV[2]=ttlMs ARGV[3]=serialized ARGV[4]=hard ARGV[5]=maxlen
const enqueueUniqueWithLimitLua = `
local lockKey = KEYS[1]
local streamKey = KEYS[2]
local lockVal = ARGV[1]
local ttlMs = tonumber(ARGV[2])
local serialized = ARGV[3]
local hard = tonumber(ARGV[4]) or 0
local maxlen = tonumber(ARGV[5]) or 0

local currentLockVal = redis.call("GET", lockKey)
if currentLockVal and currentLockVal ~= lockVal then
	return -1
end
if hard > 0 then
  local n = redis.call('XLEN', streamKey)
  if n >= hard then
    return -2
  end
end
redis.call("SET", lockKey, lockVal, "PX", ttlMs)
if maxlen > 0 then
  redis.call("XADD", streamKey, "MAXLEN", "~", maxlen, "*", "task", serialized)
else
  redis.call("XADD", streamKey, "*", "task", serialized)
end
return 1
`

// enqueueDelayedWithLimitLua enforces delayed max count then ZADD.
// KEYS[1]=delayed
// ARGV[1]=score ARGV[2]=serialized ARGV[3]=maxCount ARGV[4]=overflow (0=reject,1=drop_farthest)
const enqueueDelayedWithLimitLua = `
local delayedKey = KEYS[1]
local score = tonumber(ARGV[1])
local serialized = ARGV[2]
local maxCount = tonumber(ARGV[3]) or 0
local overflow = tonumber(ARGV[4]) or 0

if maxCount > 0 then
  local n = redis.call('ZCARD', delayedKey)
  if n >= maxCount then
    if overflow == 1 then
      local toDrop = n - maxCount + 1
      if toDrop > 0 then
        redis.call('ZREMRANGEBYRANK', delayedKey, -toDrop, -1)
      end
    else
      return -3
    end
  end
end
redis.call('ZADD', delayedKey, score, serialized)
return 1
`

// enqueueUniqueDelayedWithLimitLua unique + delayed capacity.
// KEYS[1]=lock KEYS[2]=delayed
// ARGV[1]=lockVal ARGV[2]=ttlMs ARGV[3]=serialized ARGV[4]=score ARGV[5]=maxCount ARGV[6]=overflow
const enqueueUniqueDelayedWithLimitLua = `
local lockKey = KEYS[1]
local delayedKey = KEYS[2]
local lockVal = ARGV[1]
local ttlMs = tonumber(ARGV[2])
local serialized = ARGV[3]
local score = tonumber(ARGV[4])
local maxCount = tonumber(ARGV[5]) or 0
local overflow = tonumber(ARGV[6]) or 0

local currentLockVal = redis.call("GET", lockKey)
if currentLockVal and currentLockVal ~= lockVal then
	return -1
end
if maxCount > 0 then
  local n = redis.call('ZCARD', delayedKey)
  if n >= maxCount then
    if overflow == 1 then
      local toDrop = n - maxCount + 1
      if toDrop > 0 then
        redis.call('ZREMRANGEBYRANK', delayedKey, -toDrop, -1)
      end
    else
      return -3
    end
  end
end
redis.call("SET", lockKey, lockVal, "PX", ttlMs)
redis.call("ZADD", delayedKey, score, serialized)
return 1
`

// Redis scripts for capacity-aware enqueue (owned by lifecycle).
var (
	EnqueueStreamCmd             = redis.NewScript(enqueueStreamLua)
	EnqueueUniqueWithLimitCmd    = redis.NewScript(enqueueUniqueWithLimitLua)
	EnqueueDelayedWithLimitCmd   = redis.NewScript(enqueueDelayedWithLimitLua)
	EnqueueUniqueDelayedLimitCmd = redis.NewScript(enqueueUniqueDelayedWithLimitLua)
)

// OverflowArg returns the Lua overflow flag (0=reject, 1=drop_farthest).
func (l *Lifecycle) OverflowArg() int {
	if l != nil && l.cfg.DelayedOverflow == DelayedOverflowDropFarthest {
		return 1
	}
	return 0
}

// MapEnqueueScriptResult interprets capacity-aware enqueue script return values.
func MapEnqueueScriptResult(res interface{}, metrics *LifecycleMetrics, delayed bool) error {
	val, ok := res.(int64)
	if !ok {
		// XADD returns stream ID string on success for plain enqueue script when not using our return codes
		if _, isStr := res.(string); isStr {
			return nil
		}
		return fmt.Errorf("taskmq: unexpected enqueue script result type %T", res)
	}
	switch val {
	case enqueueDuplicate:
		return ErrDuplicateTask
	case enqueueQueueFull:
		if metrics != nil {
			metrics.EnqueueRejectedTotal.Add(1)
		}
		return ErrQueueFull
	case enqueueDelayedFull:
		if metrics != nil {
			metrics.DelayedRejectedTotal.Add(1)
		}
		return ErrDelayedFull
	default:
		// XADD via script returning ID isn't int; our scripts return 1 or stream id from redis.call XADD
		// When redis.call('XADD') returns, Eval may return the ID string - handled above.
		// When we return 1, OK.
		if val == 1 || val == 0 {
			return nil
		}
		// Some redis versions return the number of removed from ZREM etc. Treat other positives as OK.
		if val > 0 {
			return nil
		}
		return fmt.Errorf("taskmq: enqueue script returned %d", val)
	}
}
