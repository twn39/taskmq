package lifecycle

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// XAddTask writes a serialized task onto a stream with hard limit + optional MAXLEN.
// When l is nil, limits are treated as disabled (unbounded XADD via script with hard=0,maxlen=0).
func (l *Lifecycle) XAddTask(ctx context.Context, rdb redis.UniversalClient, stream string, payload []byte) error {
	hard, maxlen := int64(0), int64(0)
	if l != nil {
		hard = l.cfg.EnqueueHardLimit
		maxlen = l.cfg.StreamMaxLen
	}
	res, err := EnqueueStreamCmd.Run(ctx, rdb, []string{stream}, hard, maxlen, payload).Result()
	if err != nil {
		return err
	}
	return MapEnqueueScriptResult(res, l, false)
}

// ZAddDelayed inserts into the delayed ZSET applying DelayedMaxCount + overflow policy.
// Client-facing path: honors configured DelayedOverflow (default reject).
func (l *Lifecycle) ZAddDelayed(ctx context.Context, rdb redis.UniversalClient, delayedKey string, score int64, payload []byte) error {
	return l.zAddDelayed(ctx, rdb, delayedKey, score, payload, false)
}

// ZAddDelayedSystem is for in-flight requeues (retry, rate-limit defer, cron reschedule).
// When DelayedMaxCount is set it always uses drop_farthest so work already ACKed/removed
// from the stream is never rejected (would otherwise lose tasks).
func (l *Lifecycle) ZAddDelayedSystem(ctx context.Context, rdb redis.UniversalClient, delayedKey string, score int64, payload []byte) error {
	return l.zAddDelayed(ctx, rdb, delayedKey, score, payload, true)
}

func (l *Lifecycle) zAddDelayed(ctx context.Context, rdb redis.UniversalClient, delayedKey string, score int64, payload []byte, system bool) error {
	maxCount := int64(0)
	overflow := 0
	if l != nil {
		maxCount = l.cfg.DelayedMaxCount
		if system && maxCount > 0 {
			// System requeues must not reject: force drop_farthest under capacity pressure.
			overflow = 1
		} else {
			overflow = l.OverflowArg()
		}
	}
	res, err := EnqueueDelayedWithLimitCmd.Run(ctx, rdb, []string{delayedKey}, score, payload, maxCount, overflow).Result()
	if err != nil {
		return err
	}
	return MapEnqueueScriptResult(res, l, true)
}

// ForcePromoteMember moves one delayed member to the stream under hard/MAXLEN admission.
// Returns ErrQueueFull if the stream is at hard limit (member remains delayed).
// Returns a not-found style error when the member is already gone.
func (l *Lifecycle) ForcePromoteMember(ctx context.Context, rdb redis.UniversalClient, delayedKey, streamKey, member string) error {
	hard, maxlen := int64(0), int64(0)
	if l != nil {
		hard = l.cfg.EnqueueHardLimit
		maxlen = l.cfg.StreamMaxLen
	}
	res, err := ForcePromoteMemberCmd.Run(ctx, rdb, []string{delayedKey, streamKey}, member, hard, maxlen).Result()
	if err != nil {
		return err
	}
	val, ok := res.(int64)
	if !ok {
		return fmt.Errorf("taskmq: unexpected force-promote result type %T", res)
	}
	switch val {
	case 1:
		return nil
	case 0:
		return ErrMemberGone
	case enqueueQueueFull:
		if l != nil {
			l.observe("enqueue_rejected_total", &l.metrics.EnqueueRejectedTotal, 1)
		}
		return ErrQueueFull
	default:
		return fmt.Errorf("taskmq: force-promote returned %d", val)
	}
}

// DelayedLimits returns (maxCount, overflowArg) for Lua scripts that embed delayed capacity checks.
func (l *Lifecycle) DelayedLimits() (maxCount int64, overflow int) {
	if l == nil {
		return 0, 0
	}
	return l.cfg.DelayedMaxCount, l.OverflowArg()
}

// StreamLimits returns (hardLimit, streamMaxLen) for promote/XADD scripts.
func (l *Lifecycle) StreamLimits() (hard, maxlen int64) {
	if l == nil {
		return 0, 0
	}
	return l.cfg.EnqueueHardLimit, l.cfg.StreamMaxLen
}
