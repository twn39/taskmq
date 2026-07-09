package lifecycle

import (
	"github.com/twn39/taskmq/internal/config"
)

// FromConfig maps config.LifecycleConfig → LifecycleConfig.
func FromConfig(cfg *config.Config) LifecycleConfig {
	out := DefaultLifecycleConfig()
	if cfg == nil {
		return out
	}
	lc := cfg.TaskMQ.Lifecycle
	out.StreamMaxLen = lc.StreamMaxLen
	out.EnqueueSoftLimit = lc.EnqueueSoftLimit
	out.EnqueueHardLimit = lc.EnqueueHardLimit
	out.DelayedMaxCount = lc.DelayedMaxCount
	out.DelayedMaxDelay = lc.DelayedMaxDelay
	if lc.DelayedOverflow != "" {
		out.DelayedOverflow = DelayedOverflowPolicy(lc.DelayedOverflow)
	}
	if lc.DLQMaxCount != nil {
		out.DLQMaxCount = *lc.DLQMaxCount
	}
	out.DLQMaxAge = lc.DLQMaxAge
	if lc.CancelledTTL > 0 {
		out.CancelledTTL = lc.CancelledTTL
	}
	out.MaxPayloadBytes = lc.MaxPayloadBytes
	if lc.SafeTrimInterval > 0 {
		out.SafeTrimInterval = lc.SafeTrimInterval
	}
	if lc.SafeTrimBatchLimit > 0 {
		out.SafeTrimBatchLimit = lc.SafeTrimBatchLimit
	}
	out.IdleConsumerTimeout = lc.IdleConsumerTimeout
	if lc.SafeTrimEnabled != nil {
		out.SafeTrimEnabled = *lc.SafeTrimEnabled
	}
	if lc.PurgeCancelledDelayed != nil {
		out.PurgeCancelledDelayed = *lc.PurgeCancelledDelayed
	}
	return out
}
