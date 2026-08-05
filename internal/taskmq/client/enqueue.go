package client

import (
	"context"
	"strconv"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// enqueuer implements the production-path EnqueueClient.
type enqueuer struct {
	d deps
}

// Enqueue adds a task to the Redis stream immediately (active queue).
func (e *enqueuer) Enqueue(ctx context.Context, task *taskmodel.Task, opts ...TaskOption) error {
	for _, opt := range opts {
		if err := opt(task); err != nil {
			return err
		}
	}

	if task.ID == "" {
		task.ID = generateUUID()
	}

	if e.d.lifecycle != nil {
		if err := e.d.lifecycle.CheckPayloadSize(task.Payload); err != nil {
			return err
		}
	}

	serialized, err := e.d.codec.Marshal(task)
	if err != nil {
		return err
	}

	qk := keys.KeysFor(task.Queue)
	streamKey := qk.Stream()
	hard := e.d.hardLimit()
	maxlen := e.d.streamMaxLen()

	if e.d.lifecycle != nil {
		e.d.lifecycle.NoteSoftLimitIfNeeded(ctx, e.d.rdb, task.Queue)
	}

	if task.UniqueKey != "" {
		ttl := e.d.uniqueTTL(task)
		uniqueKey := qk.Unique(task.UniqueKey)
		res, uerr := lifecycle.EnqueueUniqueWithLimitCmd.Run(ctx, e.d.rdb,
			[]string{uniqueKey, streamKey},
			task.ID, int(ttl.Milliseconds()), serialized, hard, maxlen,
		).Result()
		if uerr != nil {
			return uerr
		}
		return lifecycle.MapEnqueueScriptResult(res, e.d.lifecycle, false)
	}

	res, err := lifecycle.EnqueueStreamCmd.Run(ctx, e.d.rdb,
		[]string{streamKey},
		hard, maxlen, serialized,
	).Result()
	if err != nil {
		return err
	}
	return lifecycle.MapEnqueueScriptResult(res, e.d.lifecycle, false)
}

// EnqueueIn adds a task to the delayed queue with a delay duration.
func (e *enqueuer) EnqueueIn(ctx context.Context, task *taskmodel.Task, delay time.Duration, opts ...TaskOption) error {
	return e.EnqueueAt(ctx, task, time.Now().Add(delay), opts...)
}

// EnqueueAt adds a task to the delayed queue to be executed at a specific time.
func (e *enqueuer) EnqueueAt(ctx context.Context, task *taskmodel.Task, at time.Time, opts ...TaskOption) error {
	for _, opt := range opts {
		if err := opt(task); err != nil {
			return err
		}
	}

	if task.ID == "" {
		task.ID = generateUUID()
	}

	if e.d.lifecycle != nil {
		if err := e.d.lifecycle.CheckPayloadSize(task.Payload); err != nil {
			return err
		}
		if err := e.d.lifecycle.CheckDelayedMaxDelay(at); err != nil {
			return err
		}
	}

	serialized, err := e.d.codec.Marshal(task)
	if err != nil {
		return err
	}

	qk := keys.KeysFor(task.Queue)
	delayedKey := qk.Delayed()
	maxCount := e.d.delayedMaxCount()
	overflow := 0
	if e.d.lifecycle != nil {
		overflow = e.d.lifecycle.OverflowArg()
	}

	if task.UniqueKey != "" {
		ttl := e.d.uniqueTTL(task)
		uniqueKey := qk.Unique(task.UniqueKey)
		res, uerr := lifecycle.EnqueueUniqueDelayedLimitCmd.Run(ctx, e.d.rdb,
			[]string{uniqueKey, delayedKey},
			task.ID, int(ttl.Milliseconds()), serialized, at.UnixMilli(), maxCount, overflow,
		).Result()
		if uerr != nil {
			return uerr
		}
		if merr := lifecycle.MapEnqueueScriptResult(res, e.d.lifecycle, true); merr != nil {
			return merr
		}
		_ = e.d.rdb.Publish(ctx, qk.DelayedWakeupChannel(), strconv.FormatInt(at.UnixMilli(), 10)).Err()
		return nil
	}

	res, err := lifecycle.EnqueueDelayedWithLimitCmd.Run(ctx, e.d.rdb,
		[]string{delayedKey},
		at.UnixMilli(), serialized, maxCount, overflow,
	).Result()
	if err != nil {
		return err
	}
	if err := lifecycle.MapEnqueueScriptResult(res, e.d.lifecycle, true); err != nil {
		return err
	}
	_ = e.d.rdb.Publish(ctx, qk.DelayedWakeupChannel(), strconv.FormatInt(at.UnixMilli(), 10)).Err()
	return nil
}
