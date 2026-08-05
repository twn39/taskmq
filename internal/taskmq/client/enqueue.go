package client

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/events"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/meta"
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
		if merr := lifecycle.MapEnqueueScriptResult(res, e.d.lifecycle, false); merr != nil {
			return merr
		}
		_ = e.d.meta.Put(ctx, task, meta.StatePending)
		e.d.events.Emit(ctx, task.Queue, events.TypeEnqueued, task.ID, task.Name, "")
		return nil
	}

	res, err := lifecycle.EnqueueStreamCmd.Run(ctx, e.d.rdb,
		[]string{streamKey},
		hard, maxlen, serialized,
	).Result()
	if err != nil {
		return err
	}
	if mapErr := lifecycle.MapEnqueueScriptResult(res, e.d.lifecycle, false); mapErr != nil {
		return mapErr
	}
	_ = e.d.meta.Put(ctx, task, meta.StatePending)
	e.d.events.Emit(ctx, task.Queue, events.TypeEnqueued, task.ID, task.Name, "")
	return nil
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
		_ = e.d.meta.Put(ctx, task, meta.StateDelayed)
		e.d.events.Emit(ctx, task.Queue, events.TypeDelayed, task.ID, task.Name, "enqueued")
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
	_ = e.d.meta.Put(ctx, task, meta.StateDelayed)
	e.d.events.Emit(ctx, task.Queue, events.TypeDelayed, task.ID, task.Name, "enqueued")
	return nil
}

// BulkResult summarizes EnqueueBulk outcomes.
type BulkResult struct {
	// Succeeded holds task IDs that were enqueued (including delayed).
	Succeeded []string `json:"succeeded"`
	// Failed holds per-index errors (empty string means success at that index).
	// Parallel to the input slice when FailFast is false; otherwise may be shorter.
	Errors []error `json:"-"`
	// FailedIndexes lists input indices that failed when FailFast is false.
	FailedIndexes []int `json:"failed_indexes,omitempty"`
}

// BulkOption configures EnqueueBulk.
type BulkOption func(*bulkOpts)

type bulkOpts struct {
	failFast bool
}

// WithBulkFailFast stops after the first error (default false: continue remaining).
func WithBulkFailFast(v bool) BulkOption {
	return func(o *bulkOpts) { o.failFast = v }
}

type bulkItem struct {
	idx  int
	task *taskmodel.Task
}

// EnqueueBulk enqueues many tasks efficiently.
// Non-unique immediate tasks sharing a queue are pipelined; unique tasks fall back to single-path.
func (e *enqueuer) EnqueueBulk(ctx context.Context, tasks []*taskmodel.Task, opts ...BulkOption) (*BulkResult, error) {
	bo := bulkOpts{}
	for _, o := range opts {
		o(&bo)
	}
	out := &BulkResult{
		Succeeded: make([]string, 0, len(tasks)),
		Errors:    make([]error, len(tasks)),
	}
	if len(tasks) == 0 {
		return out, nil
	}

	var deferred []bulkItem
	byQueue := map[string][]bulkItem{}

	for i, t := range tasks {
		if t == nil {
			err := fmt.Errorf("taskmq: bulk task[%d] is nil", i)
			out.Errors[i] = err
			out.FailedIndexes = append(out.FailedIndexes, i)
			if bo.failFast {
				return out, err
			}
			continue
		}
		if t.ID == "" {
			t.ID = generateUUID()
		}
		if t.UniqueKey != "" {
			deferred = append(deferred, bulkItem{i, t})
			continue
		}
		byQueue[t.Queue] = append(byQueue[t.Queue], bulkItem{i, t})
	}

	for queue, items := range byQueue {
		if err := e.pipelineImmediate(ctx, queue, items, out, bo.failFast); err != nil && bo.failFast {
			return out, err
		}
	}
	for _, it := range deferred {
		err := e.Enqueue(ctx, it.task)
		if err != nil {
			out.Errors[it.idx] = err
			out.FailedIndexes = append(out.FailedIndexes, it.idx)
			if bo.failFast {
				return out, err
			}
			continue
		}
		out.Succeeded = append(out.Succeeded, it.task.ID)
	}
	if len(out.FailedIndexes) > 0 {
		return out, fmt.Errorf("taskmq: bulk enqueue finished with %d failure(s)", len(out.FailedIndexes))
	}
	return out, nil
}

func (e *enqueuer) pipelineImmediate(ctx context.Context, queue string, items []bulkItem, out *BulkResult, failFast bool) error {
	if len(items) == 0 {
		return nil
	}
	hard := e.d.hardLimit()
	maxlen := e.d.streamMaxLen()
	streamKey := keys.KeysFor(queue).Stream()

	if e.d.lifecycle != nil {
		e.d.lifecycle.NoteSoftLimitIfNeeded(ctx, e.d.rdb, queue)
	}

	type prepared struct {
		idx        int
		task       *taskmodel.Task
		serialized []byte
	}
	prep := make([]prepared, 0, len(items))
	for _, it := range items {
		if e.d.lifecycle != nil {
			if err := e.d.lifecycle.CheckPayloadSize(it.task.Payload); err != nil {
				out.Errors[it.idx] = err
				out.FailedIndexes = append(out.FailedIndexes, it.idx)
				if failFast {
					return err
				}
				continue
			}
		}
		ser, err := e.d.codec.Marshal(it.task)
		if err != nil {
			out.Errors[it.idx] = err
			out.FailedIndexes = append(out.FailedIndexes, it.idx)
			if failFast {
				return err
			}
			continue
		}
		prep = append(prep, prepared{idx: it.idx, task: it.task, serialized: ser})
	}
	if len(prep) == 0 {
		return nil
	}

	// Sequential script Run (EVALSHA with auto-load). Pipelining EVALSHA is unreliable
	// across miniredis/cluster until scripts are warmed; batching still wins on API + meta.
	for _, p := range prep {
		res, err := lifecycle.EnqueueStreamCmd.Run(ctx, e.d.rdb, []string{streamKey}, hard, maxlen, p.serialized).Result()
		if err != nil {
			out.Errors[p.idx] = err
			out.FailedIndexes = append(out.FailedIndexes, p.idx)
			if failFast {
				return err
			}
			continue
		}
		if mapErr := lifecycle.MapEnqueueScriptResult(res, e.d.lifecycle, false); mapErr != nil {
			out.Errors[p.idx] = mapErr
			out.FailedIndexes = append(out.FailedIndexes, p.idx)
			if failFast {
				return mapErr
			}
			continue
		}
		_ = e.d.meta.Put(ctx, p.task, meta.StatePending)
		e.d.events.Emit(ctx, p.task.Queue, events.TypeEnqueued, p.task.ID, p.task.Name, "")
		out.Succeeded = append(out.Succeeded, p.task.ID)
	}
	return nil
}
