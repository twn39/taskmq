package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	"github.com/twn39/taskmq/internal/taskmq/metricsq"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

// Terminal outcome matrix against real Redis (docs/TERMINAL_OUTCOMES.md).
// Unit tests cover Handled/Abort middleware contracts; this file asserts Redis-visible effects.

func settlementRedis(t *testing.T) (context.Context, context.CancelFunc, *redis.Client) {
	return RequireRedis(t)
}

func settlementLC() *lifecycle.Lifecycle {
	return DefaultTestLifecycle()
}

func flushQueue(ctx context.Context, rdb redis.UniversalClient, queue string, ids ...string) {
	FlushQueue(ctx, rdb, queue, ids...)
}

// settlementFailBroker returns errors on settlement paths so messages stay in the PEL.
type settlementFailBroker struct {
	schedErr error
	moveErr  error
}

func (b *settlementFailBroker) MoveToDLQ(ctx context.Context, t *taskmodel.Task, streamKey, msgID, group string, dlqQueueName string) error {
	return b.moveErr
}
func (b *settlementFailBroker) ScheduleRetry(ctx context.Context, t *taskmodel.Task, streamKey, msgID, group string, runAt time.Time) error {
	return b.schedErr
}
func (b *settlementFailBroker) DeferRateLimitedTask(ctx context.Context, msgID string, t *taskmodel.Task, group string, runAt time.Time) error {
	return errors.New("defer disabled in fail broker")
}
func (b *settlementFailBroker) ReleaseUniqueLock(ctx context.Context, t *taskmodel.Task) error {
	return nil
}
func (b *settlementFailBroker) RenewUniqueLock(ctx context.Context, t *taskmodel.Task, ttl time.Duration) error {
	return nil
}
func (b *settlementFailBroker) CompleteTask(ctx context.Context, t *taskmodel.Task, streamKey, msgID, group string) error {
	return errors.New("complete disabled in fail broker")
}

func TestSettlementMatrix_SuccessAndSkipRetry(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_ok")
	flushQueue(ctx, rdb, queue, "ok-1", "skip-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	var okCount atomic.Int64
	logger := zap.NewNop()
	pool := mqworker.NewWorkerPool(rdb, logger, queue,
		mqworker.WithGroup("settlement-group"),
		mqworker.WithConsumer("settlement-c1"),
		mqworker.WithConcurrency(2),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("ok", func(ctx context.Context, task *taskmodel.Task) error {
		okCount.Add(1)
		return nil
	})
	pool.Register("skip", func(ctx context.Context, task *taskmodel.Task) error {
		return taskmodel.SkipRetry(errors.New("permanent"))
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))

	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("ok", []byte(`{}`), taskmodel.TaskOptions{
		ID: "ok-1", Queue: queue, MaxRetry: taskmodel.Ptr(1),
	})))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("skip", []byte(`{}`), taskmodel.TaskOptions{
		ID: "skip-1", Queue: queue, MaxRetry: taskmodel.Ptr(5),
	})))

	require.Eventually(t, func() bool { return okCount.Load() >= 1 }, 15*time.Second, 50*time.Millisecond)

	store := meta.NewStore(rdb)
	require.Eventually(t, func() bool {
		info, _ := store.Get(ctx, queue, "ok-1")
		return info != nil && info.State == meta.StateCompleted
	}, 15*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
		return n >= 1
	}, 15*time.Second, 50*time.Millisecond)

	// Meta state is best-effort after broker settle; wait for DLQ state visibility.
	require.Eventually(t, func() bool {
		info, err := c.GetTaskInfo(ctx, queue, "skip-1")
		return err == nil && info != nil && info.State == meta.StateDLQ
	}, 15*time.Second, 50*time.Millisecond)

	ms := metricsq.NewStore(rdb)
	require.Eventually(t, func() bool {
		snap, err := ms.Snapshot(ctx, queue)
		return err == nil && snap[metricsq.FieldCompleted] >= 1 && snap[metricsq.FieldDLQ] >= 1
	}, 10*time.Second, 50*time.Millisecond)
}

func TestSettlementMatrix_RetrySchedulesDelayed(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_retry")
	flushQueue(ctx, rdb, queue, "retry-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	var fails atomic.Int64
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("settlement-retry-g"),
		mqworker.WithConsumer("settlement-retry-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		// Fixed multi-second backoff so the task stays in delayed long enough to observe.
		mqworker.WithRetryPolicy(policy.NewExponentialBackoff(5*time.Second, 1*time.Minute, false)),
	)
	pool.Register("fail", func(ctx context.Context, task *taskmodel.Task) error {
		fails.Add(1)
		return errors.New("transient")
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("fail", []byte(`{}`), taskmodel.TaskOptions{
		ID: "retry-1", Queue: queue, MaxRetry: taskmodel.Ptr(3),
	})))

	require.Eventually(t, func() bool { return fails.Load() >= 1 }, 15*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.Delayed()).Result()
		return n >= 1
	}, 15*time.Second, 50*time.Millisecond)

	// Message settled out of the stream/PEL (not left pending for Complete).
	require.Eventually(t, func() bool {
		pending, err := rdb.XPending(ctx, qk.Stream(), "settlement-retry-g").Result()
		return err == nil && pending.Count == 0
	}, 10*time.Second, 50*time.Millisecond)

	store := meta.NewStore(rdb)
	require.Eventually(t, func() bool {
		info, _ := store.Get(ctx, queue, "retry-1")
		return info != nil && (info.State == meta.StateRetry || info.State == meta.StateDelayed)
	}, 10*time.Second, 50*time.Millisecond)

	ms := metricsq.NewStore(rdb)
	snap, _ := ms.Snapshot(ctx, queue)
	require.GreaterOrEqual(t, snap[metricsq.FieldRetried], int64(1))
}

func TestSettlementMatrix_MaxRetryGoesToDLQ(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_max")
	flushQueue(ctx, rdb, queue, "max-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("settlement-max-g"),
		mqworker.WithConsumer("settlement-max-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithRetryPolicy(policy.NewExponentialBackoff(10*time.Millisecond, time.Second, false)),
	)
	pool.Register("fail", func(ctx context.Context, task *taskmodel.Task) error {
		return errors.New("always fail")
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	// MaxRetry=0 → first failure increments Retry and goes straight to DLQ (no schedule).
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("fail", []byte(`{}`), taskmodel.TaskOptions{
		ID: "max-1", Queue: queue, MaxRetry: taskmodel.Ptr(0),
	})))

	store := meta.NewStore(rdb)
	require.Eventually(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
		info, _ := store.Get(ctx, queue, "max-1")
		return n >= 1 && info != nil && info.State == meta.StateDLQ
	}, 15*time.Second, 50*time.Millisecond)

	delayed, _ := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.Equal(t, int64(0), delayed)
}

func TestSettlementMatrix_NoHandlerGoesToDLQ(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_nh")
	flushQueue(ctx, rdb, queue, "nh-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("settlement-nh-g"),
		mqworker.WithConsumer("settlement-nh-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	// No handlers registered.
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("missing", []byte(`{}`), taskmodel.TaskOptions{
		ID: "nh-1", Queue: queue, MaxRetry: taskmodel.Ptr(5),
	})))

	require.Eventually(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
		return n >= 1
	}, 15*time.Second, 50*time.Millisecond)
}

func TestSettlementMatrix_CancelBeforeRun(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_cancel")
	flushQueue(ctx, rdb, queue, "cancel-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	var ran atomic.Int64
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("settlement-cancel-g"),
		mqworker.WithConsumer("settlement-cancel-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("work", func(ctx context.Context, task *taskmodel.Task) error {
		ran.Add(1)
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.CancelTask(ctx, queue, "cancel-1"))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("work", []byte(`{}`), taskmodel.TaskOptions{
		ID: "cancel-1", Queue: queue, MaxRetry: taskmodel.Ptr(1),
	})))

	// Gate completes/discards; handler must not run.
	require.Eventually(t, func() bool {
		info, _ := meta.NewStore(rdb).Get(ctx, queue, "cancel-1")
		return info != nil && info.State == meta.StateCancelled
	}, 15*time.Second, 50*time.Millisecond)
	require.Equal(t, int64(0), ran.Load())

	require.Eventually(t, func() bool {
		pending, err := rdb.XPending(ctx, qk.Stream(), "settlement-cancel-g").Result()
		return err == nil && pending.Count == 0
	}, 10*time.Second, 50*time.Millisecond)
}

func TestSettlementMatrix_RateLimitDefers(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_rl")
	flushQueue(ctx, rdb, queue, "rl-1", "rl-2")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	var ran atomic.Int64
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("settlement-rl-g"),
		mqworker.WithConsumer("settlement-rl-c"),
		mqworker.WithConcurrency(2),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithRateLimit(1, 30*time.Second),
	)
	pool.Register("work", func(ctx context.Context, task *taskmodel.Task) error {
		ran.Add(1)
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("work", []byte(`{}`), taskmodel.TaskOptions{
		ID: "rl-1", Queue: queue,
	})))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("work", []byte(`{}`), taskmodel.TaskOptions{
		ID: "rl-2", Queue: queue,
	})))

	// At least one deferred into delayed; not both left permanently pending.
	require.Eventually(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.Delayed()).Result()
		return n >= 1
	}, 15*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		return ran.Load() >= 1
	}, 15*time.Second, 50*time.Millisecond)

	ms := metricsq.NewStore(rdb)
	require.Eventually(t, func() bool {
		snap, _ := ms.Snapshot(ctx, queue)
		return snap[metricsq.FieldDeferred] >= 1
	}, 10*time.Second, 50*time.Millisecond)
}

func TestSettlementMatrix_CorruptPayloadDiscarded(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_corrupt")
	flushQueue(ctx, rdb, queue)
	qk := keys.KeysFor(queue)
	group := "settlement-corrupt-g"
	lc := settlementLC()

	// Pre-seed a corrupt stream entry before the pool creates the group from "$".
	// Use "0" so existing messages are readable after group creation... Worker creates group
	// with "0" or "$"? Check base worker.
	_ = rdb.XGroupCreateMkStream(ctx, qk.Stream(), group, "0").Err()
	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: qk.Stream(),
		Values: map[string]interface{}{"task": "not-valid-json-{{{"},
	}).Result()
	require.NoError(t, err)

	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("settlement-corrupt-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	// Corrupt message is XACK+XDEL discarded; stream empties, no DLQ entry.
	require.Eventually(t, func() bool {
		n, _ := rdb.XLen(ctx, qk.Stream()).Result()
		pending, err := rdb.XPending(ctx, qk.Stream(), group).Result()
		return err == nil && n == 0 && pending.Count == 0
	}, 15*time.Second, 50*time.Millisecond)

	dlq, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
	require.Equal(t, int64(0), dlq)
}

func TestSettlementMatrix_UnrecoverableAlias(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_unrec")
	flushQueue(ctx, rdb, queue, "unrec-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("settlement-unrec-g"),
		mqworker.WithConsumer("settlement-unrec-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("bad", func(ctx context.Context, task *taskmodel.Task) error {
		return taskmodel.Unrecoverable(errors.New("fatal"))
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("bad", []byte(`{}`), taskmodel.TaskOptions{
		ID: "unrec-1", Queue: queue, MaxRetry: taskmodel.Ptr(10),
	})))

	store := meta.NewStore(rdb)
	require.Eventually(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
		info, _ := store.Get(ctx, queue, "unrec-1")
		return n >= 1 && info != nil && info.State == meta.StateDLQ
	}, 15*time.Second, 50*time.Millisecond)
}

// TestSettlementMatrix_SettlementBrokerFailureLeavesPEL verifies that when ScheduleRetry
// fails, the message is not Handled and remains in the PEL (not XACK'd).
func TestSettlementMatrix_SettlementBrokerFailureLeavesPEL(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "settlement_pel")
	group := "settlement-pel-g"
	flushQueue(ctx, rdb, queue, "pel-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	failBr := &settlementFailBroker{schedErr: errors.New("redis settlement unavailable")}
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("settlement-pel-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithBroker(failBr),
		mqworker.WithRetryPolicy(policy.NewExponentialBackoff(10*time.Millisecond, time.Second, false)),
	)
	pool.Register("fail", func(ctx context.Context, task *taskmodel.Task) error {
		return errors.New("handler boom")
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("fail", []byte(`{}`), taskmodel.TaskOptions{
		ID: "pel-1", Queue: queue, MaxRetry: taskmodel.Ptr(5),
	})))

	// Handler runs and settlement fails → pending stays > 0; not in DLQ/delayed.
	require.Eventually(t, func() bool {
		pending, err := rdb.XPending(ctx, qk.Stream(), group).Result()
		return err == nil && pending.Count >= 1
	}, 15*time.Second, 50*time.Millisecond)

	dlq, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
	delayed, _ := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.Equal(t, int64(0), dlq, "must not MoveToDLQ when ScheduleRetry is the path")
	require.Equal(t, int64(0), delayed, "must not schedule retry when broker fails")
}

// TestSettlementMatrix_CrashRecoveryMaxRetryGoesToDLQ seeds a stream message with a high
// __delivery_count so routeExceededMaxRetry fires against the real Redis broker.
func TestSettlementMatrix_CrashRecoveryMaxRetryGoesToDLQ(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "settlement_crash")
	group := "settlement-crash-g"
	taskID := "crash-max-1"
	flushQueue(ctx, rdb, queue, taskID)
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	// Pre-create group so the seeded message is readable (">" only sees new after group create).
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, qk.Stream(), group, "0").Err())

	task := taskmodel.NewTask("never", []byte(`{}`), taskmodel.TaskOptions{
		ID: taskID, Queue: queue, MaxRetry: taskmodel.Ptr(1),
	})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: qk.Stream(),
		Values: map[string]interface{}{
			"task":             string(ser),
			"__delivery_count": int64(10), // Retry becomes 9 > MaxRetry 1
		},
	}).Result()
	require.NoError(t, err)

	var handlerHits atomic.Int64
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("settlement-crash-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("never", func(ctx context.Context, task *taskmodel.Task) error {
		handlerHits.Add(1)
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	store := meta.NewStore(rdb)
	require.Eventually(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
		info, _ := store.Get(ctx, queue, taskID)
		return n >= 1 && info != nil && info.State == meta.StateDLQ
	}, 15*time.Second, 50*time.Millisecond)

	require.Equal(t, int64(0), handlerHits.Load(), "handler must not run when crash max-retry gate fires")
	pending, err := rdb.XPending(ctx, qk.Stream(), group).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), pending.Count)
}

// TestSettlementMatrix_MoveToDLQBrokerFailureLeavesPEL covers permanent-failure path when
// MoveToDLQ itself fails (MaxRetry=0 → middleware chooses DLQ, not ScheduleRetry).
func TestSettlementMatrix_MoveToDLQBrokerFailureLeavesPEL(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "settlement_movedlq")
	group := "settlement-movedlq-g"
	flushQueue(ctx, rdb, queue, "md-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	failBr := &settlementFailBroker{moveErr: errors.New("dlq redis unavailable")}
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("settlement-movedlq-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithBroker(failBr),
	)
	pool.Register("fail", func(ctx context.Context, task *taskmodel.Task) error {
		return errors.New("permanent-ish")
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("fail", []byte(`{}`), taskmodel.TaskOptions{
		ID: "md-1", Queue: queue, MaxRetry: taskmodel.Ptr(0),
	})))

	require.Eventually(t, func() bool {
		pending, err := rdb.XPending(ctx, qk.Stream(), group).Result()
		return err == nil && pending.Count >= 1
	}, 15*time.Second, 50*time.Millisecond)

	dlq, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
	require.Equal(t, int64(0), dlq)
}

// TestSettlementMatrix_CompleteTaskBrokerFailureLeavesPEL: handler succeeds but CompleteTask fails → PEL retained.
func TestSettlementMatrix_CompleteTaskBrokerFailureLeavesPEL(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "settlement_complete")
	group := "settlement-complete-g"
	flushQueue(ctx, rdb, queue, "ok-pel-1")
	qk := keys.KeysFor(queue)
	lc := settlementLC()

	failBr := &settlementFailBroker{} // CompleteTask always errors
	var ran atomic.Int64
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("settlement-complete-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithBroker(failBr),
	)
	pool.Register("ok", func(ctx context.Context, task *taskmodel.Task) error {
		ran.Add(1)
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("ok", []byte(`{}`), taskmodel.TaskOptions{
		ID: "ok-pel-1", Queue: queue,
	})))

	require.Eventually(t, func() bool { return ran.Load() >= 1 }, 15*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		pending, err := rdb.XPending(ctx, qk.Stream(), group).Result()
		return err == nil && pending.Count >= 1
	}, 15*time.Second, 50*time.Millisecond)
}

// TestSettlementMatrix_CancelMidRun: long handler waits on ctx; CancelTask aborts in-flight work.
// Settlement after cancel depends on handler return; we assert cancel marker + handler observes cancel.
func TestSettlementMatrix_CancelMidRun(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "settlement_midcancel")
	flushQueue(ctx, rdb, queue, "mid-1")
	lc := settlementLC()

	started := make(chan struct{})
	var sawCancel atomic.Bool
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("settlement-midcancel-g"),
		mqworker.WithConsumer("settlement-midcancel-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("slow", func(ctx context.Context, task *taskmodel.Task) error {
		close(started)
		select {
		case <-ctx.Done():
			sawCancel.Store(true)
			return ctx.Err()
		case <-time.After(20 * time.Second):
			return errors.New("timeout waiting for cancel")
		}
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("slow", []byte(`{}`), taskmodel.TaskOptions{
		ID: "mid-1", Queue: queue, MaxRetry: taskmodel.Ptr(0),
	})))

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler did not start")
	}

	require.NoError(t, c.CancelTask(ctx, queue, "mid-1"))
	require.Eventually(t, func() bool { return sawCancel.Load() }, 15*time.Second, 50*time.Millisecond)
}

// TestSettlementMatrix_UniqueDuplicateRejected: second enqueue with same UniqueKey
// returns ErrDuplicateTask while the lock is held (Redis-visible unique key).
func TestSettlementMatrix_UniqueDuplicateRejected(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_uniq_dup")
	flushQueue(ctx, rdb, queue, "u1", "u2")
	qk := keys.KeysFor(queue)
	lc := settlementLC()
	uniqueKey := "dedup-key"
	lockKey := qk.Unique(uniqueKey)

	c := NewTestClient(rdb, lc)
	t1 := taskmodel.NewTask("job", []byte(`1`), taskmodel.TaskOptions{
		ID: "u1", Queue: queue, UniqueKey: uniqueKey, UniqueTTL: time.Minute,
	})
	require.NoError(t, c.Enqueue(ctx, t1))
	// Lock must exist after successful unique enqueue.
	n, err := rdb.Exists(ctx, lockKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	t2 := taskmodel.NewTask("job", []byte(`2`), taskmodel.TaskOptions{
		ID: "u2", Queue: queue, UniqueKey: uniqueKey, UniqueTTL: time.Minute,
	})
	err = c.Enqueue(ctx, t2)
	require.ErrorIs(t, err, lifecycle.ErrDuplicateTask)

	WaitStreamLen(t, ctx, rdb, queue, 1, 5*time.Second)
	owner, err := rdb.Get(ctx, lockKey).Result()
	require.NoError(t, err)
	require.Equal(t, "u1", owner)
}

// TestSettlementMatrix_UniqueUnlockOnSuccess: CompleteTask releases unique lock so a
// second enqueue with the same key succeeds after the first finishes.
func TestSettlementMatrix_UniqueUnlockOnSuccess(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_uniq_ok")
	flushQueue(ctx, rdb, queue, "ok-u1", "ok-u2")
	qk := keys.KeysFor(queue)
	lc := settlementLC()
	uniqueKey := "unlock-key"
	lockKey := qk.Unique(uniqueKey)

	var ran atomic.Int64
	pool := NewTestWorkerPool(rdb, queue, "sm-uniq-g", "sm-uniq-c", 1, lc)
	pool.Register("job", func(ctx context.Context, task *taskmodel.Task) error {
		ran.Add(1)
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := NewTestClient(rdb, lc)
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`a`), taskmodel.TaskOptions{
		ID: "ok-u1", Queue: queue, UniqueKey: uniqueKey, UniqueTTL: time.Minute,
	})))

	require.Eventually(t, func() bool { return ran.Load() >= 1 }, 15*time.Second, 50*time.Millisecond)
	WaitKeyGone(t, ctx, rdb, lockKey, 15*time.Second)

	// Second unique enqueue succeeds after unlock.
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`b`), taskmodel.TaskOptions{
		ID: "ok-u2", Queue: queue, UniqueKey: uniqueKey, UniqueTTL: time.Minute,
	})))
	require.Eventually(t, func() bool { return ran.Load() >= 2 }, 15*time.Second, 50*time.Millisecond)
}

// TestSettlementMatrix_UniqueUntilStartReleasesEarly: UniqueUntilStart drops the lock
// when the handler starts so a second unique enqueue can succeed while the first runs.
func TestSettlementMatrix_UniqueUntilStartReleasesEarly(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_uniq_start")
	flushQueue(ctx, rdb, queue, "s1", "s2")
	qk := keys.KeysFor(queue)
	lc := settlementLC()
	uniqueKey := "until-start"
	lockKey := qk.Unique(uniqueKey)

	started := make(chan struct{})
	block := make(chan struct{})
	var ran atomic.Int64
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("sm-uniq-start-g"),
		mqworker.WithConsumer("sm-uniq-start-c"),
		mqworker.WithConcurrency(2),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("job", func(ctx context.Context, task *taskmodel.Task) error {
		ran.Add(1)
		if task.ID == "s1" {
			close(started)
			select {
			case <-block:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`1`), taskmodel.TaskOptions{
		ID: "s1", Queue: queue, UniqueKey: uniqueKey, UniqueTTL: time.Minute,
		UniqueScope: taskmodel.UniqueUntilStart,
	})))

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("first task did not start")
	}
	// Lock released at start — second enqueue must succeed while first still running.
	require.Eventually(t, func() bool {
		n, _ := rdb.Exists(ctx, lockKey).Result()
		return n == 0
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`2`), taskmodel.TaskOptions{
		ID: "s2", Queue: queue, UniqueKey: uniqueKey, UniqueTTL: time.Minute,
		UniqueScope: taskmodel.UniqueUntilStart,
	})))
	require.Eventually(t, func() bool { return ran.Load() >= 2 }, 15*time.Second, 50*time.Millisecond)
	close(block)
}

// TestSettlementMatrix_StalledEventOnPELReclaim: PEL janitor emits events.TypeStalled
// when reclaiming an idle pending message.
func TestSettlementMatrix_StalledEventOnPELReclaim(t *testing.T) {
	ctx, _, rdb := settlementRedis(t)
	queue := UniqueQueue(t, "sm_stalled")
	flushQueue(ctx, rdb, queue, "stall-1")
	qk := keys.KeysFor(queue)
	group := "sm-stalled-g"
	lc := settlementLC()

	// Build a serialized task and leave it in the PEL under a dead consumer.
	task := taskmodel.NewTask("job", []byte(`{}`), taskmodel.TaskOptions{ID: "stall-1", Queue: queue})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, qk.Stream(), group, "0").Err())
	msgID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: qk.Stream(),
		Values: map[string]interface{}{"task": string(ser)},
	}).Result()
	require.NoError(t, err)
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "dead-worker",
		Streams: []string{qk.Stream(), ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)
	_ = msgID

	// Worker pool with very aggressive PEL reclaim + codec-aware janitor.
	var reprocessed atomic.Int64
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("rescuer"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithJanitorInterval(50*time.Millisecond),
		mqworker.WithJanitorMinIdleTime(1*time.Millisecond),
	)
	pool.Register("job", func(ctx context.Context, task *taskmodel.Task) error {
		reprocessed.Add(1)
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	require.Eventually(t, func() bool { return reprocessed.Load() >= 1 }, 15*time.Second, 50*time.Millisecond)

	// Stalled event should appear on the events stream.
	require.Eventually(t, func() bool {
		evs, err := rdb.XRange(ctx, qk.Events(), "-", "+").Result()
		if err != nil {
			return false
		}
		for _, e := range evs {
			if typ, _ := e.Values["type"].(string); typ == "stalled" {
				if tid, _ := e.Values["task_id"].(string); tid == "stall-1" || tid != "" {
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 50*time.Millisecond)
}
