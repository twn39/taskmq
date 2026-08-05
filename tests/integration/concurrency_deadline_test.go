package integration

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

// Two consumers in the same group process each message once (no double-run).
func TestTaskMQ_MultiConsumer_ProcessesEachTaskOnce(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	queue := UniqueQueue(t, "multi_once")
	FlushQueue(ctx, rdb, queue)
	lc := DefaultTestLifecycle()
	group := "multi-once-g"

	var runs atomic.Int64
	makePool := func(consumer string) mqworker.Worker {
		pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
			mqworker.WithGroup(group),
			mqworker.WithConsumer(consumer),
			mqworker.WithConcurrency(3),
			mqworker.WithCodec(codec.JSONCodec{}),
			mqworker.WithLifecycle(lc),
		)
		pool.Register("work", func(ctx context.Context, task *taskmodel.Task) error {
			runs.Add(1)
			time.Sleep(20 * time.Millisecond)
			return nil
		})
		return pool
	}
	p1 := makePool("c1")
	p2 := makePool("c2")
	require.NoError(t, p1.Start(ctx))
	require.NoError(t, p2.Start(ctx))
	defer p1.Stop(ctx)
	defer p2.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	const n = 24
	for i := 0; i < n; i++ {
		require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("work", []byte(`{}`), taskmodel.TaskOptions{
			ID: "once-" + strconv.Itoa(i), Queue: queue,
		})))
	}

	WaitUntil(t, func() bool {
		pending, err := rdb.XPending(ctx, keys.KeysFor(queue).Stream(), group).Result()
		return err == nil && runs.Load() >= int64(n) && pending.Count == 0
	}, 20*time.Second, 40*time.Millisecond, "all tasks should run and be settled")

	require.Equal(t, int64(n), runs.Load())
}

// Concurrency option must cap simultaneous handler executions for a single pool.
func TestTaskMQ_ConcurrencyCap(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	queue := UniqueQueue(t, "conc_cap")
	FlushQueue(ctx, rdb, queue)
	lc := DefaultTestLifecycle()

	const conc = 2
	var inFlight atomic.Int64
	var maxInFlight atomic.Int64
	var done atomic.Int64
	block := make(chan struct{})

	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("conc-g"),
		mqworker.WithConsumer("conc-c"),
		mqworker.WithConcurrency(conc),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("slow", func(ctx context.Context, task *taskmodel.Task) error {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		defer inFlight.Add(-1)
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
		done.Add(1)
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer func() {
		close(block)
		pool.Stop(ctx)
	}()

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	for i := 0; i < 6; i++ {
		require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("slow", []byte(`{}`), taskmodel.TaskOptions{
			ID: "conc-" + strconv.Itoa(i), Queue: queue,
		})))
	}

	WaitUntil(t, func() bool {
		return inFlight.Load() >= int64(conc)
	}, 10*time.Second, 20*time.Millisecond, "should reach concurrency cap")

	// Hold briefly and assert never exceeded.
	time.Sleep(150 * time.Millisecond)
	require.LessOrEqual(t, maxInFlight.Load(), int64(conc))
}

// Absolute DeadlineMs that is already in the past should cancel the handler context quickly.
func TestTaskMQ_DeadlineAlreadyExpired(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	queue := UniqueQueue(t, "deadline")
	FlushQueue(ctx, rdb, queue, "dl-1")
	lc := DefaultTestLifecycle()
	qk := keys.KeysFor(queue)

	var gotCanceled atomic.Bool
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("deadline-g"),
		mqworker.WithConsumer("deadline-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithRetryPolicy(policy.NewExponentialBackoff(10*time.Millisecond, time.Second, false)),
	)
	pool.Register("work", func(ctx context.Context, task *taskmodel.Task) error {
		// EffectiveTimeout for past deadline is ~1ns; context should already be done or soon.
		select {
		case <-ctx.Done():
			gotCanceled.Store(true)
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return nil
		}
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	past := time.Now().Add(-2 * time.Second)
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("work", []byte(`{}`), taskmodel.TaskOptions{
		ID: "dl-1", Queue: queue, MaxRetry: taskmodel.Ptr(0), Deadline: past,
	})))

	store := meta.NewStore(rdb)
	WaitUntil(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
		info, _ := store.Get(ctx, queue, "dl-1")
		return gotCanceled.Load() && n >= 1 && info != nil && info.State == meta.StateDLQ
	}, 15*time.Second, 40*time.Millisecond, "past deadline failure with MaxRetry=0 goes to DLQ and updates meta state")

	require.True(t, gotCanceled.Load(), "handler should observe canceled context from past deadline")
}
