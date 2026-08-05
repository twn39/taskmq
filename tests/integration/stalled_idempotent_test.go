package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

// TestTaskMQ_StalledReclaim_ProcessesOnce: one message claimed by a dead consumer is
// reclaimed via PEL janitor / second consumer and handler runs exactly once to success.
func TestTaskMQ_StalledReclaim_ProcessesOnce(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	queue := UniqueQueue(t, "stalled_once")
	group := "stalled-group"
	qk := keys.KeysFor(queue)
	FlushQueue(ctx, rdb, queue, "stall-1")
	lc := DefaultTestLifecycle()

	// Pre-create group and leave a message pending on a dead consumer (no worker).
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, qk.Stream(), group, "0").Err())
	task := taskmodel.NewTask("work", []byte(`{}`), taskmodel.TaskOptions{
		ID: "stall-1", Queue: queue, MaxRetry: taskmodel.Ptr(3),
	})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: qk.Stream(),
		Values: map[string]interface{}{"task": string(ser)},
	}).Result()
	require.NoError(t, err)
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "dead-c", Streams: []string{qk.Stream(), ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)
	_ = id

	var runs atomic.Int64
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("rescuer"),
		mqworker.WithConcurrency(2),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		// Default factory builds PEL janitor; shorten reclaim cadence for the test.
		mqworker.WithJanitorInterval(50*time.Millisecond),
		mqworker.WithJanitorMinIdleTime(10*time.Millisecond),
	)
	pool.Register("work", func(ctx context.Context, task *taskmodel.Task) error {
		runs.Add(1)
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	require.Eventually(t, func() bool { return runs.Load() >= 1 }, 20*time.Second, 50*time.Millisecond)
	// Give a short window for accidental double reclaim.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int64(1), runs.Load(), "handler must run exactly once after reclaim")

	// Stream/PEL empty after successful complete.
	require.Eventually(t, func() bool {
		pending, err := rdb.XPending(ctx, qk.Stream(), group).Result()
		if err != nil {
			return false
		}
		n, _ := rdb.XLen(ctx, qk.Stream()).Result()
		return pending.Count == 0 && n == 0
	}, 10*time.Second, 50*time.Millisecond)
}

// TestTaskMQ_MultiConsumer_IdempotentComplete: two live consumers race on different tasks;
// each task completes once (extends concurrency_deadline multi-consumer coverage with unique IDs).
func TestTaskMQ_MultiConsumer_IdempotentComplete(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	queue := UniqueQueue(t, "multi_once")
	group := "multi-once-g"
	FlushQueue(ctx, rdb, queue)
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())

	var total atomic.Int64
	seen := make(chan string, 20)
	startPool := func(consumer string) mqworker.Worker {
		p := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
			mqworker.WithGroup(group),
			mqworker.WithConsumer(consumer),
			mqworker.WithConcurrency(2),
			mqworker.WithCodec(codec.JSONCodec{}),
			mqworker.WithLifecycle(lc),
		)
		p.Register("work", func(ctx context.Context, task *taskmodel.Task) error {
			total.Add(1)
			seen <- task.ID
			return nil
		})
		require.NoError(t, p.Start(ctx))
		return p
	}
	p1 := startPool("c1")
	p2 := startPool("c2")
	defer p1.Stop(ctx)
	defer p2.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	const n = 10
	for i := 0; i < n; i++ {
		require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("work", []byte(`{}`), taskmodel.TaskOptions{
			ID: fmt.Sprintf("task-%d", i), Queue: queue,
		})))
	}

	require.Eventually(t, func() bool { return total.Load() >= int64(n) }, 20*time.Second, 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int64(n), total.Load(), "each task should complete exactly once across consumers")
}
