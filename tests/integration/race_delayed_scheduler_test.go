package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

// TestTaskMQ_DelayedScheduler_MultiSchedulerRace simulates horizontal scaling where
// 3 separate DelayedScheduler daemon instances concurrently compete on the same queue:
// 1. 50 delayed tasks with ready timestamp (now - 100ms) are enqueued into the delayed ZSET.
// 2. 3 scheduler instances concurrently poll and promote tasks via the Redis Lua script.
// 3. Invariants:
//    - Redis Stream strictly receives exactly 50 tasks (zero duplicate promotions across schedulers).
//    - Delayed ZSET is completely emptied (ZCARD == 0).
//    - A worker consuming the stream receives strictly 50 unique tasks without duplicate executions.
func TestTaskMQ_DelayedScheduler_MultiSchedulerRace(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "multi_sched")
	group := "sched_grp"
	qk := keys.KeysFor(queue)
	streamKey := qk.Stream()
	delayedKey := qk.Delayed()
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	jsonCodec := codec.JSONCodec{}
	cli := NewTestClient(rdb, lc)

	// Pre-create consumer group
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamKey, group, "0").Err())

	const taskCount = 50
	const schedulerCount = 3

	schedCtx, schedCancel := context.WithCancel(ctx)
	defer schedCancel()

	cronMgr := runner.NewCronManager(rdb, zap.NewNop(), queue, jsonCodec, time.Minute, time.Minute, 100, 1000, lc)

	// Start 3 concurrent DelayedScheduler instances
	var schedWg sync.WaitGroup
	for i := 0; i < schedulerCount; i++ {
		schedWg.Add(1)
		s := runner.NewDelayedScheduler(rdb, zap.NewNop(), queue, cronMgr, jsonCodec, 10*time.Millisecond)
		go func(instance runner.Runner) {
			defer schedWg.Done()
			_ = instance.Run(schedCtx)
		}(s)
	}

	// Concurrently enqueue 50 overdue delayed tasks to provoke maximum race contention
	readyTime := time.Now().Add(-200 * time.Millisecond)
	var enqueueWg sync.WaitGroup
	for i := 0; i < taskCount; i++ {
		enqueueWg.Add(1)
		taskID := fmt.Sprintf("sched-race-%02d", i)
		go func(id string) {
			defer enqueueWg.Done()
			tsk := &taskmodel.Task{
				ID:       id,
				Queue:    queue,
				Name:     "timed.action",
				MaxRetry: 1,
				Payload:  []byte(fmt.Sprintf(`{"id":"%s"}`, id)),
			}
			err := cli.EnqueueAt(ctx, tsk, readyTime)
			require.NoError(t, err)
		}(taskID)
	}
	enqueueWg.Wait()

	// Wait until Stream receives all tasks
	WaitUntil(t, func() bool {
		n, err := rdb.XLen(ctx, streamKey).Result()
		return err == nil && n == int64(taskCount)
	}, 5*time.Second, 10*time.Millisecond, "stream must receive exactly 50 promoted tasks")

	// Invariant 1: Stream XLEN must be strictly 50
	streamLen, err := rdb.XLen(ctx, streamKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(taskCount), streamLen, "Stream must not have duplicate XADD from competing schedulers")

	// Invariant 2: Delayed ZSET must be completely empty
	zcard, err := rdb.ZCard(ctx, delayedKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), zcard, "Delayed ZSET must be completely drained")

	// Invariant 3: Worker consumes all 50 tasks; verify zero duplicate deliveries
	var processedTasks sync.Map
	var processedCount atomic.Int64
	doneCh := make(chan struct{})

	w := NewTestWorkerPool(rdb, queue, group, "sched-consumer", 5, lc)
	w.Register("timed.action", func(c context.Context, t *taskmodel.Task) error {
		processedTasks.Store(t.ID, true)
		if processedCount.Add(1) == int64(taskCount) {
			close(doneCh)
		}
		return nil
	})

	require.NoError(t, w.Start(ctx))
	defer w.Stop(ctx)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for worker to process all 50 tasks (processed: %d)", processedCount.Load())
	}

	// Verify all 50 distinct task IDs were processed
	for i := 0; i < taskCount; i++ {
		id := fmt.Sprintf("sched-race-%02d", i)
		_, ok := processedTasks.Load(id)
		require.True(t, ok, "task %s must have been processed", id)
	}

	schedCancel()
	schedWg.Wait()
}
