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
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

// TestTaskMQ_WorkerPool_GracefulShutdownInFlightDrain tests real-world SIGTERM / graceful shutdown:
// 1. WorkerPool starts with concurrency 5.
// 2. 15 tasks are enqueued into the stream.
// 3. 5 tasks are pulled and begin processing concurrently (each sleeping 150ms).
// 4. As soon as all 5 are in-flight, w.Stop() is invoked.
// 5. Invariants:
//    - The 5 in-flight tasks finish execution and complete XACK settlement during shutdown drain.
//    - The remaining 10 un-pulled tasks remain safe and untouched in Redis Stream.
//    - A replacement Worker starts and consumes the remaining 10 tasks.
//    - Exactly 15 tasks are executed in total (zero data loss, zero duplicate executions).
func TestTaskMQ_WorkerPool_GracefulShutdownInFlightDrain(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "drain_shutdown")
	group := "drain_grp"
	qk := keys.KeysFor(queue)
	streamKey := qk.Stream()
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	cli := NewTestClient(rdb, lc)

	// Pre-create consumer group at "0"
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamKey, group, "0").Err())

	const totalTasks = 15
	const concurrency = 5

	// Enqueue 15 tasks
	for i := 0; i < totalTasks; i++ {
		tsk := &taskmodel.Task{
			ID:       fmt.Sprintf("drain-task-%02d", i),
			Queue:    queue,
			Name:     "heavy.compute",
			MaxRetry: 1,
			Payload:  []byte(fmt.Sprintf(`{"idx":%d}`, i)),
		}
		require.NoError(t, cli.Enqueue(ctx, tsk))
	}

	// Verify all 15 are in stream
	xlen, err := rdb.XLen(ctx, streamKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(totalTasks), xlen)

	var inFlightCount atomic.Int64
	var w1Completed atomic.Int64
	var allExecutedTasks sync.Map

	allInFlightBarrier := make(chan struct{})
	var barrierOnce sync.Once

	w1 := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("consumer-node-1"),
		mqworker.WithConcurrency(concurrency),
		mqworker.WithSyncExecution(true),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithSchedulerPollInterval(10*time.Millisecond),
		mqworker.WithJanitorInterval(50*time.Millisecond),
	)
	w1.Register("heavy.compute", func(c context.Context, t *taskmodel.Task) error {
		current := inFlightCount.Add(1)
		defer inFlightCount.Add(-1)

		if current >= int64(concurrency) {
			barrierOnce.Do(func() {
				close(allInFlightBarrier)
			})
		}

		// Hold task in-flight for 150ms to ensure shutdown overlaps execution
		time.Sleep(150 * time.Millisecond)

		allExecutedTasks.Store(t.ID, "w1")
		w1Completed.Add(1)
		return nil
	})

	require.NoError(t, w1.Start(ctx))

	// Wait until concurrency limit (5 tasks) is fully saturated in-flight
	select {
	case <-allInFlightBarrier:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for 5 tasks to enter in-flight state")
	}

	// Stop w1 with a bounded shutdown timeout context
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	stopDone := make(chan struct{})
	go func() {
		w1.Stop(shutdownCtx)
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(6 * time.Second):
		t.Fatal("worker w1 Stop hung or exceeded drain timeout")
	}

	// At this point, w1 has shut down.
	// Exactly 5 tasks must have been completed by w1 during the drain!
	w1Count := w1Completed.Load()
	require.Equal(t, int64(concurrency), w1Count, "Worker 1 should have drained exactly 5 in-flight tasks")

	// Verify that PEL for group is 0 (all 5 drained tasks were acknowledged)
	pend, err := rdb.XPending(ctx, streamKey, group).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), pend.Count, "PEL should have 0 pending tasks after graceful drain")

	// Now launch replacement Worker w2 to resume and consume the remaining 10 unpulled tasks
	var w2Completed atomic.Int64
	w2Done := make(chan struct{})

	w2 := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup(group),
		mqworker.WithConsumer("consumer-node-2"),
		mqworker.WithConcurrency(concurrency),
		mqworker.WithSyncExecution(true),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithSchedulerPollInterval(10*time.Millisecond),
		mqworker.WithJanitorInterval(50*time.Millisecond),
	)
	w2.Register("heavy.compute", func(c context.Context, t *taskmodel.Task) error {
		allExecutedTasks.Store(t.ID, "w2")
		if w2Completed.Add(1) == int64(totalTasks-concurrency) {
			close(w2Done)
		}
		return nil
	})

	require.NoError(t, w2.Start(ctx))
	defer w2.Stop(ctx)

	select {
	case <-w2Done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for w2 to finish remaining tasks (got %d)", w2Completed.Load())
	}

	require.Equal(t, int64(totalTasks-concurrency), w2Completed.Load(), "w2 must finish all remaining 10 tasks")

	// Final Invariant Check: Strictly 15 unique task IDs processed in total
	var distinctCount int
	for i := 0; i < totalTasks; i++ {
		id := fmt.Sprintf("drain-task-%02d", i)
		_, exists := allExecutedTasks.Load(id)
		require.True(t, exists, "task %s must have been executed", id)
		distinctCount++
	}
	require.Equal(t, totalTasks, distinctCount, "Total processed tasks must strictly equal 15 with zero loss and zero duplicates")
}
