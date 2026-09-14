package integration

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// TestTaskMQ_Settlement_CancellationImmunity asserts the critical architectural invariant:
// All broker settlement actions (CompleteTask, MoveToDLQ, ScheduleRetry) MUST utilize
// settleCtx (context.WithoutCancel(parent) with bounded timeout).
// Even if the execution context timed out or was explicitly cancelled by caller/SIGTERM,
// Redis outcome transitions and PEL XACK MUST execute cleanly to prevent PEL orphan leaks.
func TestTaskMQ_Settlement_CancellationImmunity(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "settle_immune")
	group := "immune_grp"
	qk := keys.KeysFor(queue)
	streamKey := qk.Stream()
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	jsonCodec := codec.JSONCodec{}
	b := broker.NewRedisBroker(rdb, jsonCodec, lc)

	// Pre-create consumer group
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamKey, group, "0").Err())

	// Part 1: Direct Broker Method Invariant under Already-Cancelled Context
	t.Run("DirectBrokerCalls_WithCancelledContext", func(t *testing.T) {
		// 1.1 CompleteTask with cancelled ctx
		tsk1 := &taskmodel.Task{ID: "task-c1", Queue: queue, Name: "action"}
		ser1, err := jsonCodec.Marshal(tsk1)
		require.NoError(t, err)
		msgID1, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: map[string]interface{}{"task": ser1}}).Result()
		require.NoError(t, err)
		_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "c1", Streams: []string{streamKey, ">"}, Count: 1}).Result()
		require.NoError(t, err)

		cancelledCtx, cancelFn := context.WithCancel(ctx)
		cancelFn() // Context is already cancelled before calling broker!

		err = b.CompleteTask(cancelledCtx, tsk1, streamKey, msgID1, group)
		require.NoError(t, err, "CompleteTask must succeed despite cancelled context")

		// 1.2 MoveToDLQ with cancelled ctx
		tsk2 := &taskmodel.Task{ID: "task-c2", Queue: queue, Name: "action"}
		ser2, err := jsonCodec.Marshal(tsk2)
		require.NoError(t, err)
		msgID2, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: map[string]interface{}{"task": ser2}}).Result()
		require.NoError(t, err)
		_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "c1", Streams: []string{streamKey, ">"}, Count: 1}).Result()
		require.NoError(t, err)

		err = b.MoveToDLQ(cancelledCtx, tsk2, streamKey, msgID2, group, qk.DLQ())
		require.NoError(t, err, "MoveToDLQ must succeed despite cancelled context")

		// 1.3 ScheduleRetry with cancelled ctx
		tsk3 := &taskmodel.Task{ID: "task-c3", Queue: queue, Name: "action"}
		ser3, err := jsonCodec.Marshal(tsk3)
		require.NoError(t, err)
		msgID3, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: map[string]interface{}{"task": ser3}}).Result()
		require.NoError(t, err)
		_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "c1", Streams: []string{streamKey, ">"}, Count: 1}).Result()
		require.NoError(t, err)

		err = b.ScheduleRetry(cancelledCtx, tsk3, streamKey, msgID3, group, time.Now().Add(time.Minute))
		require.NoError(t, err, "ScheduleRetry must succeed despite cancelled context")

		// Verify PEL is completely empty (all 3 were cleanly XACKed!)
		pend, err := rdb.XPending(ctx, streamKey, group).Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), pend.Count, "All messages must be XACKed from PEL despite cancelled ctx")
	})

	// Part 2: End-to-End Worker Pipeline Timeout & Settlement
	t.Run("WorkerPipeline_TimeoutAndDLQSettlement", func(t *testing.T) {
		cli := NewTestClient(rdb, lc)
		w := NewTestWorkerPool(rdb, queue, group, "immune-worker", 1, lc)

		w.Register("slow.action", func(c context.Context, t *taskmodel.Task) error {
			// Handler blocks until context times out
			<-c.Done()
			return c.Err()
		})

		require.NoError(t, w.Start(ctx))
		defer w.Stop(ctx)

		// Enqueue task with tight 50ms Timeout and MaxRetry 0 (immediate DLQ on failure)
		timeoutTask := &taskmodel.Task{
			ID:        "task-timeout-dlq",
			Queue:     queue,
			Name:      "slow.action",
			TimeoutMs: 50,
			MaxRetry:  0,
			Payload:   []byte(`{"data":"timeout"}`),
		}
		require.NoError(t, cli.Enqueue(ctx, timeoutTask))

		// Wait until task arrives in DLQ
		WaitUntil(t, func() bool {
			n, err := rdb.ZCard(ctx, qk.DLQ()).Result()
			return err == nil && n >= 1
		}, 5*time.Second, 20*time.Millisecond, "task must be settled to DLQ after timeout")

		// Verify that stream PEL has 0 pending items (clean XACK settlement!)
		WaitUntil(t, func() bool {
			pend, err := rdb.XPending(ctx, streamKey, group).Result()
			return err == nil && pend.Count == 0
		}, 3*time.Second, 20*time.Millisecond, "PEL must be cleared of timed-out task")
	})
}
