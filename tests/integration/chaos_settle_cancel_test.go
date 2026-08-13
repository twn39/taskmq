package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/meta"

	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// TestChaos_SettleCtx_Isolation verifies that when the execution context is cancelled
// inside the handler, the settlement action (MoveToDLQ/Complete/ScheduleRetry) still
// executes cleanly in Redis via context.WithoutCancel (settleCtx).
func TestChaos_SettleCtx_Isolation(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "chaos-settle")
	qk := keys.KeysFor(queue)
	lc := DefaultTestLifecycle()
	client := NewTestClient(rdb, lc)

	var handlerExecuted atomic.Bool

	pool := NewTestWorkerPool(rdb, queue, "group-chaos", "consumer-1", 1, lc)
	pool.Register("task:timeout-cancel", func(taskCtx context.Context, t *taskmodel.Task) error {
		handlerExecuted.Store(true)

		// Create a subcontext that immediately cancels, simulating a timeout or parent cancellation
		subCtx, subCancel := context.WithCancel(taskCtx)
		subCancel()

		// Wait briefly so context.Done() is closed
		<-subCtx.Done()

		// Return a non-retryable error to trigger DLQ settlement
		return taskmodel.ErrSkipRetry
	})

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	task := taskmodel.NewTask("task:timeout-cancel", []byte(`{"chaos":true}`), taskmodel.TaskOptions{
		ID:       "task-chaos-cancel-1",
		Queue:    queue,
		MaxRetry: taskmodel.Ptr(0),
	})
	require.NoError(t, client.Enqueue(ctx, task))

	store := meta.NewStore(rdb)

	// Verify that despite handler context cancellation, the task was successfully moved to DLQ in Redis
	WaitUntil(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
		info, _ := store.Get(ctx, queue, "task-chaos-cancel-1")
		return handlerExecuted.Load() && n >= 1 && info != nil && info.State == meta.StateDLQ
	}, 10*time.Second, 40*time.Millisecond, "settleCtx failed to persist DLQ state after context cancellation")

	require.True(t, handlerExecuted.Load())
}
