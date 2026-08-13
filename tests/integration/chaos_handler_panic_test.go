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

// TestChaos_HandlerPanic_Recovery verifies that when a task handler panics,
// the worker pool goroutine recovers gracefully without crashing, logs the error,
// and routes the panicked task to the DLQ/retry queue.
func TestChaos_HandlerPanic_Recovery(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "chaos-panic")
	qk := keys.KeysFor(queue)
	lc := DefaultTestLifecycle()
	client := NewTestClient(rdb, lc)

	var panicCount atomic.Int64
	var validProcessed atomic.Bool

	pool := NewTestWorkerPool(rdb, queue, "group-panic", "consumer-panic", 2, lc)
	pool.Register("task:panic", func(taskCtx context.Context, task *taskmodel.Task) error {
		panicCount.Add(1)
		panic("CRITICAL RUNTIME ERROR: simulated nil pointer dereference inside handler")
	})
	pool.Register("task:valid", func(taskCtx context.Context, task *taskmodel.Task) error {
		validProcessed.Store(true)
		return nil
	})

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	// Enqueue a task that panics with MaxRetry=0
	panicTask := taskmodel.NewTask("task:panic", []byte(`{"panic":true}`), taskmodel.TaskOptions{
		ID:       "task-panic-001",
		Queue:    queue,
		MaxRetry: taskmodel.Ptr(0),
	})
	require.NoError(t, client.Enqueue(ctx, panicTask))

	// Enqueue a normal task right after
	validTask := taskmodel.NewTask("task:valid", []byte(`{"valid":true}`), taskmodel.TaskOptions{
		ID:    "task-valid-002",
		Queue: queue,
	})
	require.NoError(t, client.Enqueue(ctx, validTask))

	store := meta.NewStore(rdb)

	// 1. Verify panic task went to DLQ and recorded error
	WaitUntil(t, func() bool {
		n, _ := rdb.ZCard(ctx, qk.DLQ()).Result()
		info, _ := store.Get(ctx, queue, "task-panic-001")
		return n >= 1 && info != nil && info.State == meta.StateDLQ
	}, 10*time.Second, 40*time.Millisecond, "panic task was not routed to DLQ")

	// 2. Verify worker pool survived and processed subsequent valid task
	WaitUntil(t, func() bool {
		return validProcessed.Load()
	}, 10*time.Second, 40*time.Millisecond, "worker pool crashed after panic and failed to process valid task")

	require.GreaterOrEqual(t, panicCount.Load(), int64(1))
}
