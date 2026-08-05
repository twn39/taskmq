package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

// TestTaskMQ_EnqueueContextCanceled: producer fails fast when caller cancels mid-flight.
func TestTaskMQ_EnqueueContextCanceled(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	lc := DefaultTestLifecycle()
	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))

	cctx, cancel := context.WithCancel(ctx)
	cancel() // already canceled
	err := c.Enqueue(cctx, taskmodel.NewTask("job", []byte(`{}`), taskmodel.TaskOptions{
		ID: "cancel-enq-1", Queue: UniqueQueue(t, "res_enq"),
	}))
	require.Error(t, err)
}

// TestTaskMQ_WorkerSurvivesHandlerTimeout: task TimeoutMs cancels handler ctx; message settles (retry/DLQ).
func TestTaskMQ_WorkerSurvivesHandlerTimeout(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	queue := UniqueQueue(t, "res_timeout")
	FlushQueue(ctx, rdb, queue, "to-1")
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())

	var sawCancel atomic.Bool
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("res-to-g"),
		mqworker.WithConsumer("res-to-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("slow", func(ctx context.Context, task *taskmodel.Task) error {
		select {
		case <-ctx.Done():
			sawCancel.Store(true)
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return nil
		}
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	// 200ms task timeout → handler ctx cancelled.
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("slow", []byte(`{}`), taskmodel.TaskOptions{
		ID: "to-1", Queue: queue, MaxRetry: taskmodel.Ptr(0),
		Timeout: 200 * time.Millisecond,
	})))

	require.Eventually(t, func() bool { return sawCancel.Load() }, 15*time.Second, 50*time.Millisecond)
}
