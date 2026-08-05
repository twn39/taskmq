package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/events"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	"github.com/twn39/taskmq/internal/taskmq/metricsq"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

// Inspect / events / progress / metrics against real Redis (Asynq Inspector-style smoke).
func TestTaskMQ_Inspect_GetTaskInfoProgressEventsMetrics(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	queue := UniqueQueue(t, "inspect")
	taskID := "inspect-1"
	FlushQueue(ctx, rdb, queue, taskID)
	lc := DefaultTestLifecycle()

	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("inspect-g"),
		mqworker.WithConsumer("inspect-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("job", func(ctx context.Context, task *taskmodel.Task) error {
		// Progress via ConsumeContext is wired inside Process; use client inspector path here
		// after active, then complete with result via SetResult pattern (handler returns nil).
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`{"a":1}`), taskmodel.TaskOptions{
		ID: taskID, Queue: queue,
	})))

	// Pending meta after enqueue.
	WaitUntil(t, func() bool {
		info, _ := c.GetTaskInfo(ctx, queue, taskID)
		return info != nil && (info.State == meta.StatePending || info.State == meta.StateActive || info.State == meta.StateCompleted)
	}, 10*time.Second, 30*time.Millisecond, "task meta should appear")

	// Client-side progress update (inspector API).
	require.NoError(t, c.UpdateProgress(ctx, queue, taskID, 40, "halfway"))
	info, err := c.GetTaskInfo(ctx, queue, taskID)
	require.NoError(t, err)
	require.NotNil(t, info)
	if info.State != meta.StateCompleted {
		require.Equal(t, 40, info.Progress)
		require.Equal(t, "halfway", info.ProgressData)
	}

	// Wait for completion + completed meta retention.
	WaitUntil(t, func() bool {
		info, _ := c.GetTaskInfo(ctx, queue, taskID)
		return info != nil && info.State == meta.StateCompleted
	}, 15*time.Second, 40*time.Millisecond, "task should complete")

	// Events stream should include enqueued and completed (and possibly progress/active).
	evs, err := c.ListEvents(ctx, queue, 50)
	require.NoError(t, err)
	require.NotEmpty(t, evs)
	types := map[string]bool{}
	for _, e := range evs {
		types[e.Type] = true
	}
	require.True(t, types[events.TypeEnqueued] || types[events.TypeCompleted], "expected lifecycle events, got %#v", types)

	// metricsq counters
	ms := metricsq.NewStore(rdb)
	WaitUntil(t, func() bool {
		snap, _ := ms.Snapshot(ctx, queue)
		return snap[metricsq.FieldCompleted] >= 1 || snap[metricsq.FieldProcessed] >= 1
	}, 10*time.Second, 40*time.Millisecond, "metrics counters should increase")

	d, err := metricsq.Depths(ctx, rdb, queue)
	require.NoError(t, err)
	require.Equal(t, queue, d.Queue)
}

func TestTaskMQ_Inspect_ListScheduledAndActive(t *testing.T) {
	ctx, _, rdb := RequireRedis(t)
	queue := UniqueQueue(t, "inspect_lists")
	FlushQueue(ctx, rdb, queue, "sched-1", "act-1")
	lc := DefaultTestLifecycle()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("inspect-list-g"),
		mqworker.WithConsumer("inspect-list-c"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	)
	pool.Register("slow", func(ctx context.Context, task *taskmodel.Task) error {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	require.NoError(t, pool.Start(ctx))
	defer func() {
		close(release)
		pool.Stop(ctx)
	}()

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))

	// Delayed / scheduled task.
	require.NoError(t, c.EnqueueIn(ctx, taskmodel.NewTask("later", []byte(`{}`), taskmodel.TaskOptions{
		ID: "sched-1", Queue: queue,
	}), 2*time.Hour))

	scheduled, err := c.ListScheduledTasks(ctx, queue, 10)
	require.NoError(t, err)
	require.NotEmpty(t, scheduled)

	// Active: block handler.
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("slow", []byte(`{}`), taskmodel.TaskOptions{
		ID: "act-1", Queue: queue,
	})))
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not start")
	}

	active, err := c.ListActiveTasks(ctx, queue, 10)
	require.NoError(t, err)
	require.NotEmpty(t, active)

	info, err := c.GetTaskInfo(ctx, queue, "act-1")
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, meta.StateActive, info.State)
}
