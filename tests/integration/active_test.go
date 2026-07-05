package integration

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

func TestTaskMQ_ActiveInspector_ListTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "active_list_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
		),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey)
	defer rdb.Del(ctx, streamKey)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Verify empty queue list
	tasks, err := client.ListActiveTasks(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Empty(t, tasks)

	// 2. Enqueue task (will be Pending since no worker is running)
	task1 := taskmq.NewTask("task:active_test_1", []byte("payload_1"), taskmq.TaskOptions{
		Queue: queueName,
	})
	err = client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	tasks, err = client.ListActiveTasks(ctx, queueName, 10)
	assert.NoError(t, err)
	require.Len(t, tasks, 1)

	active := tasks[0]
	assert.Equal(t, task1.ID, active.ID)
	assert.Equal(t, "task:active_test_1", active.Name)
	assert.Equal(t, "payload_1", string(active.Payload))
	assert.Equal(t, "Pending", active.Status)
	assert.Empty(t, active.Consumer)
	assert.False(t, active.EnqueuedAt.IsZero())
	assert.True(t, active.EnqueuedAt.Before(time.Now().Add(5*time.Second)))
}

func TestTaskMQ_ActiveInspector_DeletePending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "active_delete_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
		),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey)
	defer rdb.Del(ctx, streamKey)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Enqueue unique task
	task := taskmq.NewTask("task:active_del_pending", []byte("data"), taskmq.TaskOptions{
		Queue:     queueName,
		UniqueKey: "unique_lock_active_inspector",
	})
	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Verify duplicate task fails to enqueue
	task2 := taskmq.NewTask("task:active_del_pending", []byte("data"), taskmq.TaskOptions{
		Queue:     queueName,
		UniqueKey: "unique_lock_active_inspector",
	})
	err = client.Enqueue(ctx, task2)
	assert.ErrorIs(t, err, taskmq.ErrDuplicateTask)

	tasks, err := client.ListActiveTasks(ctx, queueName, 10)
	assert.NoError(t, err)
	require.Len(t, tasks, 1)

	// 2. Delete/cancel from active inspector
	err = client.DeleteActiveTask(ctx, queueName, tasks[0].StreamID)
	assert.NoError(t, err)

	// Verify task is removed
	tasks2, err := client.ListActiveTasks(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Empty(t, tasks2)

	// Verify unique lock is released (we can enqueue again)
	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)
}

func TestTaskMQ_ActiveInspector_DeleteProcessingAndCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	queueName := "active_del_proc_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	startedChan := make(chan string, 1)
	resultChan := make(chan error, 1)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithGroup("active-inspect-group"),
					taskmq.WithConsumer("active-inspect-consumer"),
					taskmq.WithConcurrency(1),
				)
				pool.Register("task:proc_cancel", func(ctx context.Context, task *taskmq.Task) error {
					startedChan <- task.ID
					select {
					case <-ctx.Done():
						resultChan <- ctx.Err()
						return ctx.Err()
					case <-time.After(5 * time.Second):
						resultChan <- nil
						return nil
					}
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey)
	defer rdb.Del(ctx, streamKey)

	app.RequireStart()

	// 1. Enqueue task
	taskID := "test-active-cancel-id"
	task := taskmq.NewTask("task:proc_cancel", []byte("payload"), taskmq.TaskOptions{
		Queue: queueName,
	})
	task.ID = taskID
	err := client.Enqueue(ctx, task)
	require.NoError(t, err)

	// 2. Wait for it to start processing
	select {
	case runningID := <-startedChan:
		assert.Equal(t, taskID, runningID)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for task to start processing")
	}

	// 3. Peek tasks, verify it's "Processing"
	tasks, err := client.ListActiveTasks(ctx, queueName, 10)
	assert.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "Processing", tasks[0].Status)
	assert.Contains(t, tasks[0].Consumer, "active-inspect-consumer")
	assert.GreaterOrEqual(t, tasks[0].Deliveries, int64(1))

	// 4. Delete the active task (which should trigger cancel context and XAck + XDel)
	err = client.DeleteActiveTask(ctx, queueName, tasks[0].StreamID)
	assert.NoError(t, err)

	// 5. Verify the worker context was cancelled
	select {
	case errRes := <-resultChan:
		assert.ErrorIs(t, errRes, context.Canceled, "Expected worker context to be cancelled")
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for worker to handle cancel context")
	}

	// Stop app
	app.RequireStop()

	// 6. Verify PEL is empty (no zombie references left)
	pends, err := rdb.XPending(ctx, streamKey, "active-inspect-group").Result()
	if err == nil && pends != nil {
		assert.Equal(t, int64(0), pends.Count, "PEL should be completely clean")
	}
}
