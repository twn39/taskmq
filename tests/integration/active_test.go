package integration

import (
	"context"
	"testing"
	"time"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"github.com/twn39/taskmq/internal/logger"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	internalredis "github.com/twn39/taskmq/internal/redis"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestTaskMQ_ActiveInspector_ListTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "active_list")
	streamKey := keys.KeysFor(queueName).Stream()

	var rdb goredis.UniversalClient
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
		),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, keys.KeysFor(queueName).Paused())
	defer rdb.Del(ctx, streamKey, keys.KeysFor(queueName).Paused())

	app.RequireStart()
	defer app.RequireStop()

	// 1. Verify empty queue list
	tasks, err := client.ListActiveTasks(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Empty(t, tasks)

	// 2. Enqueue task (will be Pending since no worker is running)
	task1 := taskmodel.NewTask("task:active_test_1", []byte("payload_1"), taskmodel.TaskOptions{
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

	queueName := UniqueQueue(t, "active_del")
	streamKey := keys.KeysFor(queueName).Stream()

	var rdb goredis.UniversalClient
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
		),
		fx.Populate(&rdb, &client),
	)

	uniqueKey := keys.KeysFor(queueName).Unique("unique_lock_active_inspector")
	rdb.Del(ctx, streamKey, uniqueKey, keys.KeysFor(queueName).Paused())
	defer rdb.Del(ctx, streamKey, uniqueKey, keys.KeysFor(queueName).Paused())

	app.RequireStart()
	defer app.RequireStop()

	// 1. Enqueue unique task
	task := taskmodel.NewTask("task:active_del_pending", []byte("data"), taskmodel.TaskOptions{
		Queue:     queueName,
		UniqueKey: "unique_lock_active_inspector",
	})
	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Verify duplicate task fails to enqueue
	task2 := taskmodel.NewTask("task:active_del_pending", []byte("data"), taskmodel.TaskOptions{
		Queue:     queueName,
		UniqueKey: "unique_lock_active_inspector",
	})
	err = client.Enqueue(ctx, task2)
	assert.ErrorIs(t, err, lifecycle.ErrDuplicateTask)

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

	queueName := UniqueQueue(t, "active_del_proc")
	streamKey := keys.KeysFor(queueName).Stream()

	startedChan := make(chan string, 1)
	resultChan := make(chan error, 1)

	var rdb goredis.UniversalClient
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb goredis.UniversalClient, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithGroup("active-inspect-group"),
					mqworker.WithConsumer("active-inspect-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:proc_cancel", func(ctx context.Context, task *taskmodel.Task) error {
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

	cancelKey := keys.KeysFor(queueName).Cancelled("test-active-cancel-id")
	rdb.Del(ctx, streamKey, keys.KeysFor(queueName).Paused(), cancelKey)
	defer rdb.Del(ctx, streamKey, keys.KeysFor(queueName).Paused(), cancelKey)

	app.RequireStart()

	// 1. Enqueue task
	taskID := "test-active-cancel-id"
	task := taskmodel.NewTask("task:proc_cancel", []byte("payload"), taskmodel.TaskOptions{
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
