package integration

import (
	"context"
	"sync"
	"testing"
	"time"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"github.com/twn39/taskmq/internal/logger"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	internalredis "github.com/twn39/taskmq/internal/redis"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestTaskMQ_TaskCancellationFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "cancel_test_queue"
	streamKey := keys.StreamKey(queueName)
	taskID := "test-running-cancel-id"

	startedChan := make(chan string, 1)
	resultChan := make(chan error, 1)

	var rdb *goredis.Client
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithGroup("cancel-group"),
					mqworker.WithConsumer("cancel-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:cancel_running", func(ctx context.Context, task *taskmodel.Task) error {
					startedChan <- task.ID
					// Wait for cancellation signal up to 3 seconds
					select {
					case <-ctx.Done():
						resultChan <- ctx.Err()
						return ctx.Err()
					case <-time.After(3 * time.Second):
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

	cancelKey := "taskmq:{" + queueName + "}:cancelled:" + taskID
	rdb.Del(ctx, streamKey, cancelKey)
	defer rdb.Del(ctx, streamKey, cancelKey)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Enqueue task
	task := taskmodel.NewTask("task:cancel_running", []byte("data"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	task.ID = taskID
	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// 2. Wait for it to start
	var runningTaskID string
	select {
	case runningTaskID = <-startedChan:
		assert.Equal(t, taskID, runningTaskID)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task execution to start")
	}

	// 3. Cancel the task while running
	err = client.CancelTask(ctx, queueName, taskID)
	assert.NoError(t, err)

	// 4. Verify context is cancelled and handler returns context.Canceled
	select {
	case resErr := <-resultChan:
		assert.ErrorIs(t, resErr, context.Canceled, "Expected task execution to be cancelled")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task to handle cancel signal")
	}
}

func TestTaskMQ_TaskCancellationBeforeRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "cancel_before_run_queue"
	streamKey := keys.StreamKey(queueName)
	taskID := "test-before-cancel-id"

	var mu sync.Mutex
	handlerInvoked := false

	var rdb *goredis.Client
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithGroup("cancel-before-run-group"),
					mqworker.WithConsumer("cancel-before-run-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:cancel_before", func(ctx context.Context, task *taskmodel.Task) error {
					mu.Lock()
					handlerInvoked = true
					mu.Unlock()
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	cancelKey := "taskmq:{" + queueName + "}:cancelled:" + taskID
	rdb.Del(ctx, streamKey, cancelKey)
	defer rdb.Del(ctx, streamKey, cancelKey)

	// Pre-create stream and consumer group with "0" so that the worker can consume backlog messages on a fresh Redis database
	_ = rdb.XGroupCreateMkStream(ctx, streamKey, "cancel-before-run-group", "0").Err()

	// 1. Enqueue task before starting worker
	task := taskmodel.NewTask("task:cancel_before", []byte("data"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	task.ID = taskID
	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// 2. Cancel it immediately before worker starts
	err = client.CancelTask(ctx, queueName, taskID)
	assert.NoError(t, err)

	// 3. Start worker pool
	app.RequireStart()
	defer app.RequireStop()

	// Wait 1 second to let worker pull message and run pre-execution check
	time.Sleep(1 * time.Second)

	// 4. Verify handler was never invoked
	mu.Lock()
	invoked := handlerInvoked
	mu.Unlock()
	assert.False(t, invoked, "Handler should not be invoked for a pre-cancelled task")

	// Verify that the task was completed and deleted from the stream
	streamLen, err := rdb.XLen(ctx, streamKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), streamLen, "Pre-cancelled task should be completed and deleted from stream")
}
