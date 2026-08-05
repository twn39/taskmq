package integration

import (
	"context"
	"errors"
	"sync/atomic"
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
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	internalredis "github.com/twn39/taskmq/internal/redis"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestTaskMQ_RetryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "retry")
	streamKey := keys.KeysFor(queueName).Stream()
	delayedKey := keys.KeysFor(queueName).Delayed()

	var execCount int64
	doneChan := make(chan bool, 1)

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
					mqworker.WithGroup("retry-group"),
					mqworker.WithConsumer("retry-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:fail", func(ctx context.Context, task *taskmodel.Task) error {
					current := atomic.AddInt64(&execCount, 1)
					if current >= 3 {
						doneChan <- true
					}
					return errors.New("simulated handler failure")
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, delayedKey)
	defer rdb.Del(ctx, streamKey, delayedKey)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue a task that allows 3 retries (MaxRetry = 3)
	task := taskmodel.NewTask("task:fail", []byte("fail payload"), taskmodel.TaskOptions{
		Queue:    queueName,
		MaxRetry: taskmodel.Ptr(3),
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case <-doneChan:
		t.Log("Task failed 3 times and completed retry loop")
		assert.Equal(t, int64(3), atomic.LoadInt64(&execCount))
	case <-ctx.Done():
		t.Fatal("Timeout waiting for retries to complete")
	}

	time.Sleep(100 * time.Millisecond)

	// Stream PEL should be empty now that task exceeded max retries and got XACKed
	pending, err := rdb.XPending(ctx, streamKey, "retry-group").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

func TestTaskMQ_UnregisteredHandlerRetryAndDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "unreg_retry")
	streamKey := keys.KeysFor(queueName).Stream()
	delayedKey := keys.KeysFor(queueName).Delayed()
	dlqKey := keys.KeysFor(queueName).DLQ()

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
					mqworker.WithGroup("unregistered-group"),
					mqworker.WithConsumer("unregistered-consumer"),
					mqworker.WithConcurrency(1),
				)
				// Do NOT register any handlers
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, delayedKey, dlqKey)
	defer rdb.Del(ctx, streamKey, delayedKey, dlqKey)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue a task with MaxRetry = 2
	task := taskmodel.NewTask("task:some_unregistered_job", []byte("data"), taskmodel.TaskOptions{
		Queue:    queueName,
		MaxRetry: taskmodel.Ptr(2),
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Poll DLQ until the task appears (since it should run out of retries and move to DLQ)
	var dlqTasks []*taskmodel.Task
	for i := 0; i < 20; i++ {
		dlqTasks, err = client.ListDeadLetters(ctx, queueName, 10)
		if err == nil && len(dlqTasks) > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	require.Len(t, dlqTasks, 1, "Task should have failed and been moved to DLQ")
	assert.Contains(t, dlqTasks[0].LastError, "no handler registered")
	assert.Equal(t, 1, dlqTasks[0].Retry)

	// Confirm active stream PEL is clean
	pending, err := rdb.XPending(ctx, streamKey, "unregistered-group").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "PEL should be completely clean")
}

func TestTaskMQ_CorruptedPayloadDiscard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "corrupt")
	streamKey := keys.KeysFor(queueName).Stream()

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
					mqworker.WithGroup("corrupted-group"),
					mqworker.WithConsumer("corrupted-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:dummy", func(ctx context.Context, task *taskmodel.Task) error {
					return nil
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
	defer app.RequireStop()

	// Enqueue a corrupted raw message directly to the Redis Stream.
	_, err := rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"task": "{invalid-json}",
		},
	}).Result()
	assert.NoError(t, err)

	// Wait for the worker to process the message and discard it.
	var pending *goredis.XPending
	for i := 0; i < 15; i++ {
		pending, err = rdb.XPending(ctx, streamKey, "corrupted-group").Result()
		if err == nil && pending.Count == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	assert.Equal(t, int64(0), pending.Count, "Corrupted message PEL should be completely clean")
}


