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
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

func TestTaskMQ_RetryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "retry_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	delayedKey := taskmq.DelayedKey(queueName)

	var execCount int64
	doneChan := make(chan bool, 1)

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
					taskmq.WithGroup("retry-group"),
					taskmq.WithConsumer("retry-consumer"),
					taskmq.WithConcurrency(1),
				)
				pool.Register("task:fail", func(ctx context.Context, task *taskmq.Task) error {
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
	task := taskmq.NewTask("task:fail", []byte("fail payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: taskmq.Ptr(3),
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

	queueName := "unregistered_retry_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	delayedKey := taskmq.DelayedKey(queueName)
	dlqKey := taskmq.DLQKey(queueName)

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
					taskmq.WithGroup("unregistered-group"),
					taskmq.WithConsumer("unregistered-consumer"),
					taskmq.WithConcurrency(1),
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
	task := taskmq.NewTask("task:some_unregistered_job", []byte("data"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: taskmq.Ptr(2),
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Poll DLQ until the task appears (since it should run out of retries and move to DLQ)
	var dlqTasks []*taskmq.Task
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

	queueName := "corrupted_payload_test_queue"
	streamKey := taskmq.StreamKey(queueName)

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
					taskmq.WithGroup("corrupted-group"),
					taskmq.WithConsumer("corrupted-consumer"),
					taskmq.WithConcurrency(1),
				)
				pool.Register("task:dummy", func(ctx context.Context, task *taskmq.Task) error {
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


