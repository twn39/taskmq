package integration

import (
	"context"
	"sync/atomic"
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

func TestTaskMQ_DelayedFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "delayed")
	streamKey := keys.KeysFor(queueName).Stream()
	delayedKey := keys.KeysFor(queueName).Delayed()

	runChan := make(chan time.Time, 1)

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
					mqworker.WithGroup("delayed-group"),
					mqworker.WithConsumer("delayed-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:delayed", func(ctx context.Context, task *taskmodel.Task) error {
					runChan <- time.Now()
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, delayedKey)
	defer rdb.Del(ctx, streamKey, delayedKey)

	enqueueTime := time.Now()

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue with a 2-second delay
	task := taskmodel.NewTask("task:delayed", []byte("delayed data"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	err := client.EnqueueIn(ctx, task, 2*time.Second)
	assert.NoError(t, err)

	select {
	case execTime := <-runChan:
		duration := execTime.Sub(enqueueTime)
		assert.GreaterOrEqual(t, duration.Seconds(), 1.8, "Task should execute after approximately 2 seconds")
		t.Logf("Task executed after %v", duration)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for delayed task execution")
	}
}

func TestTaskMQ_TimeoutCancellationFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "timeout")
	streamKey := keys.KeysFor(queueName).Stream()
	delayedKey := keys.KeysFor(queueName).Delayed()

	var execCount int64

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
					mqworker.WithGroup("timeout-group"),
					mqworker.WithConsumer("timeout-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:slow", func(ctx context.Context, task *taskmodel.Task) error {
					atomic.AddInt64(&execCount, 1)

					// Sleep for 3 seconds, but check if context is cancelled
					select {
					case <-time.After(3 * time.Second):
						return nil
					case <-ctx.Done():
						// Context cancelled (timed out)
						return ctx.Err()
					}
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

	// Enqueue a task with TimeoutMs = 500 (0.5 second), MaxRetry = 2
	task := taskmodel.NewTask("task:slow", []byte("slow payload"), taskmodel.TaskOptions{
		Queue:    queueName,
		MaxRetry: taskmodel.Ptr(2),
		Timeout:  500 * time.Millisecond,
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for the task to time out and start its second execution (execCount >= 2)
	timeoutTicker := time.NewTicker(100 * time.Millisecond)
	defer timeoutTicker.Stop()

	for atomic.LoadInt64(&execCount) < 2 {
		select {
		case <-timeoutTicker.C:
		case <-ctx.Done():
			t.Fatal("Timeout waiting for task timeout and retry execution")
		}
	}
	t.Log("Task successfully timed out on first run and triggered retry")
}

func TestTaskMQ_DelayedSchedulerWakeup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "wakeup")
	streamKey := keys.KeysFor(queueName).Stream()
	delayedKey := keys.KeysFor(queueName).Delayed()

	runChan := make(chan time.Time, 1)

	var rdb goredis.UniversalClient
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb goredis.UniversalClient, logger *zap.Logger) mqworker.Worker {
				// We configure the scheduler with a very long poll interval (e.g. 5 seconds)
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithGroup("wakeup-group"),
					mqworker.WithConsumer("wakeup-consumer"),
					mqworker.WithConcurrency(1),
					mqworker.WithSchedulerPollInterval(5*time.Second),
				)
				pool.Register("task:wakeup", func(ctx context.Context, task *taskmodel.Task) error {
					runChan <- time.Now()
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, delayedKey)
	defer rdb.Del(ctx, streamKey, delayedKey)

	enqueueTime := time.Now()

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue with a 1-second delay
	task := taskmodel.NewTask("task:wakeup", []byte("wakeup data"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	err := client.EnqueueIn(ctx, task, 1*time.Second)
	assert.NoError(t, err)

	select {
	case execTime := <-runChan:
		duration := execTime.Sub(enqueueTime)
		// If it takes significantly less than 5 seconds, it means the wakeup channel successfully preempted the 5s timer!
		assert.Less(t, duration.Seconds(), 2.5, "Task should execute in under 2.5 seconds, preempting the 5-second poll interval")
		t.Logf("Task executed after %v (5s poll interval successfully preempted)", duration)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for delayed task execution")
	}
}
