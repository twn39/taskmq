package integration

import (
	"context"
	"errors"
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

func TestTaskMQ_UniqueScope_UntilStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "unq_start")
	streamKey := keys.KeysFor(queueName).Stream()
	uniqueLockKey := keys.KeysFor(queueName).Unique("start-key")

	startedChan := make(chan bool, 1)
	handlerSleepChan := make(chan bool, 1)

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
					mqworker.WithGroup("start-group"),
					mqworker.WithConsumer("start-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:unq_start", func(ctx context.Context, task *taskmodel.Task) error {
					startedChan <- true
					<-handlerSleepChan // Keep handler running
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, uniqueLockKey)
	defer rdb.Del(ctx, streamKey, uniqueLockKey)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Enqueue task with UniqueUntilStart
	task1 := taskmodel.NewTask("task:unq_start", []byte("1"), taskmodel.TaskOptions{
		Queue:       queueName,
		UniqueKey:   "start-key",
		UniqueTTL:   5 * time.Second,
		UniqueScope: taskmodel.UniqueUntilStart,
	})
	err := client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	// Wait for execution to start
	select {
	case <-startedChan:
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task 1 to start")
	}

	// 2. Try to enqueue task 2 with the same key while task 1 is still running.
	// It should succeed because UniqueUntilStart releases the lock immediately when handler starts!
	task2 := taskmodel.NewTask("task:unq_start", []byte("2"), taskmodel.TaskOptions{
		Queue:       queueName,
		UniqueKey:   "start-key",
		UniqueTTL:   5 * time.Second,
		UniqueScope: taskmodel.UniqueUntilStart,
	})
	err = client.Enqueue(ctx, task2)
	assert.NoError(t, err, "Should allow enqueuing duplicates once task 1 starts executing under UniqueUntilStart")

	// Release handler sleep
	handlerSleepChan <- true
}

func TestTaskMQ_UniqueScope_UntilSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "unq_success")
	streamKey := keys.KeysFor(queueName).Stream()
	dlqKey := keys.KeysFor(queueName).DLQ()
	dlqIndexKey := keys.KeysFor(queueName).DLQIndex()
	uniqueLockKey := keys.KeysFor(queueName).Unique("success-key")

	runChan := make(chan error, 5)

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
					mqworker.WithGroup("success-group"),
					mqworker.WithConsumer("success-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:unq_success", func(ctx context.Context, task *taskmodel.Task) error {
					if string(task.Payload) == "fail" {
						runChan <- errors.New("fail")
						return errors.New("fail")
					}
					runChan <- nil
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, uniqueLockKey, dlqKey, dlqIndexKey)
	defer rdb.Del(ctx, streamKey, uniqueLockKey, dlqKey, dlqIndexKey)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Enqueue unique task that fails, with UniqueUntilSuccess.
	// Set MaxRetry: taskmodel.Ptr(0) directly in TaskOptions so task1 goes directly to DLQ on failure with no retries.
	task1 := taskmodel.NewTask("task:unq_success", []byte("fail"), taskmodel.TaskOptions{
		Queue:       queueName,
		MaxRetry:    taskmodel.Ptr(0),
		UniqueKey:   "success-key",
		UniqueTTL:   10 * time.Second,
		UniqueScope: taskmodel.UniqueUntilSuccess,
	})
	err := client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	// Wait for handler to execute and fail
	select {
	case hErr := <-runChan:
		assert.Error(t, hErr)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task 1 to execute")
	}

	// Wait for middleware failure cleanup: MoveToDLQ + ReleaseUniqueLock must finish.
	time.Sleep(500 * time.Millisecond)

	// 2. Try to enqueue task 2 with the same key while task 1 is scheduled for retry.
	// It should succeed because UniqueUntilSuccess releases the lock immediately on execution failure!
	task2 := taskmodel.NewTask("task:unq_success", []byte("success"), taskmodel.TaskOptions{
		Queue:       queueName,
		UniqueKey:   "success-key",
		UniqueTTL:   10 * time.Second,
		UniqueScope: taskmodel.UniqueUntilSuccess,
	})
	err = client.Enqueue(ctx, task2)
	assert.NoError(t, err, "Should allow enqueuing duplicate once task 1 failed and lock released under UniqueUntilSuccess")

	// Wait for task 2 to execute successfully
	select {
	case hErr := <-runChan:
		assert.NoError(t, hErr)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task 2 to execute")
	}
}

func TestTaskMQ_Unique_WatchdogRenewal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "unq_watch")
	streamKey := keys.KeysFor(queueName).Stream()
	uniqueLockKey := keys.KeysFor(queueName).Unique("watchdog-key")

	startedChan := make(chan bool, 1)
	handlerSleepChan := make(chan bool, 1)

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
					mqworker.WithGroup("watchdog-group"),
					mqworker.WithConsumer("watchdog-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:unq_watchdog", func(ctx context.Context, task *taskmodel.Task) error {
					startedChan <- true
					<-handlerSleepChan // Keep handler running to test watchdog
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, uniqueLockKey)
	defer rdb.Del(ctx, streamKey, uniqueLockKey)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Enqueue unique task with a very short TTL (1.5 seconds)
	task1 := taskmodel.NewTask("task:unq_watchdog", []byte("watchdog"), taskmodel.TaskOptions{
		Queue:       queueName,
		UniqueKey:   "watchdog-key",
		UniqueTTL:   1500 * time.Millisecond,
		UniqueScope: taskmodel.UniqueUntilSucceeded,
	})
	err := client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	// Wait for execution to start
	select {
	case <-startedChan:
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task to start")
	}

	// 2. Wait 2.2 seconds (which exceeds the initial 1.5-second TTL).
	// If the watchdog is working, it should have renewed the TTL, so the lock key should still exist!
	time.Sleep(2200 * time.Millisecond)

	exists, err := rdb.Exists(ctx, uniqueLockKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists, "Unique lock key should still exist due to watchdog renewal")

	// 3. Release the handler
	handlerSleepChan <- true

	// Wait briefly for completion lock release
	time.Sleep(200 * time.Millisecond)

	// 4. Verify lock is released cleanly on final success completion
	exists, err = rdb.Exists(ctx, uniqueLockKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists, "Unique lock key should have been released cleanly on task success")
}
