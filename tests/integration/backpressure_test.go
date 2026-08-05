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

func TestTaskMQ_BackpressureFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "backpressure")
	streamKey := keys.KeysFor(queueName).Stream()

	var runCount int64
	var retryCount int64
	doneChan := make(chan bool, 2)

	var rdb goredis.UniversalClient
	var client mqclient.Client
	var worker mqworker.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb goredis.UniversalClient, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(1),
					mqworker.WithExecutionPoolSize(1),
				)
				pool.Register("task:slow", func(ctx context.Context, task *taskmodel.Task) error {
					atomic.AddInt64(&runCount, 1)
					if task.Retry > 0 {
						atomic.AddInt64(&retryCount, 1)
					}

					// Sleep duration determined by payload
					sleepMs := 100
					if string(task.Payload) == "payload 1" {
						sleepMs = 2000
					}
					time.Sleep(time.Duration(sleepMs) * time.Millisecond)
					doneChan <- true
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx, streamKey).Err()
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue Task 1 and Task 2.
	// Task 1 has 5 seconds timeout (runs for 2 seconds).
	// Task 2 has 1 second timeout (runs for 0.1 second).
	// Because of backpressure, Task 2 should NOT be pulled into memory while Task 1 is executing.
	// Therefore, Task 2 will NOT time out in the queue, and both will finish successfully!
	task1 := taskmodel.NewTask("task:slow", []byte("payload 1"), taskmodel.TaskOptions{
		Queue:    queueName,
		Timeout:  5 * time.Second,
		MaxRetry: taskmodel.Ptr(1),
	})
	task2 := taskmodel.NewTask("task:slow", []byte("payload 2"), taskmodel.TaskOptions{
		Queue:    queueName,
		Timeout:  1 * time.Second,
		MaxRetry: taskmodel.Ptr(1),
	})

	err = client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	// Wait briefly to make sure Task 1 starts executing
	time.Sleep(100 * time.Millisecond)

	err = client.Enqueue(ctx, task2)
	assert.NoError(t, err)

	// Wait for both tasks to execute successfully
	for i := 0; i < 2; i++ {
		select {
		case <-doneChan:
			// One task finished successfully
		case <-ctx.Done():
			t.Fatal("Timeout waiting for tasks to execute under backpressure")
		}
	}

	assert.Equal(t, int64(2), atomic.LoadInt64(&runCount), "Both tasks should run successfully")
	assert.Equal(t, int64(0), atomic.LoadInt64(&retryCount), "No task should have failed/retried due to queue timeout")
}
