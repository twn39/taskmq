package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

func TestTaskMQ_BackpressureFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "backpressure_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	var runCount int64
	var retryCount int64
	doneChan := make(chan bool, 2)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				opts := taskmq.NewDefaultWorkerOptions(rdb, logger, queueName, taskmq.JSONCodec{}, taskmq.WorkerOptions{
					Concurrency:       1,
					ExecutionPoolSize: 1, // Only 1 concurrent task execution allowed
				})
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, opts)
				pool.Register("task:slow", func(ctx context.Context, task *taskmq.Task) error {
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
	task1 := taskmq.NewTask("task:slow", []byte("payload 1"), taskmq.TaskOptions{
		Queue:    queueName,
		Timeout:  5 * time.Second,
		MaxRetry: 1,
	})
	task2 := taskmq.NewTask("task:slow", []byte("payload 2"), taskmq.TaskOptions{
		Queue:    queueName,
		Timeout:  1 * time.Second,
		MaxRetry: 1,
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
