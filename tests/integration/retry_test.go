package integration

import (
	"context"
	"errors"
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
		MaxRetry: 3,
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
