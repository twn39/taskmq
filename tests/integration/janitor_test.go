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

func TestTaskMQ_JanitorRecoveryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueName := "janitor_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	var runCount int64
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
					taskmq.WithGroup("janitor-group"),
					taskmq.WithConsumer("janitor-consumer"),
					taskmq.WithConcurrency(1),
				)
				pool.Register("task:crash", func(ctx context.Context, task *taskmq.Task) error {
					count := atomic.AddInt64(&runCount, 1)
					if count == 1 {
						t.Log("Simulating crashed worker on first execution (handler sleeping...)")
						time.Sleep(10 * time.Second)
						return nil
					}

					t.Log("Janitor successfully recovered and executed task on second run!")
					doneChan <- true
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

	task := taskmq.NewTask("task:crash", []byte("crash payload"), taskmq.TaskOptions{
		Queue: queueName,
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case <-doneChan:
		assert.Equal(t, int64(2), atomic.LoadInt64(&runCount), "Task should have run twice (once stalled, once recovered by janitor)")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for Janitor recovery")
	}
}
