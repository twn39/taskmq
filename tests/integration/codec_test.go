package integration

import (
	"context"
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

func TestTaskMQ_BinaryCodec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "binary_codec_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	runChan := make(chan string, 1)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	binaryCodec := taskmq.BinaryCodec{}

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb *goredis.Client) taskmq.Client {
				return taskmq.NewClient(rdb, taskmq.WithClientCodec(binaryCodec))
			},
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				opts := taskmq.NewDefaultWorkerOptions(rdb, logger, queueName, binaryCodec, taskmq.WorkerOptions{
					Concurrency: 2,
					Codec:       binaryCodec,
				})
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, opts)
				pool.Register("task:binary-test", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	err := rdb.Del(ctx, streamKey).Err()
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	task := taskmq.NewTask("task:binary-test", []byte("binary-payload"), taskmq.TaskOptions{
		Queue: queueName,
	})

	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case result := <-runChan:
		assert.Equal(t, "binary-payload", result)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task to execute via BinaryCodec")
	}
}
