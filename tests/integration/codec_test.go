package integration

import (
	"context"
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
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	internalredis "github.com/twn39/taskmq/internal/redis"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestTaskMQ_BinaryCodec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "binary_codec_test_queue"
	streamKey := keys.KeysFor(queueName).Stream()

	runChan := make(chan string, 1)

	var rdb *goredis.Client
	var client mqclient.Client
	var worker mqworker.Worker

	binaryCodec := codec.BinaryCodec{}

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb *goredis.Client) mqclient.Client {
				return mqclient.NewClient(rdb, mqclient.WithClientCodec(binaryCodec))
			},
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(2),
					mqworker.WithCodec(binaryCodec),
				)
				pool.Register("task:binary-test", func(ctx context.Context, task *taskmodel.Task) error {
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

	task := taskmodel.NewTask("task:binary-test", []byte("binary-payload"), taskmodel.TaskOptions{
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
