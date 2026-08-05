package integration

import (
	"context"
	"testing"
	"time"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/logger"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	internalredis "github.com/twn39/taskmq/internal/redis"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestTaskMQ_MultiQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q1 := UniqueQueue(t, "mq1")
	q2 := UniqueQueue(t, "mq2")

	run1 := make(chan string, 1)
	run2 := make(chan string, 1)

	var rdb goredis.UniversalClient
	var client mqclient.Client
	var worker mqworker.Worker

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				cfg := NewTestConfig()
				cfg.TaskMQ.Queues = []config.QueueConfig{
					{Name: q1, Concurrency: 1},
					{Name: q2, Concurrency: 1},
				}
				return cfg
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			ProvideSharedLifecycle,
			ProvideClientWithLifecycle,
			func() codec.Codec { return codec.JSONCodec{} }, // Provide Codec explicitly
			taskmq.ProvideWorkers,
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	_ = rdb.Del(ctx, keys.KeysFor(q1).Stream()).Err()
	_ = rdb.Del(ctx, keys.KeysFor(q2).Stream()).Err()

	// Assert the returned worker is a MultiQueueWorker
	mqWorker, ok := worker.(mqworker.MultiQueueWorker)
	assert.True(t, ok, "Worker should implement MultiQueueWorker")

	// Register handlers on specific queues
	mqWorker.Queue(q1).Register("task:q1", func(ctx context.Context, task *taskmodel.Task) error {
		run1 <- string(task.Payload)
		return nil
	})
	mqWorker.Queue(q2).Register("task:q2", func(ctx context.Context, task *taskmodel.Task) error {
		run2 <- string(task.Payload)
		return nil
	})

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue tasks to different queues
	t1 := taskmodel.NewTask("task:q1", []byte("payload-1"), taskmodel.TaskOptions{Queue: q1})
	err := client.Enqueue(ctx, t1)
	assert.NoError(t, err)

	t2 := taskmodel.NewTask("task:q2", []byte("payload-2"), taskmodel.TaskOptions{Queue: q2})
	err = client.Enqueue(ctx, t2)
	assert.NoError(t, err)

	// Verify execution
	select {
	case p1 := <-run1:
		assert.Equal(t, "payload-1", p1)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task on queue 1")
	}

	select {
	case p2 := <-run2:
		assert.Equal(t, "payload-2", p2)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task on queue 2")
	}
}
