package integration

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestTaskMQ_MultiQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q1 := "multi_queue_1"
	q2 := "multi_queue_2"

	run1 := make(chan string, 1)
	run2 := make(chan string, 1)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

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
			taskmq.NewClient,
			func() taskmq.Codec { return taskmq.JSONCodec{} }, // Provide Codec explicitly
			taskmq.ProvideWorkers,
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	_ = rdb.Del(ctx, taskmq.StreamKey(q1)).Err()
	_ = rdb.Del(ctx, taskmq.StreamKey(q2)).Err()

	// Assert the returned worker is a MultiQueueWorker
	mqWorker, ok := worker.(taskmq.MultiQueueWorker)
	assert.True(t, ok, "Worker should implement MultiQueueWorker")

	// Register handlers on specific queues
	mqWorker.Queue(q1).Register("task:q1", func(ctx context.Context, task *taskmq.Task) error {
		run1 <- string(task.Payload)
		return nil
	})
	mqWorker.Queue(q2).Register("task:q2", func(ctx context.Context, task *taskmq.Task) error {
		run2 <- string(task.Payload)
		return nil
	})

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue tasks to different queues
	t1 := taskmq.NewTask("task:q1", []byte("payload-1"), taskmq.TaskOptions{Queue: q1})
	err := client.Enqueue(ctx, t1)
	assert.NoError(t, err)

	t2 := taskmq.NewTask("task:q2", []byte("payload-2"), taskmq.TaskOptions{Queue: q2})
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
