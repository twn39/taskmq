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

func TestTaskMQ_UniquenessFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "unique_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	uniqueLockKey := taskmq.UniqueKey(queueName, "my-unique-key")

	runChan := make(chan bool, 1)

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
					taskmq.WithGroup("unique-group"),
					taskmq.WithConsumer("unique-consumer"),
					taskmq.WithConcurrency(1),
				)
				pool.Register("task:unique", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- true
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

	// 1. Enqueue unique task 1
	task1 := taskmq.NewTask("task:unique", []byte("data 1"), taskmq.TaskOptions{
		Queue:     queueName,
		UniqueKey: "my-unique-key",
		UniqueTTL: 5 * time.Second,
	})
	err := client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	// 2. Try enqueuing unique task 2 (with same unique key)
	task2 := taskmq.NewTask("task:unique", []byte("data 2"), taskmq.TaskOptions{
		Queue:     queueName,
		UniqueKey: "my-unique-key",
		UniqueTTL: 5 * time.Second,
	})
	err = client.Enqueue(ctx, task2)
	assert.ErrorIs(t, err, taskmq.ErrDuplicateTask, "Should return ErrDuplicateTask on duplicates")

	// 3. Wait for task 1 to run successfully
	select {
	case <-runChan:
		t.Log("Task 1 executed successfully")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task 1 execution")
	}

	// Wait briefly to allow unlocking to finish in worker hook
	time.Sleep(100 * time.Millisecond)

	// 4. Try enqueuing task 2 again (should succeed now that task 1 completed and lock was released)
	err = client.Enqueue(ctx, task2)
	assert.NoError(t, err, "Should allow enqueuing after lock has been released")

	select {
	case <-runChan:
		t.Log("Task 2 executed successfully after lock release")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task 2 execution")
	}
}
