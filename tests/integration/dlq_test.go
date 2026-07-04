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

func TestTaskMQ_DLQFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "dlq_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	dlqKey := taskmq.DLQKey(queueName)

	runChan := make(chan error, 2)
	var attempt int64

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
					taskmq.WithGroup("dlq-group"),
					taskmq.WithConsumer("dlq-consumer"),
					taskmq.WithConcurrency(1),
				)
				pool.Register("task:fail", func(ctx context.Context, task *taskmq.Task) error {
					att := atomic.AddInt64(&attempt, 1)
					if att == 1 {
						err := errors.New("simulated handler error")
						runChan <- err
						return err
					}
					// Second run succeeds!
					runChan <- nil
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, dlqKey)
	defer rdb.Del(ctx, streamKey, dlqKey)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue a task with MaxRetry = 1 (runs once, fails, immediately archived to DLQ)
	task := taskmq.NewTask("task:fail", []byte("fail payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 1,
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for first handler run to fail
	select {
	case err := <-runChan:
		assert.ErrorContains(t, err, "simulated handler error")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task execution failure")
	}

	// Wait briefly to allow archiving to DLQ to complete
	time.Sleep(100 * time.Millisecond)

	// 1. List dead letters and verify
	deadTasks, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasks, 1)
	assert.Equal(t, task.ID, deadTasks[0].ID)
	assert.Equal(t, "simulated handler error", deadTasks[0].LastError)

	// 2. Retry dead letter
	err = client.RetryDeadLetter(ctx, queueName, task.ID)
	assert.NoError(t, err)

	// Verify it was removed from DLQ ZSET index immediately by RetryDeadLetter
	deadTasksAfter, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasksAfter, 0)

	// Wait for the retried task to execute successfully
	select {
	case err := <-runChan:
		assert.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for retried task success execution")
	}

	// Wait briefly
	time.Sleep(100 * time.Millisecond)

	// Verify DLQ remains empty on success
	deadTasksAfterSuccess, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasksAfterSuccess, 0)

	// 3. Test DeleteDeadLetter
	// Reset attempt to 1 so the next enqueued task fails again
	atomic.StoreInt64(&attempt, 0)

	taskToDelete := taskmq.NewTask("task:fail", []byte("delete payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 1,
	})

	err = client.Enqueue(ctx, taskToDelete)
	assert.NoError(t, err)

	select {
	case err := <-runChan:
		assert.ErrorContains(t, err, "simulated handler error")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for second task failure")
	}

	time.Sleep(100 * time.Millisecond)

	// Verify it is in DLQ
	deadTasksBeforeDelete, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasksBeforeDelete, 1)

	// Delete it
	err = client.DeleteDeadLetter(ctx, queueName, taskToDelete.ID)
	assert.NoError(t, err)

	// Verify DLQ is empty again
	deadTasksAfterDelete, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasksAfterDelete, 0)
}
