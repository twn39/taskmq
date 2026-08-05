package integration

import (
	"context"
	"encoding/json"
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

// 1. Existing MVP flow test
func TestTaskMQ_MVPFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "mvp")
	streamKey := keys.KeysFor(queueName).Stream()

	type WelcomeEmail struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}

	runChan := make(chan *WelcomeEmail, 1)

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
					mqworker.WithGroup("test-group"),
					mqworker.WithConsumer("test-consumer"),
					mqworker.WithConcurrency(2),
				)
				pool.Register("email:welcome", func(ctx context.Context, task *taskmodel.Task) error {
					var email WelcomeEmail
					if err := json.Unmarshal(task.Payload, &email); err != nil {
						return err
					}
					runChan <- &email
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	rdb.Del(ctx, streamKey)
	defer rdb.Del(ctx, streamKey)

	app.RequireStart()
	defer app.RequireStop()

	emailData := WelcomeEmail{
		Email: "test@example.com",
		Name:  "John Doe",
	}
	payloadBytes, err := json.Marshal(emailData)
	assert.NoError(t, err)

	task := taskmodel.NewTask("email:welcome", payloadBytes, taskmodel.TaskOptions{
		Queue: queueName,
	})

	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case result := <-runChan:
		assert.Equal(t, "test@example.com", result.Email)
		assert.Equal(t, "John Doe", result.Name)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task execution")
	}

	time.Sleep(100 * time.Millisecond)

	pending, err := rdb.XPending(ctx, streamKey, "test-group").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

func TestTaskMQ_SyncExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "sync")
	streamKey := keys.KeysFor(queueName).Stream()

	runChan := make(chan string, 1)

	var rdb goredis.UniversalClient
	var client mqclient.Client
	var worker mqworker.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb goredis.UniversalClient) mqclient.Client {
				return mqclient.NewClient(rdb)
			},
			func(rdb goredis.UniversalClient, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(2),
					mqworker.WithSyncExecution(true),
				)
				pool.Register("task:sync-exec-test", func(ctx context.Context, task *taskmodel.Task) error {
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

	task := taskmodel.NewTask("task:sync-exec-test", []byte("sync-exec-payload"), taskmodel.TaskOptions{
		Queue: queueName,
	})

	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case result := <-runChan:
		assert.Equal(t, "sync-exec-payload", result)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task to execute via SyncExecution")
	}
}

func TestTaskMQ_ExecutionPoolPanicRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "panic")
	streamKey := keys.KeysFor(queueName).Stream()
	dlqKey := keys.KeysFor(queueName).DLQ()
	dlqIndexKey := keys.KeysFor(queueName).DLQIndex()

	var rdb goredis.UniversalClient
	var client mqclient.Client
	var worker mqworker.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb goredis.UniversalClient) mqclient.Client {
				return mqclient.NewClient(rdb)
			},
			func(rdb goredis.UniversalClient, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(2),
				)
				pool.Register("task:panic-test", func(ctx context.Context, task *taskmodel.Task) error {
					panic("something went terribly wrong")
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	_ = rdb.Del(ctx, streamKey).Err()
	_ = rdb.Del(ctx, dlqKey).Err()
	_ = rdb.Del(ctx, dlqIndexKey).Err()

	app.RequireStart()
	defer app.RequireStop()

	task := taskmodel.NewTask("task:panic-test", []byte("panic-payload"), taskmodel.TaskOptions{
		Queue:    queueName,
		MaxRetry: taskmodel.Ptr(1), // Fail immediately to DLQ
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for task to fail and end up in DLQ
	time.Sleep(1 * time.Second)

	// Check DLQ
	dlqTasks, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, dlqTasks, 1)
	assert.Contains(t, dlqTasks[0].LastError, "task panicked: something went terribly wrong")
}
