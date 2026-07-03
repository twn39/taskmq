package integration

import (
	"context"
	"encoding/json"
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

// 1. Existing MVP flow test
func TestTaskMQ_MVPFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "mvp_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	type WelcomeEmail struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}

	runChan := make(chan *WelcomeEmail, 1)

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
					Group:       "test-group",
					Consumer:    "test-consumer",
					Concurrency: 2,
				})
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, opts)
				pool.Register("email:welcome", func(ctx context.Context, task *taskmq.Task) error {
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

	task := taskmq.NewTask("email:welcome", payloadBytes, taskmq.TaskOptions{
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

	queueName := "sync_exec_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	runChan := make(chan string, 1)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb *goredis.Client) taskmq.Client {
				return taskmq.NewClient(rdb)
			},
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				opts := taskmq.NewDefaultWorkerOptions(rdb, logger, queueName, taskmq.JSONCodec{}, taskmq.WorkerOptions{
					Concurrency:   2,
					SyncExecution: true,
				})
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, opts)
				pool.Register("task:sync-exec-test", func(ctx context.Context, task *taskmq.Task) error {
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

	task := taskmq.NewTask("task:sync-exec-test", []byte("sync-exec-payload"), taskmq.TaskOptions{
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

	queueName := "pool_panic_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	dlqKey := taskmq.DLQKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb *goredis.Client) taskmq.Client {
				return taskmq.NewClient(rdb)
			},
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				opts := taskmq.NewDefaultWorkerOptions(rdb, logger, queueName, taskmq.JSONCodec{}, taskmq.WorkerOptions{
					Concurrency: 2,
				})
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, opts)
				pool.Register("task:panic-test", func(ctx context.Context, task *taskmq.Task) error {
					panic("something went terribly wrong")
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	_ = rdb.Del(ctx, streamKey).Err()
	_ = rdb.Del(ctx, dlqKey).Err()

	app.RequireStart()
	defer app.RequireStop()

	task := taskmq.NewTask("task:panic-test", []byte("panic-payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 1, // Fail immediately to DLQ
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for task to fail and end up in DLQ
	time.Sleep(1 * time.Second)

	// Check DLQ
	dlqTasks, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, dlqTasks, 1)
	assert.Contains(t, dlqTasks[0].LastError, "task handler panicked: something went terribly wrong")
}
