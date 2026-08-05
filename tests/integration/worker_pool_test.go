package integration

import (
	"context"
	"fmt"
	"sync/atomic"
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

func TestTaskMQ_WorkerPool_ParentContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queueName := UniqueQueue(t, "parent_cancel")

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
					mqworker.WithConcurrency(1),
					mqworker.WithContext(ctx),
				)
				pool.Register("task:test", func(ctx context.Context, task *taskmodel.Task) error {
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	rdb.Del(ctx, keys.KeysFor(queueName).Stream())

	app.RequireStart()

	// Cancel the parent context!
	cancel()

	// Wait for the cancellation to propagate and shutdown loops
	time.Sleep(1500 * time.Millisecond)

	// Enqueue a task
	task := taskmodel.NewTask("task:test", []byte("payload"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	err := client.Enqueue(context.Background(), task)
	assert.NoError(t, err)

	// Wait to see if it executes (it should not, since worker is stopped)
	time.Sleep(1 * time.Second)

	// Verify task is still in stream and not acknowledged (meaning it wasn't processed)
	pending, err := rdb.XPending(context.Background(), keys.KeysFor(queueName).Stream(), "taskmq-group").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "Task should not even be read/claimed (pending count should be 0 in group since group didn't pull)")

	app.RequireStop()
}

func TestTaskMQ_WorkerPool_GracefulShutdownDeadline(t *testing.T) {
	queueName := UniqueQueue(t, "grace_shutdown")

	var rdb goredis.UniversalClient
	var client mqclient.Client
	var worker mqworker.Worker

	var taskCancelled int32

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb goredis.UniversalClient, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:long", func(ctx context.Context, task *taskmodel.Task) error {
					fmt.Printf("DEBUG: task:long handler started\n")
					select {
					case <-time.After(5 * time.Second):
						fmt.Printf("DEBUG: task:long completed successfully after 5s\n")
						return nil
					case <-ctx.Done():
						fmt.Printf("DEBUG: task:long ctx.Done() fired: %v\n", ctx.Err())
						atomic.StoreInt32(&taskCancelled, 1)
						return ctx.Err()
					}
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	rdb.Del(context.Background(), keys.KeysFor(queueName).Stream())

	app.RequireStart()

	// Enqueue the long task
	task := taskmodel.NewTask("task:long", []byte("payload"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	err := client.Enqueue(context.Background(), task)
	assert.NoError(t, err)

	// Wait briefly to ensure task starts executing
	time.Sleep(200 * time.Millisecond)

	// Call worker.Stop with a short deadline context (e.g. 1.0 seconds)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer shutdownCancel()

	stopStart := time.Now()
	worker.Stop(shutdownCtx)
	stopDuration := time.Since(stopStart)

	// The stop duration should be around 1.0 second
	assert.LessOrEqual(t, stopDuration.Seconds(), 1.4, "Stop should respect the deadline and exit before Fx hook timeout")
	assert.Equal(t, int32(1), atomic.LoadInt32(&taskCancelled), "Task should have been cancelled by Stop because deadline was reached")

	app.RequireStop()
}
