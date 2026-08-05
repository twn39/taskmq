package integration

import (
	"context"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"sync"
	"testing"
	"time"
)

func TestTaskMQ_AtomicUniqueEnqueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "atomic_enq")
	streamKey := keys.KeysFor(queueName).Stream()
	uniqueLockKey := keys.KeysFor(queueName).Unique("atomic-key")

	var rdb goredis.UniversalClient
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
		),
		fx.Populate(&rdb, &client),
	)

	app.RequireStart()
	defer app.RequireStop()

	rdb.Del(ctx, streamKey, uniqueLockKey)
	defer rdb.Del(ctx, streamKey, uniqueLockKey)

	// Concurrently enqueue 20 unique tasks. Only one should succeed.
	var wg sync.WaitGroup
	successCount := 0
	duplicateCount := 0
	var mu sync.Mutex

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task := taskmodel.NewTask("task:atomic", []byte("data"), taskmodel.TaskOptions{
				Queue:     queueName,
				UniqueKey: "atomic-key",
				UniqueTTL: 10 * time.Second,
			})
			err := client.Enqueue(ctx, task)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successCount++
			} else if err == lifecycle.ErrDuplicateTask {
				duplicateCount++
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, successCount, "Exactly 1 concurrent enqueue should succeed")
	assert.Equal(t, 19, duplicateCount, "Exactly 19 concurrent enqueues should return duplicate error")

	// Verify that the uniqueness lock exists and holds the task ID of the successfully enqueued task
	lockVal, err := rdb.Get(ctx, uniqueLockKey).Result()
	assert.NoError(t, err)
	assert.NotEmpty(t, lockVal)

	// Verify that Stream has exactly 1 task
	streamLen, err := rdb.XLen(ctx, streamKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), streamLen)
}

func TestTaskMQ_AtomicCompleteAndRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "atomic_complete")
	streamKey := keys.KeysFor(queueName).Stream()
	uniqueLockKey := keys.KeysFor(queueName).Unique("complete-key")

	runChan := make(chan bool, 1)

	var rdb goredis.UniversalClient
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb goredis.UniversalClient, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithGroup("complete-group"),
					mqworker.WithConsumer("complete-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:complete", func(ctx context.Context, task *taskmodel.Task) error {
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

	// Enqueue unique task
	task := taskmodel.NewTask("task:complete", []byte("data"), taskmodel.TaskOptions{
		Queue:     queueName,
		UniqueKey: "complete-key",
		UniqueTTL: 10 * time.Second,
	})
	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for execution
	select {
	case <-runChan:
		t.Log("Task executed successfully")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for execution")
	}

	// Sleep briefly for complete task hook to run
	time.Sleep(150 * time.Millisecond)

	// Verify that lock is deleted from Redis (atomic complete deleted it)
	exists, err := rdb.Exists(ctx, uniqueLockKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists, "Lock key should be atomically deleted upon task completion")

	// Verify that stream is empty (XDEL was called)
	streamLen, err := rdb.XLen(ctx, streamKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), streamLen, "Message should be atomically deleted from stream upon completion")
}
