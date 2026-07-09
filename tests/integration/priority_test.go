package integration

import (
	"context"
	"fmt"
	"sync"
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

func TestTaskMQ_StrictPriorityFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	qLow := "low_priority_queue"
	qCritical := "critical_priority_queue"

	var executionOrder []string
	var mu sync.Mutex
	doneChan := make(chan struct{})

	var rdb *goredis.Client
	var client mqclient.Client
	var worker mqworker.Worker

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				cfg := NewTestConfig()
				cfg.TaskMQ.PriorityQueuesEnabled = true
				cfg.TaskMQ.PriorityStrategy = "strict"
				cfg.TaskMQ.Queues = []config.QueueConfig{
					{Name: qLow, Concurrency: 1, Priority: 1},
					{Name: qCritical, Concurrency: 1, Priority: 9},
				}
				return cfg
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func() codec.Codec { return codec.JSONCodec{} },
			taskmq.ProvideWorkers,
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	errDel := rdb.Del(ctx, keys.StreamKey(qLow), keys.StreamKey(qCritical)).Err()
	t.Logf("Del err: %v", errDel)
	defer rdb.Del(ctx, keys.StreamKey(qLow), keys.StreamKey(qCritical))

	// Pre-create consumer groups with "0" cursor so we can read pre-existing messages
	errGroupLow := rdb.XGroupCreateMkStream(ctx, keys.StreamKey(qLow), "taskmq-priority-group", "0").Err()
	errGroupCrit := rdb.XGroupCreateMkStream(ctx, keys.StreamKey(qCritical), "taskmq-priority-group", "0").Err()
	t.Logf("XGroupCreateMkStream qLow err: %v, qCritical err: %v", errGroupLow, errGroupCrit)

	// Register handlers
	mqWorker, ok := worker.(mqworker.MultiQueueWorker)
	assert.True(t, ok)

	handler := func(queueName string) mqworker.HandlerFunc {
		return func(ctx context.Context, task *taskmodel.Task) error {
			t.Logf("HANDLER RUNNING: queueName=%s, taskName=%s, taskID=%s", queueName, task.Name, task.ID)
			mu.Lock()
			executionOrder = append(executionOrder, queueName)
			orderLen := len(executionOrder)
			mu.Unlock()

			if orderLen >= 10 {
				close(doneChan)
			}
			return nil
		}
	}

	mqWorker.Queue(qLow).Register("task:test:low", handler("low"))
	mqWorker.Queue(qCritical).Register("task:test:critical", handler("critical"))

	// Enqueue 5 low priority tasks first, then 5 critical priority tasks
	for i := 0; i < 5; i++ {
		tLow := taskmodel.NewTask("task:test:low", []byte(fmt.Sprintf("low-%d", i)), taskmodel.TaskOptions{Queue: qLow})
		err := client.Enqueue(ctx, tLow)
		assert.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		tCritical := taskmodel.NewTask("task:test:critical", []byte(fmt.Sprintf("critical-%d", i)), taskmodel.TaskOptions{Queue: qCritical})
		err := client.Enqueue(ctx, tCritical)
		assert.NoError(t, err)
	}

	lenLow, errLow := rdb.XLen(ctx, keys.StreamKey(qLow)).Result()
	lenCrit, errCrit := rdb.XLen(ctx, keys.StreamKey(qCritical)).Result()
	t.Logf("XLEN before start: low=%d (err: %v), critical=%d (err: %v)", lenLow, errLow, lenCrit, errCrit)

	// Now start the worker pool
	app.RequireStart()
	defer app.RequireStop()

	// Wait for all 10 tasks to be executed
	select {
	case <-doneChan:
		// Verify strict priority execution order:
		// Since concurrency is 1, strict priority must consume all critical tasks before any low tasks.
		mu.Lock()
		defer mu.Unlock()
		assert.Len(t, executionOrder, 10)
		for i := 0; i < 5; i++ {
			assert.Equal(t, "critical", executionOrder[i], fmt.Sprintf("Task at index %d should be critical", i))
		}
		for i := 5; i < 10; i++ {
			assert.Equal(t, "low", executionOrder[i], fmt.Sprintf("Task at index %d should be low", i))
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for tasks execution")
	}
}

func TestTaskMQ_WeightedPriorityFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	qLow := "low_weighted_queue"
	qCritical := "critical_weighted_queue"

	var executionOrder []string
	var mu sync.Mutex
	doneChan := make(chan struct{})

	var rdb *goredis.Client
	var client mqclient.Client
	var worker mqworker.Worker

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				cfg := NewTestConfig()
				cfg.TaskMQ.PriorityQueuesEnabled = true
				cfg.TaskMQ.PriorityStrategy = "weighted"
				cfg.TaskMQ.Queues = []config.QueueConfig{
					{Name: qLow, Concurrency: 2, Priority: 1},
					{Name: qCritical, Concurrency: 2, Priority: 9},
				}
				return cfg
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func() codec.Codec { return codec.JSONCodec{} },
			taskmq.ProvideWorkers,
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	_ = rdb.Del(ctx, keys.StreamKey(qLow), keys.StreamKey(qCritical)).Err()
	defer rdb.Del(ctx, keys.StreamKey(qLow), keys.StreamKey(qCritical))

	// Pre-create consumer groups with "0" cursor so we can read pre-existing messages
	_ = rdb.XGroupCreateMkStream(ctx, keys.StreamKey(qLow), "taskmq-priority-group", "0").Err()
	_ = rdb.XGroupCreateMkStream(ctx, keys.StreamKey(qCritical), "taskmq-priority-group", "0").Err()

	// Register handlers
	mqWorker, ok := worker.(mqworker.MultiQueueWorker)
	assert.True(t, ok)

	handler := func(queueName string) mqworker.HandlerFunc {
		return func(ctx context.Context, task *taskmodel.Task) error {
			mu.Lock()
			executionOrder = append(executionOrder, queueName)
			orderLen := len(executionOrder)
			mu.Unlock()

			if orderLen >= 10 {
				select {
				case doneChan <- struct{}{}:
				default:
				}
			}
			return nil
		}
	}

	mqWorker.Queue(qLow).Register("task:weighted:low", handler("low"))
	mqWorker.Queue(qCritical).Register("task:weighted:critical", handler("critical"))

	// Enqueue 5 low weighted tasks and 5 critical weighted tasks
	for i := 0; i < 5; i++ {
		tLow := taskmodel.NewTask("task:weighted:low", []byte(fmt.Sprintf("low-%d", i)), taskmodel.TaskOptions{Queue: qLow})
		err := client.Enqueue(ctx, tLow)
		assert.NoError(t, err)

		tCritical := taskmodel.NewTask("task:weighted:critical", []byte(fmt.Sprintf("critical-%d", i)), taskmodel.TaskOptions{Queue: qCritical})
		err = client.Enqueue(ctx, tCritical)
		assert.NoError(t, err)
	}

	app.RequireStart()
	defer app.RequireStop()

	// Wait for execution
	select {
	case <-doneChan:
		mu.Lock()
		defer mu.Unlock()
		assert.Len(t, executionOrder, 10)
		t.Logf("Weighted execution order: %v", executionOrder)
		var lowCount, criticalCount int
		for _, q := range executionOrder {
			if q == "low" {
				lowCount++
			} else if q == "critical" {
				criticalCount++
			}
		}
		assert.Greater(t, lowCount, 0, "Should process at least one low priority task")
		assert.Greater(t, criticalCount, 0, "Should process at least one critical priority task")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for tasks execution")
	}
}
