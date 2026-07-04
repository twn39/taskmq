package integration

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestTaskMQ_GCRARateLimiting(t *testing.T) {
	ctx := context.Background()
	var rdb *redis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	qName := "gcra_rate_limit_queue"

	app := fxtest.New(
		t,
		fx.Provide(
			func() *config.Config {
				cfg := NewTestConfig()
				cfg.TaskMQ.Queues = []config.QueueConfig{
					{
						Name:              qName,
						Concurrency:       10,
						RateLimitMax:      5,
						RateLimitDuration: 2 * time.Second,
					},
				}
				return cfg
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func() taskmq.Codec { return taskmq.JSONCodec{} },
			taskmq.ProvideWorkers,
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis keys
	rdb.Del(ctx, taskmq.StreamKey(qName), taskmq.DelayedKey(qName), taskmq.RateLimitKey(qName, ""))
	defer rdb.Del(ctx, taskmq.StreamKey(qName), taskmq.DelayedKey(qName), taskmq.RateLimitKey(qName, ""))

	// Pre-create consumer group with "0" cursor so we can read pre-existing messages
	_ = rdb.XGroupCreateMkStream(ctx, taskmq.StreamKey(qName), "taskmq-group-"+qName, "0").Err()

	var mu sync.Mutex
	executionTimes := make([]time.Time, 0)
	doneChan := make(chan struct{})

	// Register task handler
	mqWorker, ok := worker.(taskmq.MultiQueueWorker)
	assert.True(t, ok)

	mqWorker.Queue(qName).Register("task:rate_limit:test", func(ctx context.Context, task *taskmq.Task) error {
		mu.Lock()
		executionTimes = append(executionTimes, time.Now())
		count := len(executionTimes)
		mu.Unlock()

		if count >= 10 {
			close(doneChan)
		}
		return nil
	})

	// Start App
	app.RequireStart()
	defer app.RequireStop()

	// Enqueue 10 tasks
	startTime := time.Now()
	for i := 0; i < 10; i++ {
		err := client.Enqueue(ctx, &taskmq.Task{
			Queue: qName,
			Name:  "task:rate_limit:test",
		})
		assert.NoError(t, err)
	}

	// Wait for execution completion
	select {
	case <-doneChan:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for tasks to execute")
	}

	// Analyze execution timestamps:
	// - 5 tasks (burst size) should execute in the first batch (close to startTime).
	// - The remaining 5 tasks must be rate-limited and delayed.
	// - Total duration should be at least ~2 seconds.
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, executionTimes, 10)
	totalDuration := executionTimes[9].Sub(startTime)
	t.Logf("Total execution duration for 10 tasks: %v", totalDuration)
	assert.GreaterOrEqual(t, totalDuration, 1800*time.Millisecond, "Rate limiter should enforce delay of at least ~2s for enqueued tasks")
}

func TestTaskMQ_GroupRateLimiting(t *testing.T) {
	ctx := context.Background()
	var rdb *redis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	qName := "group_rate_limit_queue"

	app := fxtest.New(
		t,
		fx.Provide(
			func() *config.Config {
				cfg := NewTestConfig()
				cfg.TaskMQ.Queues = []config.QueueConfig{
					{
						Name:              qName,
						Concurrency:       10,
						RateLimitMax:      2,
						RateLimitDuration: 3 * time.Second,
						RateLimitKeyField: "tenantId", // Extract group key from payload
					},
				}
				return cfg
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func() taskmq.Codec { return taskmq.JSONCodec{} },
			taskmq.ProvideWorkers,
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis keys
	rdb.Del(ctx, taskmq.StreamKey(qName), taskmq.DelayedKey(qName), taskmq.RateLimitKey(qName, "tenant-A"), taskmq.RateLimitKey(qName, "tenant-B"))
	defer rdb.Del(ctx, taskmq.StreamKey(qName), taskmq.DelayedKey(qName), taskmq.RateLimitKey(qName, "tenant-A"), taskmq.RateLimitKey(qName, "tenant-B"))

	// Pre-create consumer group with "0" cursor so we can read pre-existing messages
	_ = rdb.XGroupCreateMkStream(ctx, taskmq.StreamKey(qName), "taskmq-group-"+qName, "0").Err()

	var mu sync.Mutex
	executionTimes := make(map[string][]time.Time)
	doneChan := make(chan struct{})

	// Register task handler
	mqWorker, ok := worker.(taskmq.MultiQueueWorker)
	assert.True(t, ok)

	mqWorker.Queue(qName).Register("task:group_limit:test", func(ctx context.Context, task *taskmq.Task) error {
		var payload map[string]interface{}
		_ = json.Unmarshal(task.Payload, &payload)
		tenantID := payload["tenantId"].(string)

		mu.Lock()
		executionTimes[tenantID] = append(executionTimes[tenantID], time.Now())
		totalCount := len(executionTimes["tenant-A"]) + len(executionTimes["tenant-B"])
		mu.Unlock()

		if totalCount >= 4 {
			close(doneChan)
		}
		return nil
	})

	// Start App
	app.RequireStart()
	defer app.RequireStop()

	// Enqueue 4 tasks:
	// - 2 for tenant-A (within rate limit of 2)
	// - 2 for tenant-B (within rate limit of 2)
	startTime := time.Now()
	for _, tenant := range []string{"tenant-A", "tenant-B"} {
		payloadBytes, _ := json.Marshal(map[string]string{"tenantId": tenant})
		for i := 0; i < 2; i++ {
			err := client.Enqueue(ctx, &taskmq.Task{
				Queue:   qName,
				Name:    "task:group_limit:test",
				Payload: payloadBytes,
			})
			assert.NoError(t, err)
		}
	}

	// Wait for execution completion
	select {
	case <-doneChan:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for group tasks to execute")
	}

	// Because tenant-A and tenant-B are rate-limited independently (limit of 2 each),
	// all 4 tasks should execute immediately without waiting for the 3-second window!
	mu.Lock()
	defer mu.Unlock()

	totalDuration := time.Since(startTime)
	t.Logf("Total group execution duration: %v", totalDuration)
	assert.Less(t, totalDuration, 1500*time.Millisecond, "Independent group limiters should allow concurrent executions up to their individual limits")
	assert.Len(t, executionTimes["tenant-A"], 2)
	assert.Len(t, executionTimes["tenant-B"], 2)
}

func TestTaskMQ_PriorityQueueRateLimiting(t *testing.T) {
	ctx := context.Background()
	var rdb *redis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	qLow := "pq_rl_low"
	qCritical := "pq_rl_critical"

	app := fxtest.New(
		t,
		fx.Provide(
			func() *config.Config {
				cfg := NewTestConfig()
				cfg.TaskMQ.PriorityQueuesEnabled = true
				cfg.TaskMQ.PriorityStrategy = "strict"
				cfg.TaskMQ.Queues = []config.QueueConfig{
					{
						Name:        qLow,
						Concurrency: 1,
						Priority:    1,
					},
					{
						Name:              qCritical,
						Concurrency:       1,
						Priority:          10,
						RateLimitMax:      1,
						RateLimitDuration: 3 * time.Second, // Heavily rate limited
					},
				}
				return cfg
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func() taskmq.Codec { return taskmq.JSONCodec{} },
			taskmq.ProvideWorkers,
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis keys
	rdb.Del(ctx, taskmq.StreamKey(qLow), taskmq.StreamKey(qCritical), taskmq.RateLimitKey(qCritical, ""))
	defer rdb.Del(ctx, taskmq.StreamKey(qLow), taskmq.StreamKey(qCritical), taskmq.RateLimitKey(qCritical, ""))

	// Pre-create consumer groups with "0" cursor so we can read pre-existing messages
	_ = rdb.XGroupCreateMkStream(ctx, taskmq.StreamKey(qLow), "taskmq-priority-group", "0").Err()
	_ = rdb.XGroupCreateMkStream(ctx, taskmq.StreamKey(qCritical), "taskmq-priority-group", "0").Err()

	var mu sync.Mutex
	executedQueues := make([]string, 0)
	doneChan := make(chan struct{})

	// Register task handlers
	mqWorker, ok := worker.(taskmq.MultiQueueWorker)
	assert.True(t, ok)

	handler := func(q string) taskmq.HandlerFunc {
		return func(ctx context.Context, task *taskmq.Task) error {
			mu.Lock()
			executedQueues = append(executedQueues, q)
			totalCount := len(executedQueues)
			mu.Unlock()

			if totalCount >= 4 {
				close(doneChan)
			}
			return nil
		}
	}

	mqWorker.Queue(qLow).Register("task:pq_rl:low", handler(qLow))
	mqWorker.Queue(qCritical).Register("task:pq_rl:critical", handler(qCritical))

	// Enqueue 2 critical priority tasks and 2 low priority tasks
	// - 1st critical task runs immediately.
	// - 2nd critical task hits the 3s rate limit and gets deferred.
	// - The worker pool skips the rate-limited critical queue and immediately executes the 2 low priority tasks!
	err := client.Enqueue(ctx, &taskmq.Task{Queue: qCritical, Name: "task:pq_rl:critical"})
	assert.NoError(t, err)
	err = client.Enqueue(ctx, &taskmq.Task{Queue: qCritical, Name: "task:pq_rl:critical"})
	assert.NoError(t, err)

	for i := 0; i < 2; i++ {
		err := client.Enqueue(ctx, &taskmq.Task{Queue: qLow, Name: "task:pq_rl:low"})
		assert.NoError(t, err)
	}

	lenLow, _ := rdb.XLen(ctx, taskmq.StreamKey(qLow)).Result()
	lenCrit, _ := rdb.XLen(ctx, taskmq.StreamKey(qCritical)).Result()
	t.Logf("=== BEFORE START: low_len=%d, critical_len=%d ===", lenLow, lenCrit)

	// Start App
	app.RequireStart()
	defer app.RequireStop()

	// Wait for execution completion
	select {
	case <-doneChan:
	case <-time.After(8 * time.Second):
		t.Fatal("Timeout waiting for priority tasks to execute under rate limiting")
	}

	mu.Lock()
	defer mu.Unlock()

	// Execution order should contain low priority tasks before the second critical task completes,
	// because the critical queue got rate-limited and skipped.
	t.Logf("Executed queue order: %v", executedQueues)
	assert.Len(t, executedQueues, 4)
	assert.Equal(t, qCritical, executedQueues[0])
	assert.Equal(t, qLow, executedQueues[1])
	assert.Equal(t, qLow, executedQueues[2])
	assert.Equal(t, qCritical, executedQueues[3])
}

func TestTaskMQ_RateLimitDeferralAtomicity(t *testing.T) {
	ctx := context.Background()
	var rdb *redis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	qName := "atomicity_rate_limit_queue"

	app := fxtest.New(
		t,
		fx.Provide(
			func() *config.Config {
				cfg := NewTestConfig()
				cfg.TaskMQ.Queues = []config.QueueConfig{
					{
						Name:              qName,
						Concurrency:       1,
						RateLimitMax:      1,
						RateLimitDuration: 10 * time.Second, // Long rate limit duration
						RateLimitKeyField: "tenantId",        // Forces Phase 2 deferral!
					},
				}
				return cfg
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func() taskmq.Codec { return taskmq.JSONCodec{} },
			taskmq.ProvideWorkers,
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis keys
	rdb.Del(ctx, taskmq.StreamKey(qName), taskmq.DelayedKey(qName), taskmq.RateLimitKey(qName, "tenant-A"))
	defer rdb.Del(ctx, taskmq.StreamKey(qName), taskmq.DelayedKey(qName), taskmq.RateLimitKey(qName, "tenant-A"))

	// Pre-create consumer group
	_ = rdb.XGroupCreateMkStream(ctx, taskmq.StreamKey(qName), "taskmq-group-"+qName, "0").Err()

	var mu sync.Mutex
	executionCount := 0
	firstTaskDone := make(chan struct{})

	mqWorker, ok := worker.(taskmq.MultiQueueWorker)
	assert.True(t, ok)

	mqWorker.Queue(qName).Register("task:atomicity:test", func(ctx context.Context, task *taskmq.Task) error {
		mu.Lock()
		executionCount++
		count := executionCount
		mu.Unlock()

		if count == 1 {
			close(firstTaskDone)
		}
		return nil
	})

	app.RequireStart()
	defer app.RequireStop()

	payloadBytes, _ := json.Marshal(map[string]string{"tenantId": "tenant-A"})

	// 1. Enqueue first task (should execute immediately)
	err := client.Enqueue(ctx, &taskmq.Task{
		Queue:   qName,
		Name:    "task:atomicity:test",
		Payload: payloadBytes,
	})
	assert.NoError(t, err)

	// Wait for first task to execute
	select {
	case <-firstTaskDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for first task")
	}

	// 2. Enqueue second task (should hit rate limit and get deferred immediately)
	err = client.Enqueue(ctx, &taskmq.Task{
		Queue:   qName,
		Name:    "task:atomicity:test",
		Payload: payloadBytes,
	})
	assert.NoError(t, err)

	// Wait and verify: the second task must be deferred atomically
	// It should:
	// a) Be deleted from the Stream (Stream length = 0)
	// b) Be acknowledged (PEL size = 0)
	// c) Be placed in the Delayed ZSET (ZSET size = 1)
	assert.Eventually(t, func() bool {
		streamLen, _ := rdb.XLen(ctx, taskmq.StreamKey(qName)).Result()
		zsetSize, _ := rdb.ZCard(ctx, taskmq.DelayedKey(qName)).Result()
		
		// Check PEL size
		pendingInfo, _ := rdb.XPending(ctx, taskmq.StreamKey(qName), "taskmq-group-"+qName).Result()
		pelSize := 0
		if pendingInfo != nil {
			pelSize = int(pendingInfo.Count)
		}

		t.Logf("CHECK: streamLen=%d, pelSize=%d, zsetSize=%d", streamLen, pelSize, zsetSize)

		return streamLen == 0 && pelSize == 0 && zsetSize == 1
	}, 3*time.Second, 100*time.Millisecond, "Task was not deferred atomically into delayed ZSET or cleaned from Stream/PEL")
}

