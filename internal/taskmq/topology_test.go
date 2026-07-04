package taskmq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/config"
	"go.uber.org/zap"
)

func TestBuildWorkerTopology_DefaultQueue(t *testing.T) {
	// Create an empty config
	cfg := &config.Config{
		TaskMQ: config.TaskMQConfig{
			Queues: []config.QueueConfig{}, // Empty list should trigger fallback to "default" queue
		},
	}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	logger := zap.NewNop()

	worker, err := BuildWorkerTopology(rdb, logger, cfg, JSONCodec{}, context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, worker)

	// Should be a MultiQueueWorker
	mw, ok := worker.(MultiQueueWorker)
	assert.True(t, ok, "Expected worker to implement MultiQueueWorker")

	// Should have the "default" queue registered
	defaultWorker := mw.Queue("default")
	assert.NotNil(t, defaultWorker)

	// Should be a *workerPool
	pool, ok := defaultWorker.(*workerPool)
	assert.True(t, ok, "Expected default worker to be *workerPool")
	assert.Equal(t, "default", pool.queue)
	assert.Equal(t, 5, pool.concurrency)
	assert.Equal(t, "taskmq-group-default", pool.group)
	assert.Equal(t, "taskmq-consumer-default-1", pool.consumer)
}

func TestBuildWorkerTopology_MultipleNormalQueues(t *testing.T) {
	cfg := &config.Config{
		TaskMQ: config.TaskMQConfig{
			Queues: []config.QueueConfig{
				{
					Name:        "queue-a",
					Concurrency: 3,
					Group:       "group-a",
					Consumer:    "consumer-a",
				},
				{
					Name:              "queue-b",
					Concurrency:       10,
					RateLimitMax:      100,
					RateLimitDuration: 1 * time.Minute,
					RateLimitKeyField: "user_id",
				},
			},
			PriorityQueuesEnabled: false,
		},
	}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	logger := zap.NewNop()

	worker, err := BuildWorkerTopology(rdb, logger, cfg, JSONCodec{}, context.Background())
	assert.NoError(t, err)

	mw, ok := worker.(MultiQueueWorker)
	assert.True(t, ok)

	// Verify queue-a
	workerA := mw.Queue("queue-a")
	assert.NotNil(t, workerA)
	poolA, ok := workerA.(*workerPool)
	assert.True(t, ok)
	assert.Equal(t, "queue-a", poolA.queue)
	assert.Equal(t, 3, poolA.concurrency)
	assert.Equal(t, "group-a", poolA.group)
	assert.Equal(t, "consumer-a", poolA.consumer)

	// Verify queue-b
	workerB := mw.Queue("queue-b")
	assert.NotNil(t, workerB)
	poolB, ok := workerB.(*workerPool)
	assert.True(t, ok)
	assert.Equal(t, "queue-b", poolB.queue)
	assert.Equal(t, 10, poolB.concurrency)
	assert.Equal(t, "taskmq-group-queue-b", poolB.group) // fallback
	assert.Equal(t, int64(100), poolB.rateLimitMax)
	assert.Equal(t, 1*time.Minute, poolB.rateLimitDuration)
	assert.Equal(t, "user_id", poolB.rateLimitKeyField)
}

func TestBuildWorkerTopology_PriorityAndNormalQueues(t *testing.T) {
	cfg := &config.Config{
		TaskMQ: config.TaskMQConfig{
			PriorityQueuesEnabled: true,
			PriorityStrategy:      "strict",
			Queues: []config.QueueConfig{
				{
					Name:        "high-priority-queue",
					Concurrency: 2,
					Priority:    10,
				},
				{
					Name:        "low-priority-queue",
					Concurrency: 3,
					Priority:    1,
				},
				{
					Name:        "normal-queue",
					Concurrency: 4,
					Priority:    0, // normal queue (priority = 0 or disabled)
				},
			},
		},
	}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	logger := zap.NewNop()

	worker, err := BuildWorkerTopology(rdb, logger, cfg, JSONCodec{}, context.Background())
	assert.NoError(t, err)

	mw, ok := worker.(MultiQueueWorker)
	assert.True(t, ok)

	// Verify priority queues (high-priority-queue and low-priority-queue should share the SAME priorityWorker instance)
	workerHigh := mw.Queue("high-priority-queue")
	workerLow := mw.Queue("low-priority-queue")
	assert.NotNil(t, workerHigh)
	assert.NotNil(t, workerLow)

	pwHigh, ok := workerHigh.(*priorityWorker)
	assert.True(t, ok)
	pwLow, ok := workerLow.(*priorityWorker)
	assert.True(t, ok)

	// They must point to the exact same PriorityWorker instance
	assert.Same(t, pwHigh, pwLow)

	// Verify priorityWorker settings
	assert.Equal(t, "strict", pwHigh.priorityStrategy)
	assert.Equal(t, 5, pwHigh.concurrency) // Concurrency is the sum of priority queue concurrency (2 + 3)
	assert.Len(t, pwHigh.queues, 2)
	assert.Equal(t, "taskmq-priority-group", pwHigh.group)

	// Verify normal queue remains as a separate workerPool instance
	workerNormal := mw.Queue("normal-queue")
	assert.NotNil(t, workerNormal)
	poolNormal, ok := workerNormal.(*workerPool)
	assert.True(t, ok)
	assert.Equal(t, "normal-queue", poolNormal.queue)
	assert.Equal(t, 4, poolNormal.concurrency)
	assert.Equal(t, "taskmq-group-normal-queue", poolNormal.group)
}
