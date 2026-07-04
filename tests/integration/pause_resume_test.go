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

func TestTaskMQ_PauseResume_SingleQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "pause_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	pausedKey := taskmq.PausedKey(queueName)

	runChan := make(chan string, 10)

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
				pool := taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithGroup("pause-group"),
					taskmq.WithConsumer("pause-consumer"),
					taskmq.WithConcurrency(2),
				)
				pool.Register("task:pause_resume", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up keys before test
	rdb.Del(ctx, streamKey, pausedKey)
	defer rdb.Del(ctx, streamKey, pausedKey)

	app.RequireStart()
	defer app.RequireStop()

	// Case 1: Active queue - Task 1 should be processed immediately
	task1 := taskmq.NewTask("task:pause_resume", []byte("task1"), taskmq.TaskOptions{Queue: queueName})
	err := client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	select {
	case p := <-runChan:
		assert.Equal(t, "task1", p)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for task1 to be processed")
	}

	// Case 2: Pause queue - Enqueue Task 2, it should NOT be processed
	err = client.Pause(ctx, queueName)
	assert.NoError(t, err)

	// Verify IsPaused status
	isPaused, err := client.IsPaused(ctx, queueName)
	assert.NoError(t, err)
	assert.True(t, isPaused)

	// Allow some time for subscriber to receive state change
	time.Sleep(200 * time.Millisecond)

	task2 := taskmq.NewTask("task:pause_resume", []byte("task2"), taskmq.TaskOptions{Queue: queueName})
	err = client.Enqueue(ctx, task2)
	assert.NoError(t, err)

	// Wait to verify it's NOT executed
	select {
	case p := <-runChan:
		t.Fatalf("Task %s should NOT have been processed because the queue is paused!", p)
	case <-time.After(1 * time.Second):
		// Success: task was not processed
	}

	// Case 3: Resume queue - Task 2 should now be processed immediately
	err = client.Resume(ctx, queueName)
	assert.NoError(t, err)

	// Verify IsPaused status
	isPaused, err = client.IsPaused(ctx, queueName)
	assert.NoError(t, err)
	assert.False(t, isPaused)

	select {
	case p := <-runChan:
		assert.Equal(t, "task2", p)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for task2 to be processed after resume")
	}
}

func TestTaskMQ_PauseResume_MultiQueuePriority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueActive := "priority_active_queue"
	queuePaused := "priority_paused_queue"

	streamActive := taskmq.StreamKey(queueActive)
	streamPaused := taskmq.StreamKey(queuePaused)
	pausedKey := taskmq.PausedKey(queuePaused)

	runChan := make(chan string, 10)

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
				pw := taskmq.NewPriorityWorker(rdb, logger,
					taskmq.WithGroup("priority-group"),
					taskmq.WithConsumer("priority-consumer"),
					taskmq.WithConcurrency(2),
					taskmq.WithPriorityStrategy("strict"),
					taskmq.WithPriorityQueues([]taskmq.QueuePriority{
						{Name: queueActive, Weight: 10},
						{Name: queuePaused, Weight: 5},
					}),
				)
				pw.Register("task:priority_test", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pw
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	rdb.Del(ctx, streamActive, streamPaused, pausedKey, taskmq.PausedKey(queueActive))
	defer rdb.Del(ctx, streamActive, streamPaused, pausedKey, taskmq.PausedKey(queueActive))

	// Create client temporarily to pause the queue before starting the worker pool
	tempClient := taskmq.NewClient(rdb)
	err := tempClient.Pause(ctx, queuePaused)
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue to paused queue
	taskPaused := taskmq.NewTask("task:priority_test", []byte("paused_task"), taskmq.TaskOptions{Queue: queuePaused})
	err = client.Enqueue(ctx, taskPaused)
	assert.NoError(t, err)

	// Enqueue to active queue
	taskActive := taskmq.NewTask("task:priority_test", []byte("active_task"), taskmq.TaskOptions{Queue: queueActive})
	err = client.Enqueue(ctx, taskActive)
	assert.NoError(t, err)

	// Verify only active_task is processed
	select {
	case p := <-runChan:
		assert.Equal(t, "active_task", p)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for active_task to execute")
	}

	// Verify paused_task has not executed yet
	select {
	case p := <-runChan:
		t.Fatalf("Task %s should NOT have executed while queue was paused", p)
	case <-time.After(1 * time.Second):
		// Success: paused task not executed
	}

	// Resume the paused queue
	err = client.Resume(ctx, queuePaused)
	assert.NoError(t, err)

	// Verify paused_task executes now
	select {
	case p := <-runChan:
		assert.Equal(t, "paused_task", p)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for paused_task to execute after resume")
	}
}
