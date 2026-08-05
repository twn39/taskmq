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
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestTaskMQ_PauseResume_SingleQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "pause")
	streamKey := keys.KeysFor(queueName).Stream()
	pausedKey := keys.KeysFor(queueName).Paused()

	runChan := make(chan string, 10)

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
					mqworker.WithGroup("pause-group"),
					mqworker.WithConsumer("pause-consumer"),
					mqworker.WithConcurrency(2),
				)
				pool.Register("task:pause_resume", func(ctx context.Context, task *taskmodel.Task) error {
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

	// Wait for control subscriber to be ready
	controlChannel := keys.KeysFor(queueName).Control()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for control subscriber to be ready")
		default:
			numSub, err := rdb.PubSubNumSub(ctx, controlChannel).Result()
			if err == nil && numSub[controlChannel] > 0 {
				goto ready
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
ready:

	// Case 1: Active queue - Task 1 should be processed immediately
	task1 := taskmodel.NewTask("task:pause_resume", []byte("task1"), taskmodel.TaskOptions{Queue: queueName})
	err := client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	select {
	case p := <-runChan:
		assert.Equal(t, "task1", p)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for task1 to be processed")
	}

	// Wait briefly to ensure the worker's control subscriber is fully established
	time.Sleep(200 * time.Millisecond)

	// Case 2: Pause queue - Enqueue Task 2, it should NOT be processed
	err = client.Pause(ctx, queueName)
	assert.NoError(t, err)

	// Verify IsPaused status
	isPaused, err := client.IsPaused(ctx, queueName)
	assert.NoError(t, err)
	assert.True(t, isPaused)

	// Allow some time for subscriber to receive state change and worker to unblock from XReadGroup and detect the pause state
	time.Sleep(1500 * time.Millisecond)

	task2 := taskmodel.NewTask("task:pause_resume", []byte("task2"), taskmodel.TaskOptions{Queue: queueName})
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

	queueActive := UniqueQueue(t, "pause_active")
	queuePaused := UniqueQueue(t, "pause_paused")

	streamActive := keys.KeysFor(queueActive).Stream()
	streamPaused := keys.KeysFor(queuePaused).Stream()
	pausedKey := keys.KeysFor(queuePaused).Paused()

	runChan := make(chan string, 10)

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
				pw := mqworker.NewPriorityWorker(rdb, logger,
					mqworker.WithGroup("priority-group"),
					mqworker.WithConsumer("priority-consumer"),
					mqworker.WithConcurrency(2),
					mqworker.WithPriorityStrategy("strict"),
					mqworker.WithPriorityQueues([]mqworker.QueuePriority{
						{Name: queueActive, Weight: 10},
						{Name: queuePaused, Weight: 5},
					}),
				)
				pw.Register("task:priority_test", func(ctx context.Context, task *taskmodel.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pw
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	rdb.Del(ctx, streamActive, streamPaused, pausedKey, keys.KeysFor(queueActive).Paused())
	defer rdb.Del(ctx, streamActive, streamPaused, pausedKey, keys.KeysFor(queueActive).Paused())

	// Create client temporarily to pause the queue before starting the worker pool
	tempClient := mqclient.NewClient(rdb)
	err := tempClient.Pause(ctx, queuePaused)
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue to paused queue
	taskPaused := taskmodel.NewTask("task:priority_test", []byte("paused_task"), taskmodel.TaskOptions{Queue: queuePaused})
	err = client.Enqueue(ctx, taskPaused)
	assert.NoError(t, err)

	// Enqueue to active queue
	taskActive := taskmodel.NewTask("task:priority_test", []byte("active_task"), taskmodel.TaskOptions{Queue: queueActive})
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

func TestTaskMQ_PauseBlockedWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "pause_blocked")
	streamKey := keys.KeysFor(queueName).Stream()
	pausedKey := keys.KeysFor(queueName).Paused()

	runChan := make(chan string, 1)

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
					mqworker.WithGroup("pause-blocked-group"),
					mqworker.WithConsumer("pause-blocked-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:pause_blocked_test", func(ctx context.Context, task *taskmodel.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, pausedKey)
	defer rdb.Del(ctx, streamKey, pausedKey)

	// Start worker pool. Since no tasks exist, it blocks in XReadGroup.
	app.RequireStart()
	defer app.RequireStop()

	// Wait 500ms to ensure worker is fully started and blocked in XReadGroup.
	time.Sleep(500 * time.Millisecond)

	// Pause the queue.
	err := client.Pause(ctx, queueName)
	assert.NoError(t, err)

	// Wait 1500ms. Since we set Block: 1s, the worker will unblock from XReadGroup within 1s,
	// detect the pause state, and block on pauseCh.
	time.Sleep(1500 * time.Millisecond)

	// Now enqueue a task.
	task := taskmodel.NewTask("task:pause_blocked_test", []byte("delayed_payload"), taskmodel.TaskOptions{Queue: queueName})
	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Verify task is NOT processed by the worker (since worker should be paused).
	select {
	case p := <-runChan:
		t.Fatalf("Task %s should NOT have executed while queue was paused", p)
	case <-time.After(1500 * time.Millisecond):
		// Success: task not processed.
	}

	// Resume the queue.
	err = client.Resume(ctx, queueName)
	assert.NoError(t, err)

	// Verify the task executes immediately now.
	select {
	case p := <-runChan:
		assert.Equal(t, "delayed_payload", p)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for task to execute after resume")
	}
}
