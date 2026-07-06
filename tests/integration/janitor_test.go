package integration

import (
	"context"
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

func TestTaskMQ_JanitorRecoveryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueName := "janitor_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	var runCount int64
	doneChan := make(chan bool, 1)

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
					taskmq.WithGroup("janitor-group"),
					taskmq.WithConsumer("janitor-consumer"),
					taskmq.WithConcurrency(1),
				)
				pool.Register("task:crash", func(ctx context.Context, task *taskmq.Task) error {
					count := atomic.AddInt64(&runCount, 1)
					if count == 1 {
						t.Log("Simulating crashed worker on first execution (handler sleeping...)")
						time.Sleep(10 * time.Second)
						return nil
					}

					t.Log("Janitor successfully recovered and executed task on second run!")
					doneChan <- true
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey)
	defer rdb.Del(ctx, streamKey)

	app.RequireStart()
	defer app.RequireStop()

	task := taskmq.NewTask("task:crash", []byte("crash payload"), taskmq.TaskOptions{
		Queue: queueName,
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case <-doneChan:
		assert.Equal(t, int64(2), atomic.LoadInt64(&runCount), "Task should have run twice (once stalled, once recovered by janitor)")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for Janitor recovery")
	}
}

func TestTaskMQ_PoisonPillRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "poison_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client

	// Setup client and redis
	appSetup := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
		),
		fx.Populate(&rdb, &client),
	)
	appSetup.RequireStart()
	defer appSetup.RequireStop()

	// Clear previous data
	rdb.Del(ctx, streamKey)
	rdb.Del(ctx, taskmq.DLQKey(queueName))
	rdb.Del(ctx, taskmq.DLQIndexKey(queueName))
	defer func() {
		rdb.Del(ctx, streamKey)
		rdb.Del(ctx, taskmq.DLQKey(queueName))
		rdb.Del(ctx, taskmq.DLQIndexKey(queueName))
	}()

	// Block channel to simulate crash/hang
	blockChan := make(chan struct{})

	// 1. Start Worker Pool 1 to create the consumer group, then enqueue, consume, and stop
	var runCount1 int64
	w1Sig := make(chan struct{}, 1)
	w1 := taskmq.NewWorkerPool(rdb, zap.NewNop(), queueName,
		taskmq.WithGroup("poison-group"),
		taskmq.WithConsumer("consumer-w1"),
		taskmq.WithConcurrency(1),
		taskmq.WithJanitorInterval(10*time.Second),
		taskmq.WithJanitorMinIdleTime(10*time.Second),
	)
	w1.Register("task:poison", func(ctx context.Context, task *taskmq.Task) error {
		atomic.AddInt64(&runCount1, 1)
		w1Sig <- struct{}{}
		<-blockChan // Block forever to simulate crashed/hung worker
		return nil
	})

	assert.NoError(t, w1.Start(ctx))

	// Enqueue a task with MaxRetry = 1 after starting w1
	task := taskmq.NewTask("task:poison", []byte("poison payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 1,
	})
	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for worker 1 to pick up and run the task
	select {
	case <-w1Sig:
	case <-ctx.Done():
		t.Fatal("Timeout waiting for worker 1 to start task")
	}

	w1.Stop(ctx) // Crash/Stop worker pool 1. Message remains in PEL for consumer-w1.
	assert.Equal(t, int64(1), atomic.LoadInt64(&runCount1))

	// Wait briefly to ensure the message becomes idle for 50ms
	time.Sleep(100 * time.Millisecond)

	// 2. Start Worker Pool 2. Janitor will reclaim. (Delivery Count = 2, Retry = 1)
	// It should run the handler again, and then we stop it to leave it pending.
	var runCount2 int64
	w2Sig := make(chan struct{}, 1)
	w2 := taskmq.NewWorkerPool(rdb, zap.NewNop(), queueName,
		taskmq.WithGroup("poison-group"),
		taskmq.WithConsumer("consumer-w2"),
		taskmq.WithConcurrency(1),
		taskmq.WithJanitorInterval(50*time.Millisecond),
		taskmq.WithJanitorMinIdleTime(50*time.Millisecond),
	)
	w2.Register("task:poison", func(ctx context.Context, task *taskmq.Task) error {
		atomic.AddInt64(&runCount2, 1)
		w2Sig <- struct{}{}
		<-blockChan // Block forever to simulate crashed/hung worker again
		return nil
	})

	assert.NoError(t, w2.Start(ctx))

	// Wait for worker 2 to reclaim and run the task
	select {
	case <-w2Sig:
	case <-ctx.Done():
		t.Fatal("Timeout waiting for worker 2 to reclaim task")
	}

	w2.Stop(ctx) // Crash/Stop worker pool 2. Message remains in PEL.
	assert.Equal(t, int64(1), atomic.LoadInt64(&runCount2))

	// Wait briefly to ensure the message becomes idle for 50ms again
	time.Sleep(100 * time.Millisecond)

	// 3. Start Worker Pool 3. Janitor will reclaim. (Delivery Count = 3, Retry = 2 > MaxRetry = 1)
	// It should directly move to DLQ without running the handler.
	var runCount3 int64
	w3 := taskmq.NewWorkerPool(rdb, zap.NewNop(), queueName,
		taskmq.WithGroup("poison-group"),
		taskmq.WithConsumer("consumer-w3"),
		taskmq.WithConcurrency(1),
		taskmq.WithJanitorInterval(50*time.Millisecond),
		taskmq.WithJanitorMinIdleTime(50*time.Millisecond),
	)
	w3.Register("task:poison", func(ctx context.Context, task *taskmq.Task) error {
		atomic.AddInt64(&runCount3, 1)
		return nil
	})

	assert.NoError(t, w3.Start(ctx))

	// Poll DLQ until task appears (indicating direct DLQ routing)
	var dlqTasks []*taskmq.Task
	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for task to be routed directly to DLQ")
		default:
			dlqTasks, err = client.ListDeadLetters(ctx, queueName, 10)
			if err == nil && len(dlqTasks) > 0 {
				goto Done
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
Done:
	w3.Stop(ctx)

	// Assertions
	assert.Equal(t, int64(0), atomic.LoadInt64(&runCount3), "Handler 3 should NOT have run because task exceeded MaxRetry")
	assert.Len(t, dlqTasks, 1)
	assert.Equal(t, task.ID, dlqTasks[0].ID)
	assert.Contains(t, dlqTasks[0].LastError, "task exceeded max retry limits")
}
