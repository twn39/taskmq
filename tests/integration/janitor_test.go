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
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskMQ_JanitorRecoveryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "janitor")
	streamKey := keys.KeysFor(queueName).Stream()

	var runCount int64
	doneChan := make(chan bool, 1)

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
					mqworker.WithGroup("janitor-group"),
					mqworker.WithConsumer("janitor-consumer"),
					mqworker.WithConcurrency(1),
				)
				pool.Register("task:crash", func(ctx context.Context, task *taskmodel.Task) error {
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

	task := taskmodel.NewTask("task:crash", []byte("crash payload"), taskmodel.TaskOptions{
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
	// Hung handlers ignore context; Stop must not wait the production default (30s)
	// or this test's overall deadline will expire mid-scenario.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "poison")
	streamKey := keys.KeysFor(queueName).Stream()

	var rdb goredis.UniversalClient
	var client mqclient.Client

	// Setup client and redis
	appSetup := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
		),
		fx.Populate(&rdb, &client),
	)
	appSetup.RequireStart()
	defer appSetup.RequireStop()

	// Clear previous data
	rdb.Del(ctx, streamKey)
	rdb.Del(ctx, keys.KeysFor(queueName).DLQ())
	rdb.Del(ctx, keys.KeysFor(queueName).DLQIndex())
	defer func() {
		rdb.Del(ctx, streamKey)
		rdb.Del(ctx, keys.KeysFor(queueName).DLQ())
		rdb.Del(ctx, keys.KeysFor(queueName).DLQIndex())
	}()

	// Block channel to simulate crash/hang
	blockChan := make(chan struct{})
	// Short shutdown so Stop returns quickly while handlers remain hung (crash sim).
	const crashShutdown = 200 * time.Millisecond

	// 1. Start Worker Pool 1 to create the consumer group, then enqueue, consume, and stop
	var runCount1 int64
	w1Sig := make(chan struct{}, 1)
	w1 := mqworker.NewWorkerPool(rdb, zap.NewNop(), queueName,
		mqworker.WithGroup("poison-group"),
		mqworker.WithConsumer("consumer-w1"),
		mqworker.WithConcurrency(1),
		mqworker.WithJanitorInterval(10*time.Second),
		mqworker.WithJanitorMinIdleTime(10*time.Second),
		mqworker.WithShutdownTimeout(crashShutdown),
	)
	w1.Register("task:poison", func(ctx context.Context, task *taskmodel.Task) error {
		atomic.AddInt64(&runCount1, 1)
		w1Sig <- struct{}{}
		<-blockChan // Block forever to simulate crashed/hung worker
		return nil
	})

	assert.NoError(t, w1.Start(ctx))

	// Enqueue a task with MaxRetry = 1 after starting w1
	task := taskmodel.NewTask("task:poison", []byte("poison payload"), taskmodel.TaskOptions{
		Queue:    queueName,
		MaxRetry: taskmodel.Ptr(1),
	})
	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for worker 1 to pick up and run the task
	select {
	case <-w1Sig:
	case <-ctx.Done():
		t.Fatal("Timeout waiting for worker 1 to start task")
	}

	w1.Stop(context.Background()) // Crash/Stop worker pool 1. Message remains in PEL for consumer-w1.
	assert.Equal(t, int64(1), atomic.LoadInt64(&runCount1))

	// Wait briefly to ensure the message becomes idle for 50ms
	time.Sleep(100 * time.Millisecond)

	// 2. Start Worker Pool 2. Janitor will reclaim. (Delivery Count = 2, Retry = 1)
	// It should run the handler again, and then we stop it to leave it pending.
	var runCount2 int64
	w2Sig := make(chan struct{}, 1)
	w2 := mqworker.NewWorkerPool(rdb, zap.NewNop(), queueName,
		mqworker.WithGroup("poison-group"),
		mqworker.WithConsumer("consumer-w2"),
		mqworker.WithConcurrency(1),
		mqworker.WithJanitorInterval(50*time.Millisecond),
		mqworker.WithJanitorMinIdleTime(50*time.Millisecond),
		mqworker.WithShutdownTimeout(crashShutdown),
	)
	w2.Register("task:poison", func(ctx context.Context, task *taskmodel.Task) error {
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

	w2.Stop(context.Background()) // Crash/Stop worker pool 2. Message remains in PEL.
	assert.Equal(t, int64(1), atomic.LoadInt64(&runCount2))

	// Wait briefly to ensure the message becomes idle for 50ms again
	time.Sleep(100 * time.Millisecond)

	// 3. Start Worker Pool 3. Janitor will reclaim. (Delivery Count = 3, Retry = 2 > MaxRetry = 1)
	// It should directly move to DLQ without running the handler.
	var runCount3 int64
	w3 := mqworker.NewWorkerPool(rdb, zap.NewNop(), queueName,
		mqworker.WithGroup("poison-group"),
		mqworker.WithConsumer("consumer-w3"),
		mqworker.WithConcurrency(1),
		mqworker.WithJanitorInterval(50*time.Millisecond),
		mqworker.WithJanitorMinIdleTime(50*time.Millisecond),
		mqworker.WithShutdownTimeout(crashShutdown),
	)
	w3.Register("task:poison", func(ctx context.Context, task *taskmodel.Task) error {
		atomic.AddInt64(&runCount3, 1)
		return nil
	})

	assert.NoError(t, w3.Start(ctx))

	// Poll DLQ until task appears (indicating direct DLQ routing)
	var dlqTasks []*taskmodel.Task
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
	w3.Stop(context.Background())

	// Assertions
	assert.Equal(t, int64(0), atomic.LoadInt64(&runCount3), "Handler 3 should NOT have run because task exceeded MaxRetry")
	assert.Len(t, dlqTasks, 1)
	assert.Equal(t, task.ID, dlqTasks[0].ID)
	assert.Contains(t, dlqTasks[0].LastError, "task exceeded max retry limits")
}
