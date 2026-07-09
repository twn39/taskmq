package integration

import (
	"context"
	"encoding/json"
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

func TestTaskMQ_CronFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "cron_test_queue"

	var runCount int64
	doneChan := make(chan bool, 1)

	var rdb *goredis.Client
	var client mqclient.Client
	var worker mqworker.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(2),
					mqworker.WithCronHealingInterval(2*time.Second),
					mqworker.WithCronHealingLockTTL(1800*time.Millisecond),
				)
				// Register handler for the cron job
				pool.Register("cron:ticker", func(ctx context.Context, task *taskmodel.Task) error {
					val := atomic.AddInt64(&runCount, 1)
					if val >= 3 {
						select {
						case doneChan <- true:
						default:
						}
					}
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		keys.KeysFor(queueName).Stream(),
		keys.KeysFor(queueName).Delayed(),
		keys.KeysFor(queueName).CronConfigs(),
	).Err()
	assert.NoError(t, err)

	// Register the Cron task before starting the worker
	task := taskmodel.NewTask("cron:ticker", []byte("tick-payload"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	// Trigger every 2 seconds
	err = client.RegisterCron(ctx, "cron:ticker", "*/2 * * * * *", task)
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// Wait for cron to trigger at least 3 times
	select {
	case <-doneChan:
		// Success!
	case <-ctx.Done():
		t.Fatal("Timeout waiting for cron to execute 3 times")
	}

	// Now verify Self-Healing:
	// 1. Corrupt/Delete the ZSET entry representing the next scheduled run
	delayedKey := keys.KeysFor(queueName).Delayed()
	err = rdb.Del(ctx, delayedKey).Err()
	assert.NoError(t, err)

	// Reset run counter
	atomic.StoreInt64(&runCount, 0)

	// 2. Wait for Self-healing loop to detect the missing scheduled execution and heal it
	// Self-healing loop runs every 10 seconds. So within 15 seconds, it should heal and execute at least once.
	healCtx, healCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer healCancel()

	healChan := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-healCtx.Done():
				return
			default:
				if atomic.LoadInt64(&runCount) >= 1 {
					healChan <- true
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	select {
	case <-healChan:
		// Self-healing recovered the scheduler chain successfully!
	case <-healCtx.Done():
		t.Fatal("Timeout waiting for self-healing loop to reschedule missing cron task")
	}
}

func TestTaskMQ_CronSelfHealing_CustomConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "cron_healing_custom_test_queue"

	var runCount int64
	doneChan := make(chan bool, 1)

	var rdb *goredis.Client
	var client mqclient.Client
	var worker mqworker.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(1),
					mqworker.WithCronHealingInterval(1*time.Second),
					mqworker.WithCronHealingLockTTL(800*time.Millisecond),
				)
				pool.Register("cron:healing:custom", func(ctx context.Context, task *taskmodel.Task) error {
					val := atomic.AddInt64(&runCount, 1)
					if val >= 1 {
						select {
						case doneChan <- true:
						default:
						}
					}
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		keys.KeysFor(queueName).Stream(),
		keys.KeysFor(queueName).Delayed(),
		keys.KeysFor(queueName).CronConfigs(),
	).Err()
	assert.NoError(t, err)

	// Register the Cron task before starting the worker
	task := taskmodel.NewTask("cron:healing:custom", []byte("healing-payload"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	// Trigger every 1 second
	err = client.RegisterCron(ctx, "cron:healing:custom", "*/1 * * * * *", task)
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// Wait for cron to run at least once
	select {
	case <-doneChan:
		// Success!
	case <-ctx.Done():
		t.Fatal("Timeout waiting for cron task execution")
	}

	// Corrupt/Delete the ZSET entry to simulate broken chain
	delayedKey := keys.KeysFor(queueName).Delayed()
	err = rdb.Del(ctx, delayedKey).Err()
	assert.NoError(t, err)

	// Reset run counter
	atomic.StoreInt64(&runCount, 0)

	// Wait for self-healing loop to heal and reschedule (interval is 1s, so it should heal within 2s)
	healCtx, healCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer healCancel()

	healChan := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-healCtx.Done():
				return
			default:
				if atomic.LoadInt64(&runCount) >= 1 {
					healChan <- true
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	select {
	case <-healChan:
		// Successfully healed!
	case <-healCtx.Done():
		t.Fatal("Timeout waiting for custom config self-healing loop to reschedule task")
	}
}

func TestTaskMQ_CronSelfHealing_Pagination_ExceededLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "cron_healing_pag_exceeded_test_queue"

	var rdb *goredis.Client
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(1),
					mqworker.WithCronHealingInterval(1*time.Second),
					mqworker.WithCronHealingLockTTL(800*time.Millisecond),
					mqworker.WithCronHealingScanBatchSize(2),
					mqworker.WithCronHealingScanMaxCount(5),
				)
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		keys.KeysFor(queueName).Stream(),
		keys.KeysFor(queueName).Delayed(),
		keys.KeysFor(queueName).CronConfigs(),
	).Err()
	assert.NoError(t, err)

	// 1. Register a Cron task.
	task := taskmodel.NewTask("cron:pagination", []byte("payload"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	// Trigger every 10 minutes so it doesn't execute immediately
	err = client.RegisterCron(ctx, "cron:pagination", "*/10 * * * *", task)
	assert.NoError(t, err)

	// Fetch the registered cron task from ZSET to find its score
	delayedKey := keys.KeysFor(queueName).Delayed()
	members, err := rdb.ZRangeWithScores(ctx, delayedKey, 0, -1).Result()
	assert.NoError(t, err)
	assert.Len(t, members, 1)
	cronScore := members[0].Score

	// 2. Add 8 dummy delayed tasks to the ZSET with a slightly lower score (so they are sorted before the cron task)
	for i := 0; i < 8; i++ {
		dummyTask := taskmodel.NewTask("dummy", []byte("dummy-payload"), taskmodel.TaskOptions{
			Queue: queueName,
		})
		serialized, err := json.Marshal(dummyTask)
		assert.NoError(t, err)
		err = rdb.ZAdd(ctx, delayedKey, goredis.Z{
			Score:  cronScore - float64(10-i),
			Member: string(serialized),
		}).Err()
		assert.NoError(t, err)
	}

	// Verify ZSET has 9 elements: 8 dummy tasks first, then 1 cron task.
	allMembers, err := rdb.ZRangeWithScores(ctx, delayedKey, 0, -1).Result()
	assert.NoError(t, err)
	assert.Len(t, allMembers, 9)

	// Start worker pool.
	// Since CronHealingScanMaxCount is 5 and the cron task is at index 8 (9th item),
	// the self-healing loop will scan only the first 5 (indices 0 to 4) and miss the cron task.
	// Therefore, it will assume the cron task is missing and schedule a duplicate.
	app.RequireStart()
	defer app.RequireStop()

	// Wait for self-healing loop to run (interval is 1s, wait 2s)
	time.Sleep(2 * time.Second)

	// Fetch ZSET members again.
	// Since it decided to schedule a duplicate, a new member (representing the rescheduled cron task with different CreatedAt)
	// should have been added to the ZSET.
	newMembers, err := rdb.ZRangeWithScores(ctx, delayedKey, 0, -1).Result()
	assert.NoError(t, err)
	assert.Greater(t, len(newMembers), 9, "Self-healing loop should have rescheduled the cron task because it was beyond scan limit")
}

func TestTaskMQ_CronSelfHealing_Pagination_WithinLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "cron_healing_pag_within_test_queue"

	var rdb *goredis.Client
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(1),
					mqworker.WithCronHealingInterval(1*time.Second),
					mqworker.WithCronHealingLockTTL(800*time.Millisecond),
					mqworker.WithCronHealingScanBatchSize(2),
					mqworker.WithCronHealingScanMaxCount(15),
				)
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		keys.KeysFor(queueName).Stream(),
		keys.KeysFor(queueName).Delayed(),
		keys.KeysFor(queueName).CronConfigs(),
	).Err()
	assert.NoError(t, err)

	// 1. Register a Cron task.
	task := taskmodel.NewTask("cron:pagination", []byte("payload"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	// Trigger every 10 minutes so it doesn't execute immediately
	err = client.RegisterCron(ctx, "cron:pagination", "*/10 * * * *", task)
	assert.NoError(t, err)

	// Fetch the registered cron task from ZSET to find its score
	delayedKey := keys.KeysFor(queueName).Delayed()
	members, err := rdb.ZRangeWithScores(ctx, delayedKey, 0, -1).Result()
	assert.NoError(t, err)
	assert.Len(t, members, 1)
	cronScore := members[0].Score

	// 2. Add 8 dummy delayed tasks to the ZSET with a slightly lower score (so they are sorted before the cron task)
	for i := 0; i < 8; i++ {
		dummyTask := taskmodel.NewTask("dummy", []byte("dummy-payload"), taskmodel.TaskOptions{
			Queue: queueName,
		})
		serialized, err := json.Marshal(dummyTask)
		assert.NoError(t, err)
		err = rdb.ZAdd(ctx, delayedKey, goredis.Z{
			Score:  cronScore - float64(10-i),
			Member: string(serialized),
		}).Err()
		assert.NoError(t, err)
	}

	app.RequireStart()
	defer app.RequireStop()

	// Wait for self-healing loop to run
	time.Sleep(2 * time.Second)

	// Fetch ZSET members again. Since it successfully found the cron task, it should not have scheduled a duplicate.
	finalMembers, err := rdb.ZRangeWithScores(ctx, delayedKey, 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 9, len(finalMembers), "Self-healing loop should not have rescheduled the cron task because it was within scan limit")
}

func TestTaskMQ_CronOverwrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "cron_overwrite_test_queue"

	var version1Count int64
	var version2Count int64
	doneChan := make(chan bool, 1)

	var rdb *goredis.Client
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(2),
					mqworker.WithCronHealingInterval(1*time.Second),
				)
				pool.Register("cron:overwrite", func(ctx context.Context, task *taskmodel.Task) error {
					payload := string(task.Payload)
					if payload == "version-1" {
						atomic.AddInt64(&version1Count, 1)
					} else if payload == "version-2" {
						atomic.AddInt64(&version2Count, 1)
						select {
						case doneChan <- true:
						default:
						}
					}
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		keys.KeysFor(queueName).Stream(),
		keys.KeysFor(queueName).Delayed(),
		keys.KeysFor(queueName).CronConfigs(),
	).Err()
	assert.NoError(t, err)

	// Register version-1 (runs every second)
	task1 := taskmodel.NewTask("cron:overwrite", []byte("version-1"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	err = client.RegisterCron(ctx, "cron:overwrite", "*/1 * * * * *", task1)
	assert.NoError(t, err)

	// Immediately overwrite with version-2 (runs every second)
	task2 := taskmodel.NewTask("cron:overwrite", []byte("version-2"), taskmodel.TaskOptions{
		Queue: queueName,
	})
	err = client.RegisterCron(ctx, "cron:overwrite", "*/1 * * * * *", task2)
	assert.NoError(t, err)

	// Start worker pool
	app.RequireStart()
	defer app.RequireStop()

	// Wait for version-2 to trigger at least once
	select {
	case <-doneChan:
		// Success!
	case <-ctx.Done():
		t.Fatal("Timeout waiting for version-2 of cron job")
	}

	// Give a bit of extra time to ensure version-1 didn't trigger
	time.Sleep(1500 * time.Millisecond)

	assert.Equal(t, int64(0), atomic.LoadInt64(&version1Count), "Version 1 should have been completely unscheduled/removed on overwrite")
	assert.GreaterOrEqual(t, atomic.LoadInt64(&version2Count), int64(1), "Version 2 should have executed at least once")
}
