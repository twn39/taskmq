package integration

import (
	"context"
	"encoding/json"
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

func TestTaskMQ_CronFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "cron_test_queue"

	var runCount int64
	doneChan := make(chan bool, 1)

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
					taskmq.WithConcurrency(2),
					taskmq.WithCronHealingInterval(2*time.Second),
					taskmq.WithCronHealingLockTTL(1800*time.Millisecond),
				)
				// Register handler for the cron job
				pool.Register("cron:ticker", func(ctx context.Context, task *taskmq.Task) error {
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
		taskmq.StreamKey(queueName),
		taskmq.DelayedKey(queueName),
		taskmq.CronConfigsKey(queueName),
	).Err()
	assert.NoError(t, err)

	// Register the Cron task before starting the worker
	task := taskmq.NewTask("cron:ticker", []byte("tick-payload"), taskmq.TaskOptions{
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
	delayedKey := taskmq.DelayedKey(queueName)
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
					taskmq.WithConcurrency(1),
					taskmq.WithCronHealingInterval(1*time.Second),
					taskmq.WithCronHealingLockTTL(800*time.Millisecond),
				)
				pool.Register("cron:healing:custom", func(ctx context.Context, task *taskmq.Task) error {
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
		taskmq.StreamKey(queueName),
		taskmq.DelayedKey(queueName),
		taskmq.CronConfigsKey(queueName),
	).Err()
	assert.NoError(t, err)

	// Register the Cron task before starting the worker
	task := taskmq.NewTask("cron:healing:custom", []byte("healing-payload"), taskmq.TaskOptions{
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
	delayedKey := taskmq.DelayedKey(queueName)
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
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithConcurrency(1),
					taskmq.WithCronHealingInterval(1*time.Second),
					taskmq.WithCronHealingLockTTL(800*time.Millisecond),
					taskmq.WithCronHealingScanBatchSize(2),
					taskmq.WithCronHealingScanMaxCount(5),
				)
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		taskmq.StreamKey(queueName),
		taskmq.DelayedKey(queueName),
		taskmq.CronConfigsKey(queueName),
	).Err()
	assert.NoError(t, err)

	// 1. Register a Cron task.
	task := taskmq.NewTask("cron:pagination", []byte("payload"), taskmq.TaskOptions{
		Queue: queueName,
	})
	// Trigger every 10 minutes so it doesn't execute immediately
	err = client.RegisterCron(ctx, "cron:pagination", "*/10 * * * *", task)
	assert.NoError(t, err)

	// Fetch the registered cron task from ZSET to find its score
	delayedKey := taskmq.DelayedKey(queueName)
	members, err := rdb.ZRangeWithScores(ctx, delayedKey, 0, -1).Result()
	assert.NoError(t, err)
	assert.Len(t, members, 1)
	cronScore := members[0].Score

	// 2. Add 8 dummy delayed tasks to the ZSET with a slightly lower score (so they are sorted before the cron task)
	for i := 0; i < 8; i++ {
		dummyTask := taskmq.NewTask("dummy", []byte("dummy-payload"), taskmq.TaskOptions{
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
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithConcurrency(1),
					taskmq.WithCronHealingInterval(1*time.Second),
					taskmq.WithCronHealingLockTTL(800*time.Millisecond),
					taskmq.WithCronHealingScanBatchSize(2),
					taskmq.WithCronHealingScanMaxCount(15),
				)
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		taskmq.StreamKey(queueName),
		taskmq.DelayedKey(queueName),
		taskmq.CronConfigsKey(queueName),
	).Err()
	assert.NoError(t, err)

	// 1. Register a Cron task.
	task := taskmq.NewTask("cron:pagination", []byte("payload"), taskmq.TaskOptions{
		Queue: queueName,
	})
	// Trigger every 10 minutes so it doesn't execute immediately
	err = client.RegisterCron(ctx, "cron:pagination", "*/10 * * * *", task)
	assert.NoError(t, err)

	// Fetch the registered cron task from ZSET to find its score
	delayedKey := taskmq.DelayedKey(queueName)
	members, err := rdb.ZRangeWithScores(ctx, delayedKey, 0, -1).Result()
	assert.NoError(t, err)
	assert.Len(t, members, 1)
	cronScore := members[0].Score

	// 2. Add 8 dummy delayed tasks to the ZSET with a slightly lower score (so they are sorted before the cron task)
	for i := 0; i < 8; i++ {
		dummyTask := taskmq.NewTask("dummy", []byte("dummy-payload"), taskmq.TaskOptions{
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
