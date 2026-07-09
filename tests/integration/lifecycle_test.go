package integration

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

func lifecycleTestConfig(mutate func(*config.Config)) *config.Config {
	cfg := NewTestConfig()
	// Isolate ports per suite by leaving defaults; Redis shared with other tests.
	if mutate != nil {
		mutate(cfg)
	}
	return cfg
}

func TestTaskMQ_Lifecycle_EnqueueHardLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "lifecycle_hard_limit_q"
	streamKey := taskmq.StreamKey(queueName)
	// Block handlers so messages remain in the stream (PEL) and XLEN stays elevated.
	block := make(chan struct{})

	var rdb *goredis.Client
	var client taskmq.Client
	var lc *taskmq.Lifecycle

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				return lifecycleTestConfig(func(c *config.Config) {
					c.TaskMQ.Queues = []config.QueueConfig{{Name: queueName, Concurrency: 1}}
					c.TaskMQ.Lifecycle.EnqueueHardLimit = 3
					off := false
					c.TaskMQ.Lifecycle.SafeTrimEnabled = &off
				})
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(cfg *config.Config) *taskmq.Lifecycle {
				return taskmq.NewLifecycle(taskmq.LifecycleFromConfig(cfg))
			},
			func(rdb *goredis.Client, codec taskmq.Codec, lifecycle *taskmq.Lifecycle) taskmq.Client {
				return taskmq.NewClient(rdb,
					taskmq.WithClientCodec(codec),
					taskmq.WithClientLifecycle(lifecycle),
				)
			},
			func() taskmq.Codec { return taskmq.JSONCodec{} },
			func(rdb *goredis.Client, logger *zap.Logger, lifecycle *taskmq.Lifecycle) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithGroup("lc-hard-g"),
					taskmq.WithConsumer("lc-hard-c"),
					taskmq.WithConcurrency(1),
					taskmq.WithLifecycle(lifecycle),
					taskmq.WithCodec(taskmq.JSONCodec{}),
				)
				pool.Register("noop", func(ctx context.Context, task *taskmq.Task) error {
					select {
					case <-block:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &lc),
	)

	require.NoError(t, rdb.Del(ctx, streamKey, taskmq.DelayedKey(queueName), taskmq.DLQKey(queueName)).Err())
	defer func() {
		close(block)
		_ = rdb.Del(ctx, streamKey, taskmq.DelayedKey(queueName), taskmq.DLQKey(queueName))
	}()

	app.RequireStart()
	defer app.RequireStop()

	for i := 0; i < 3; i++ {
		err := client.Enqueue(ctx, taskmq.NewTask("noop", []byte(fmt.Sprintf("%d", i)), taskmq.TaskOptions{Queue: queueName}))
		require.NoError(t, err, "enqueue %d", i)
	}
	// Allow worker to pick up at least one into PEL; remaining stay pending in stream.
	time.Sleep(100 * time.Millisecond)

	err := client.Enqueue(ctx, taskmq.NewTask("noop", []byte("overflow"), taskmq.TaskOptions{Queue: queueName}))
	require.Error(t, err)
	assert.True(t, errors.Is(err, taskmq.ErrQueueFull))
	assert.GreaterOrEqual(t, lc.Metrics().EnqueueRejectedTotal.Load(), int64(1))

	n, err := rdb.XLen(ctx, streamKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
}

func TestTaskMQ_Lifecycle_DelayedPromoteBackpressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "lifecycle_promote_bp_q"
	streamKey := taskmq.StreamKey(queueName)
	delayedKey := taskmq.DelayedKey(queueName)
	block := make(chan struct{})

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				return lifecycleTestConfig(func(c *config.Config) {
					c.TaskMQ.Queues = []config.QueueConfig{{Name: queueName, Concurrency: 1}}
					c.TaskMQ.Lifecycle.EnqueueHardLimit = 2
					c.TaskMQ.SchedulerPollInterval = 50 * time.Millisecond
					off := false
					c.TaskMQ.Lifecycle.SafeTrimEnabled = &off
				})
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(cfg *config.Config) *taskmq.Lifecycle {
				return taskmq.NewLifecycle(taskmq.LifecycleFromConfig(cfg))
			},
			func(rdb *goredis.Client, lifecycle *taskmq.Lifecycle) taskmq.Client {
				return taskmq.NewClient(rdb, taskmq.WithClientLifecycle(lifecycle), taskmq.WithClientCodec(taskmq.JSONCodec{}))
			},
			func(rdb *goredis.Client, logger *zap.Logger, lifecycle *taskmq.Lifecycle) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithGroup("lc-promo-g"),
					taskmq.WithConsumer("lc-promo-c"),
					taskmq.WithConcurrency(1),
					taskmq.WithLifecycle(lifecycle),
					taskmq.WithSchedulerPollInterval(50*time.Millisecond),
					taskmq.WithCodec(taskmq.JSONCodec{}),
				)
				pool.Register("later", func(ctx context.Context, task *taskmq.Task) error {
					select {
					case <-block:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	require.NoError(t, rdb.Del(ctx, streamKey, delayedKey).Err())
	defer func() {
		close(block)
		_ = rdb.Del(ctx, streamKey, delayedKey)
	}()

	app.RequireStart()
	defer app.RequireStop()

	// Fill stream to hard limit; blocking handler keeps entries in stream/PEL.
	for i := 0; i < 2; i++ {
		require.NoError(t, client.Enqueue(ctx, taskmq.NewTask("later", []byte("s"), taskmq.TaskOptions{Queue: queueName})))
	}
	// Ready delayed task should NOT promote while stream is full
	require.NoError(t, client.EnqueueAt(ctx, taskmq.NewTask("later", []byte("d"), taskmq.TaskOptions{Queue: queueName}), time.Now().Add(-time.Second)))

	// Wait for scheduler ticks
	time.Sleep(250 * time.Millisecond)

	xlen, err := rdb.XLen(ctx, streamKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), xlen, "stream must stay at hard limit")

	zcard, err := rdb.ZCard(ctx, delayedKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), zcard, "delayed task must remain until stream has room")
}

func TestTaskMQ_Lifecycle_DelayedMaxCountAndDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "lifecycle_delayed_cap_q"
	delayedKey := taskmq.DelayedKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				return lifecycleTestConfig(func(c *config.Config) {
					c.TaskMQ.Queues = []config.QueueConfig{{Name: queueName, Concurrency: 1}}
					c.TaskMQ.Lifecycle.DelayedMaxCount = 2
					c.TaskMQ.Lifecycle.DelayedMaxDelay = time.Hour
					c.TaskMQ.Lifecycle.DelayedOverflow = "reject"
					off := false
					c.TaskMQ.Lifecycle.SafeTrimEnabled = &off
				})
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(cfg *config.Config) *taskmq.Lifecycle {
				return taskmq.NewLifecycle(taskmq.LifecycleFromConfig(cfg))
			},
			func(rdb *goredis.Client, lifecycle *taskmq.Lifecycle) taskmq.Client {
				return taskmq.NewClient(rdb, taskmq.WithClientLifecycle(lifecycle))
			},
			func(rdb *goredis.Client, logger *zap.Logger, lifecycle *taskmq.Lifecycle) taskmq.Worker {
				return taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithGroup("lc-del-g"),
					taskmq.WithConsumer("lc-del-c"),
					taskmq.WithConcurrency(1),
					taskmq.WithLifecycle(lifecycle),
				)
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	require.NoError(t, rdb.Del(ctx, delayedKey).Err())
	defer rdb.Del(ctx, delayedKey)

	app.RequireStart()
	defer app.RequireStop()

	err := client.EnqueueAt(ctx, taskmq.NewTask("x", []byte("1"), taskmq.TaskOptions{Queue: queueName}), time.Now().Add(48*time.Hour))
	require.Error(t, err)
	assert.True(t, errors.Is(err, taskmq.ErrDelayTooFar))

	require.NoError(t, client.EnqueueIn(ctx, taskmq.NewTask("x", []byte("1"), taskmq.TaskOptions{Queue: queueName}), 10*time.Minute))
	require.NoError(t, client.EnqueueIn(ctx, taskmq.NewTask("x", []byte("2"), taskmq.TaskOptions{Queue: queueName}), 20*time.Minute))
	err = client.EnqueueIn(ctx, taskmq.NewTask("x", []byte("3"), taskmq.TaskOptions{Queue: queueName}), 30*time.Minute)
	require.Error(t, err)
	assert.True(t, errors.Is(err, taskmq.ErrDelayedFull))

	n, err := rdb.ZCard(ctx, delayedKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestTaskMQ_Lifecycle_DLQMaxCountViaWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueName := "lifecycle_dlq_cap_q"
	streamKey := taskmq.StreamKey(queueName)
	dlqKey := taskmq.DLQKey(queueName)
	dlqIndexKey := taskmq.DLQIndexKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client
	var lc *taskmq.Lifecycle
	var done int64

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				return lifecycleTestConfig(func(c *config.Config) {
					c.TaskMQ.Queues = []config.QueueConfig{{Name: queueName, Concurrency: 1}}
					cap := int64(2)
					c.TaskMQ.Lifecycle.DLQMaxCount = &cap
					off := false
					c.TaskMQ.Lifecycle.SafeTrimEnabled = &off
				})
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(cfg *config.Config) *taskmq.Lifecycle {
				return taskmq.NewLifecycle(taskmq.LifecycleFromConfig(cfg))
			},
			func(rdb *goredis.Client, lifecycle *taskmq.Lifecycle) taskmq.Client {
				return taskmq.NewClient(rdb, taskmq.WithClientLifecycle(lifecycle))
			},
			func(rdb *goredis.Client, logger *zap.Logger, lifecycle *taskmq.Lifecycle) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithGroup("lc-dlq-g"),
					taskmq.WithConsumer("lc-dlq-c"),
					taskmq.WithConcurrency(1),
					taskmq.WithLifecycle(lifecycle),
					taskmq.WithCodec(taskmq.JSONCodec{}),
				)
				pool.Register("always_fail", func(ctx context.Context, task *taskmq.Task) error {
					atomic.AddInt64(&done, 1)
					return errors.New("force dlq")
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &lc),
	)

	require.NoError(t, rdb.Del(ctx, streamKey, dlqKey, dlqIndexKey).Err())
	defer rdb.Del(ctx, streamKey, dlqKey, dlqIndexKey)

	app.RequireStart()
	defer app.RequireStop()

	// MaxRetry=0 → first failure goes to DLQ immediately.
	for i := 0; i < 4; i++ {
		task := taskmq.NewTask("always_fail", []byte("x"), taskmq.TaskOptions{
			Queue:    queueName,
			MaxRetry: taskmq.Ptr(0),
		})
		require.NoError(t, client.Enqueue(ctx, task))
	}

	require.Eventually(t, func() bool {
		n, err := rdb.ZCard(ctx, dlqKey).Result()
		return err == nil && n == 2 && atomic.LoadInt64(&done) >= 4
	}, 8*time.Second, 50*time.Millisecond, "DLQ should cap at 2 after 4 failures")

	listed, err := client.ListDeadLetters(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, listed, 2)

	// Index must stay consistent with ZSET members.
	ids, err := rdb.ZRange(ctx, dlqKey, 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, ids, 2)
	for _, id := range ids {
		ok, err := rdb.HExists(ctx, dlqIndexKey, id).Result()
		require.NoError(t, err)
		assert.Truef(t, ok, "missing DLQ index for %s; metrics=%v", id, lc.Metrics().Snapshot())
	}
	assert.GreaterOrEqual(t, lc.Metrics().DLQEvictedTotal.Load(), int64(1))
}

func TestTaskMQ_Lifecycle_MaxPayloadRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "lifecycle_payload_q"
	var client taskmq.Client
	var rdb *goredis.Client

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				return lifecycleTestConfig(func(c *config.Config) {
					c.TaskMQ.Queues = []config.QueueConfig{{Name: queueName, Concurrency: 1}}
					c.TaskMQ.Lifecycle.MaxPayloadBytes = 8
					off := false
					c.TaskMQ.Lifecycle.SafeTrimEnabled = &off
				})
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(cfg *config.Config) *taskmq.Lifecycle {
				return taskmq.NewLifecycle(taskmq.LifecycleFromConfig(cfg))
			},
			func(rdb *goredis.Client, lifecycle *taskmq.Lifecycle) taskmq.Client {
				return taskmq.NewClient(rdb, taskmq.WithClientLifecycle(lifecycle))
			},
			func(rdb *goredis.Client, logger *zap.Logger, lifecycle *taskmq.Lifecycle) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithGroup("lc-pay-g"),
					taskmq.WithConsumer("lc-pay-c"),
					taskmq.WithConcurrency(1),
					taskmq.WithLifecycle(lifecycle),
				)
				pool.Register("x", func(ctx context.Context, task *taskmq.Task) error { return nil })
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&client, &rdb),
	)

	_ = rdb.Del(ctx, taskmq.StreamKey(queueName))
	app.RequireStart()
	defer app.RequireStop()

	err := client.Enqueue(ctx, taskmq.NewTask("x", []byte("123456789"), taskmq.TaskOptions{Queue: queueName}))
	require.Error(t, err)
	assert.True(t, errors.Is(err, taskmq.ErrPayloadTooLarge))

	require.NoError(t, client.Enqueue(ctx, taskmq.NewTask("x", []byte("12345678"), taskmq.TaskOptions{Queue: queueName})))
}

func TestTaskMQ_Lifecycle_CancelledDelayedPurged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "lifecycle_cancel_delayed_q"
	delayedKey := taskmq.DelayedKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				return lifecycleTestConfig(func(c *config.Config) {
					c.TaskMQ.Queues = []config.QueueConfig{{Name: queueName, Concurrency: 1}}
					on := true
					off := false
					c.TaskMQ.Lifecycle.PurgeCancelledDelayed = &on
					c.TaskMQ.Lifecycle.SafeTrimEnabled = &off
					c.TaskMQ.Lifecycle.SafeTrimInterval = 50 * time.Millisecond
					// Retention janitor runs when SafeTrim OR purge cancelled — purge needs janitor.
					// Enable SafeTrim with long batch but keep purge path via SafeTrimInterval.
					// Actually PurgeCancelledDelayed alone enables janitor (see Run condition).
				})
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(cfg *config.Config) *taskmq.Lifecycle {
				// Force short interval for purge-only mode
				lcCfg := taskmq.LifecycleFromConfig(cfg)
				lcCfg.SafeTrimInterval = 50 * time.Millisecond
				lcCfg.PurgeCancelledDelayed = true
				lcCfg.SafeTrimEnabled = false
				return taskmq.NewLifecycle(lcCfg)
			},
			func(rdb *goredis.Client, lifecycle *taskmq.Lifecycle) taskmq.Client {
				return taskmq.NewClient(rdb, taskmq.WithClientLifecycle(lifecycle))
			},
			func(rdb *goredis.Client, logger *zap.Logger, lifecycle *taskmq.Lifecycle) taskmq.Worker {
				return taskmq.NewWorkerPool(rdb, logger, queueName,
					taskmq.WithGroup("lc-cd-g"),
					taskmq.WithConsumer("lc-cd-c"),
					taskmq.WithConcurrency(1),
					taskmq.WithLifecycle(lifecycle),
				)
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	require.NoError(t, rdb.Del(ctx, delayedKey).Err())
	defer rdb.Del(ctx, delayedKey)

	app.RequireStart()
	defer app.RequireStop()

	task := taskmq.NewTask("future", []byte("x"), taskmq.TaskOptions{Queue: queueName, ID: "cancel-me"})
	task.ID = "cancel-me"
	require.NoError(t, client.EnqueueIn(ctx, task, 2*time.Hour))
	require.NoError(t, client.CancelTask(ctx, queueName, "cancel-me"))

	require.Eventually(t, func() bool {
		n, err := rdb.ZCard(ctx, delayedKey).Result()
		return err == nil && n == 0
	}, 5*time.Second, 50*time.Millisecond, "cancelled delayed task should be purged by retention janitor")
}

func TestTaskMQ_Lifecycle_ModuleSharedLifecycle(t *testing.T) {
	// Ensure taskmq.Module wires a single Lifecycle into Client.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "lifecycle_module_q"
	var client taskmq.Client
	var lc *taskmq.Lifecycle
	var rdb *goredis.Client

	app := fxtest.New(t,
		fx.Provide(
			func() *config.Config {
				return lifecycleTestConfig(func(c *config.Config) {
					c.Server.GRPCPort = ":50191"
					c.TaskMQ.Queues = []config.QueueConfig{{Name: queueName, Concurrency: 1}}
					c.TaskMQ.Lifecycle.EnqueueHardLimit = 1
					off := false
					c.TaskMQ.Lifecycle.SafeTrimEnabled = &off
				})
			},
			logger.NewLogger,
			internalredis.NewRedisClient,
		),
		taskmq.Module,
		fx.Populate(&client, &lc, &rdb),
	)

	require.NoError(t, rdb.Del(ctx, taskmq.StreamKey(queueName)).Err())
	app.RequireStart()
	defer app.RequireStop()

	require.NotNil(t, lc)

	// Pre-fill stream without going through worker consumption race: use hard limit 1.
	// Pause first so the single entry is not completed before second enqueue.
	require.NoError(t, client.Pause(ctx, queueName))
	time.Sleep(80 * time.Millisecond)

	require.NoError(t, client.Enqueue(ctx, taskmq.NewTask("x", []byte("1"), taskmq.TaskOptions{Queue: queueName})))
	err := client.Enqueue(ctx, taskmq.NewTask("x", []byte("2"), taskmq.TaskOptions{Queue: queueName}))
	require.Error(t, err)
	assert.True(t, errors.Is(err, taskmq.ErrQueueFull))
	assert.GreaterOrEqual(t, lc.Metrics().EnqueueRejectedTotal.Load(), int64(1))
}
