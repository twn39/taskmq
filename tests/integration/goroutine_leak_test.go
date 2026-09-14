package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	internalconfig "github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

// TestTaskMQ_GoroutineLeak_IdleWorkerPool asserts that an idle WorkerPool
// with all background daemons (Scheduler, Janitor, Retention, Cron, 5 Consumers, PubSub subscribers)
// cleans up every single spawned goroutine on w.Stop().
func TestTaskMQ_GoroutineLeak_IdleWorkerPool(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	defer SetupLeakDetector(t)()

	queue := UniqueQueue(t, "leak_idle")
	group := "leak_idle_grp"
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	w := NewTestWorkerPool(rdb, queue, group, "idle-consumer", 5, lc)
	w.Register("ping", func(c context.Context, t *taskmodel.Task) error { return nil })

	require.NoError(t, w.Start(ctx))
	time.Sleep(100 * time.Millisecond) // Let all loops initialize and run
	w.Stop(ctx)
}

// TestTaskMQ_GoroutineLeak_HighThroughputTasksDrained asserts that after a WorkerPool
// processes a burst of 50 concurrent tasks, all dynamic execution goroutines,
// semaphores, and worker pool loops are 100% destroyed on w.Stop().
func TestTaskMQ_GoroutineLeak_HighThroughputTasksDrained(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	defer SetupLeakDetector(t)()

	queue := UniqueQueue(t, "leak_load")
	group := "leak_load_grp"
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	cli := NewTestClient(rdb, lc)

	// Pre-create consumer group
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, keys.KeysFor(queue).Stream(), group, "0").Err())

	const taskCount = 50
	for i := 0; i < taskCount; i++ {
		tsk := &taskmodel.Task{
			ID:       fmt.Sprintf("leak-task-%d", i),
			Queue:    queue,
			Name:     "work.item",
			MaxRetry: 1,
			Payload:  []byte(`{"status":"ok"}`),
		}
		require.NoError(t, cli.Enqueue(ctx, tsk))
	}

	var completed atomic.Int64
	doneCh := make(chan struct{})

	w := NewTestWorkerPool(rdb, queue, group, "load-consumer", 10, lc)
	w.Register("work.item", func(c context.Context, t *taskmodel.Task) error {
		if completed.Add(1) == int64(taskCount) {
			close(doneCh)
		}
		return nil
	})

	require.NoError(t, w.Start(ctx))

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for 50 tasks to complete")
	}

	w.Stop(ctx)
}

// TestTaskMQ_GoroutineLeak_MultiQueuePriorityWorker asserts that a PriorityWorker
// coordinating multiple queues with weighted priority round-robin and per-queue
// control loops cleans up all goroutines on Stop().
func TestTaskMQ_GoroutineLeak_MultiQueuePriorityWorker(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	defer SetupLeakDetector(t)()

	qHigh := UniqueQueue(t, "leak_pq_high")
	qMed := UniqueQueue(t, "leak_pq_med")
	qLow := UniqueQueue(t, "leak_pq_low")
	group := "leak_pq_grp"

	for _, q := range []string{qHigh, qMed, qLow} {
		FlushQueue(ctx, rdb, q)
		defer FlushQueue(ctx, rdb, q)
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, keys.KeysFor(q).Stream(), group, "0").Err())
	}

	lc := DefaultTestLifecycle()
	pw := mqworker.NewPriorityWorker(
		rdb,
		zap.NewNop(),
		mqworker.WithPriorityQueues([]mqworker.QueuePriority{
			{Name: qHigh, Weight: 5},
			{Name: qMed, Weight: 3},
			{Name: qLow, Weight: 1},
		}),
		mqworker.WithGroup(group),
		mqworker.WithConsumer("pq-leak-consumer"),
		mqworker.WithConcurrency(3),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithSchedulerPollInterval(10*time.Millisecond),
		mqworker.WithJanitorInterval(50*time.Millisecond),
	)

	pw.Register("job", func(c context.Context, t *taskmodel.Task) error { return nil })

	require.NoError(t, pw.Start(ctx))
	time.Sleep(100 * time.Millisecond)
	pw.Stop(ctx)
}

// TestTaskMQ_GoroutineLeak_DecoupledDaemonSet asserts that decoupled background daemons
// (DaemonSet: Scheduler, Janitor, Retention, Cron) started under role "daemon"
// cleanly terminate all goroutines when their parent context is cancelled.
func TestTaskMQ_GoroutineLeak_DecoupledDaemonSet(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	defer SetupLeakDetector(t)()

	queue := UniqueQueue(t, "leak_daemon")
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	ds := runner.NewDaemonSet(zap.NewNop())
	runner.BuildQueueDaemons(ds, rdb, zap.NewNop(), codec.JSONCodec{}, lc, runner.QueueDaemonConfig{
		Queue:             queue,
		Group:             "daemon_grp",
		Concurrency:       2,
		SchedulerInterval: 10 * time.Millisecond,
		JanitorInterval:   50 * time.Millisecond,
	})

	daemonCtx, daemonCancel := context.WithCancel(ctx)
	stopDone := make(chan struct{})

	go func() {
		_ = ds.Run(daemonCtx)
		close(stopDone)
	}()

	time.Sleep(100 * time.Millisecond) // Let all daemon loops initialize and tick
	daemonCancel()                     // Signal shutdown

	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DaemonSet to exit")
	}
}

// TestTaskMQ_GoroutineLeak_PausedQueueAndCancelledMarker asserts that when consumers
// are blocked on a paused queue channel, or handlers are running with a cancel poller,
// stopping the worker unblocks all select cases and causes zero goroutine leakage.
func TestTaskMQ_GoroutineLeak_PausedQueueAndCancelledMarker(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	defer SetupLeakDetector(t)()

	queue := UniqueQueue(t, "leak_pause")
	group := "leak_pause_grp"
	qk := keys.KeysFor(queue)
	streamKey := qk.Stream()
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	cli := NewTestClient(rdb, lc)

	// Pre-create stream and group
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamKey, group, "0").Err())

	// Explicitly pause the queue so consumer blocks on pause channel
	require.NoError(t, rdb.Set(ctx, qk.Paused(), "1", time.Minute).Err())

	// Enqueue a task while queue is paused
	tsk := &taskmodel.Task{
		ID:       "paused-task-1",
		Queue:    queue,
		Name:     "paused.action",
		MaxRetry: 1,
	}
	require.NoError(t, cli.Enqueue(ctx, tsk))

	w := NewTestWorkerPool(rdb, queue, group, "pause-consumer", 2, lc)
	w.Register("paused.action", func(c context.Context, t *taskmodel.Task) error { return nil })

	require.NoError(t, w.Start(ctx))
	time.Sleep(150 * time.Millisecond) // Consumers will read message and block on <-pauseCh

	// Stopping worker should cleanly cancel consumerCtx and break out of <-pauseCh
	w.Stop(ctx)
}

// TestTaskMQ_GoroutineLeak_UberFxLifecycleApp asserts that when the full application
// is assembled via Uber Fx (Client + WorkerPool + Lifecycle hooks), calling
// app.RequireStart() and app.RequireStop() leaves zero leaked goroutines.
func TestTaskMQ_GoroutineLeak_UberFxLifecycleApp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	defer SetupLeakDetector(t)()

	queueName := UniqueQueue(t, "leak_fx")
	cfg := NewTestConfig()

	var rdb redis.UniversalClient
	var client mqclient.Client

	app := fxtest.New(t,
		fx.Supply(cfg),
		fx.Provide(
			func(c *internalconfig.Config) internalconfig.RedisConfig { return c.Redis },
			func(c *internalconfig.Config) internalconfig.LoggerConfig { return c.Logger },
			logger.NewLogger,
			internalredis.NewRedisClient,
			ProvideSharedLifecycle,
			ProvideClientWithLifecycle,
			func(rdb redis.UniversalClient, l *zap.Logger, lc *lifecycle.Lifecycle) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, l, queueName,
					mqworker.WithGroup("fx-leak-group"),
					mqworker.WithConsumer("fx-leak-consumer"),
					mqworker.WithConcurrency(2),
					mqworker.WithLifecycle(lc),
				)
				pool.Register("fx.ping", func(ctx context.Context, task *taskmodel.Task) error { return nil })
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	// Clean queue
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	FlushQueue(ctx, rdb, queueName)
	defer FlushQueue(ctx, rdb, queueName)

	app.RequireStart()

	// Enqueue a quick task
	_ = client.Enqueue(ctx, taskmodel.NewTask("fx.ping", []byte(`{}`), taskmodel.TaskOptions{Queue: queueName}))
	time.Sleep(100 * time.Millisecond)

	app.RequireStop()
}
