package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

// Helper to create a go-redis client connected through the chaos proxy
func newProxyRedisClient(proxy *TCPChaosProxy) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         proxy.Addr(),
		DialTimeout:  1 * time.Second,
		ReadTimeout:  3 * time.Second, // Bounded above XReadGroup Block(1s)
		WriteTimeout: 2 * time.Second,
		PoolTimeout:  2 * time.Second,
	})
}

// 1. TestChaos_ProducerNetworkPartition: verifies producer fails fast when partition occurs,
// and transparently self-heals when network connectivity is restored.
func TestChaos_ProducerNetworkPartition(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel, directRdb := RequireRedis(t)
	defer cancel()

	proxy := StartTCPChaosProxy(t, redisTestAddr())
	defer proxy.Close()

	proxyRdb := newProxyRedisClient(proxy)
	defer proxyRdb.Close()

	queue := UniqueQueue(t, "chaos_prod")
	defer FlushQueue(ctx, directRdb, queue)

	lc := DefaultTestLifecycle()
	client := mqclient.NewClient(proxyRdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))

	// 1. Initial Enqueue through proxy succeeds
	t1 := taskmodel.NewTask("job.ok", []byte(`{"step":1}`), taskmodel.TaskOptions{ID: "t-init", Queue: queue})
	require.NoError(t, client.Enqueue(ctx, t1))

	// 2. Inject Blackhole (silent packet drop) -> Enqueue must fail fast on context timeout
	proxy.SetBlackhole(true)
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer timeoutCancel()

	t2 := taskmodel.NewTask("job.timeout", []byte(`{"step":2}`), taskmodel.TaskOptions{ID: "t-timeout", Queue: queue})
	err := client.Enqueue(timeoutCtx, t2)
	require.Error(t, err, "expected enqueue to fail during network blackhole")

	// 3. Inject Hard Cut (TCP RST)
	proxy.SetBlackhole(false)
	proxy.Cut()

	// Next operation may see broken pipe or transparently reconnect
	t3 := taskmodel.NewTask("job.cut", []byte(`{"step":3}`), taskmodel.TaskOptions{ID: "t-cut", Queue: queue})
	// Try enqueue; either transparent reconnect succeeds or next retry succeeds immediately
	err = client.Enqueue(ctx, t3)
	if err != nil {
		// If initial cut returned error on dead socket, next attempt MUST succeed
		err = client.Enqueue(ctx, t3)
		require.NoError(t, err, "producer must self-heal on subsequent attempt")
	}

	// 4. Verify tasks arrived in Redis
	qk := keys.KeysFor(queue)
	messages, err := directRdb.XRange(ctx, qk.Stream(), "-", "+").Result()
	require.NoError(t, err)
	require.NotEmpty(t, messages)
}

// 2. TestChaos_WorkerLongPollDisconnect: verifies Worker XReadGroup loop survives
// abrupt connection RST while blocked waiting for messages, and self-heals upon network restore.
func TestChaos_WorkerLongPollDisconnect(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel, directRdb := RequireRedis(t)
	defer cancel()

	proxy := StartTCPChaosProxy(t, redisTestAddr())
	defer proxy.Close()

	proxyRdb := newProxyRedisClient(proxy)
	defer proxyRdb.Close()

	queue := UniqueQueue(t, "chaos_worker_poll")
	defer FlushQueue(ctx, directRdb, queue)

	var processedCount atomic.Int32

	pool := mqworker.NewWorkerPool(proxyRdb, zap.NewNop(), queue,
		mqworker.WithGroup("chaos-cg"),
		mqworker.WithConsumer("c-chaos"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
	)
	pool.Register("job.ping", func(ctx context.Context, task *taskmodel.Task) error {
		processedCount.Add(1)
		return nil
	})

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	// Wait 200ms for worker to enter XReadGroup block state
	time.Sleep(200 * time.Millisecond)

	// Inject hard network cut while worker is blocking in XReadGroup
	proxy.Cut()

	// Keep network down for 500ms
	proxy.SetBlackhole(true)
	time.Sleep(500 * time.Millisecond)
	proxy.SetBlackhole(false)

	// Enqueue tasks via direct connection to Redis
	lc := DefaultTestLifecycle()
	directClient := mqclient.NewClient(directRdb, mqclient.WithClientLifecycle(lc))
	require.NoError(t, directClient.Enqueue(ctx, taskmodel.NewTask("job.ping", []byte(`{}`), taskmodel.TaskOptions{
		ID: "p-reconn-1", Queue: queue,
	})))

	// Worker must self-heal and consume the task
	require.Eventually(t, func() bool {
		return processedCount.Load() == 1
	}, 10*time.Second, 100*time.Millisecond, "worker failed to self-heal and process message after disconnect")
}

// 3. TestChaos_InFlightSettlementNetworkFailureAndPELReclaim:
// verifies that if Redis disconnects during task settlement (CompleteTask),
// the message safely remains in PEL (Zero Data Loss) and is recovered by the PEL Janitor upon reconnect.
func TestChaos_InFlightSettlementNetworkFailureAndPELReclaim(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel, directRdb := RequireRedis(t)
	defer cancel()

	proxy := StartTCPChaosProxy(t, redisTestAddr())
	defer proxy.Close()

	proxyRdb := newProxyRedisClient(proxy)
	defer proxyRdb.Close()

	queue := UniqueQueue(t, "chaos_pel_reclaim")
	defer FlushQueue(ctx, directRdb, queue)

	var runCount atomic.Int32
	handlerStarted := make(chan struct{})

	// 1. Worker 1 pulls the message, then crashes/stops abruptly before ACKing/settling
	w1 := mqworker.NewWorkerPool(proxyRdb, zap.NewNop(), queue,
		mqworker.WithGroup("pel-cg"),
		mqworker.WithConsumer("c-pel-1"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithDisableJanitor(), // Do not recover on w1
		mqworker.WithShutdownTimeout(10*time.Millisecond),
	)

	w1.Register("job.inflight", func(ctx context.Context, task *taskmodel.Task) error {
		runCount.Add(1)
		close(handlerStarted)
		// Simulate hang / abrupt crash by blocking until test stops w1
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})

	require.NoError(t, w1.Start(ctx))

	// Enqueue via direct Redis connection
	lc := DefaultTestLifecycle()
	directClient := mqclient.NewClient(directRdb, mqclient.WithClientLifecycle(lc))
	require.NoError(t, directClient.Enqueue(ctx, taskmodel.NewTask("job.inflight", []byte(`{}`), taskmodel.TaskOptions{
		ID: "inflight-1", Queue: queue,
	})))

	// Wait for handler to start processing on w1
	<-handlerStarted

	// Abruptly close proxy and stop w1 (simulating worker crash/partition where settlement cannot reach Redis)
	proxy.Close()
	w1.Stop(context.Background())

	// Verify message is safely preserved in PEL (Zero Data Loss)
	qk := keys.KeysFor(queue)
	xpending, err := directRdb.XPending(ctx, qk.Stream(), "pel-cg").Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, xpending.Count, int64(1), "message must safely remain in PEL when worker crashes without reaching Redis")

	// Wait 100ms so minIdleTime (50ms) elapses
	time.Sleep(100 * time.Millisecond)

	// 2. Worker 2 starts up with active PEL Janitor to recover stalled messages
	w2 := mqworker.NewWorkerPool(directRdb, zap.NewNop(), queue,
		mqworker.WithGroup("pel-cg"),
		mqworker.WithConsumer("c-pel-2"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithJanitorInterval(50*time.Millisecond),
		mqworker.WithJanitorMinIdleTime(50*time.Millisecond),
	)

	w2Done := make(chan struct{})
	w2.Register("job.inflight", func(ctx context.Context, task *taskmodel.Task) error {
		runCount.Add(1)
		close(w2Done)
		return nil
	})

	require.NoError(t, w2.Start(ctx))
	defer w2.Stop(ctx)

	// PEL Janitor must reclaim and re-execute message
	select {
	case <-w2Done:
	case <-time.After(10 * time.Second):
		t.Fatal("PEL Janitor failed to reclaim and re-execute message")
	}

	require.Equal(t, int32(2), runCount.Load())

	// Verify PEL is now completely clean (task completed and acknowledged)
	require.Eventually(t, func() bool {
		pending, err := directRdb.XPending(ctx, qk.Stream(), "pel-cg").Result()
		return err == nil && pending.Count == 0
	}, 5*time.Second, 50*time.Millisecond)
}

// 4. TestChaos_DelayedSchedulerNetworkPartition: verifies DelayedScheduler survives
// network cut while migrating due tasks from ZSET to Stream.
func TestChaos_DelayedSchedulerNetworkPartition(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel, directRdb := RequireRedis(t)
	defer cancel()

	proxy := StartTCPChaosProxy(t, redisTestAddr())
	defer proxy.Close()

	proxyRdb := newProxyRedisClient(proxy)
	defer proxyRdb.Close()

	queue := UniqueQueue(t, "chaos_delayed")
	defer FlushQueue(ctx, directRdb, queue)

	// Pre-load Lua scripts through proxy
	require.NoError(t, lifecycle.LoadScripts(ctx, proxyRdb))

	scheduler := runner.NewDelayedScheduler(
		proxyRdb,
		zap.NewNop(),
		queue,
		nil,
		codec.JSONCodec{},
		100*time.Millisecond,
	)

	sctx, scancel := context.WithCancel(ctx)
	defer scancel()

	go func() {
		_ = scheduler.Run(sctx)
	}()

	// Enqueue a delayed task due in 200ms directly to Redis ZSET
	task := taskmodel.NewTask("job.delayed", []byte(`{}`), taskmodel.TaskOptions{
		ID: "d-chaos-1", Queue: queue,
	})
	serialized, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)

	qk := keys.KeysFor(queue)
	dueAt := time.Now().Add(200 * time.Millisecond).UnixMilli()
	require.NoError(t, directRdb.ZAdd(ctx, qk.Delayed(), redis.Z{
		Score:  float64(dueAt),
		Member: serialized,
	}).Err())

	// Cut proxy right when task becomes due
	time.Sleep(150 * time.Millisecond)
	proxy.Cut()
	proxy.SetBlackhole(true)
	time.Sleep(400 * time.Millisecond)

	// Restore network
	proxy.SetBlackhole(false)

	// Scheduler must self-heal on next tick and migrate task from ZSET to Stream
	require.Eventually(t, func() bool {
		msgs, err := directRdb.XRange(ctx, qk.Stream(), "-", "+").Result()
		return err == nil && len(msgs) == 1
	}, 10*time.Second, 100*time.Millisecond, "Delayed scheduler failed to migrate task after network healing")
}

// 5. TestChaos_RedisClientKillSelfHealing: tests application recovery when Redis server
// abruptly drops client connections via CLIENT KILL.
func TestChaos_RedisClientKillSelfHealing(t *testing.T) {
	defer SetupLeakDetector(t)()

	ctx, cancel, directRdb := RequireRedis(t)
	defer cancel()

	proxy := StartTCPChaosProxy(t, redisTestAddr())
	defer proxy.Close()

	proxyRdb := newProxyRedisClient(proxy)
	defer proxyRdb.Close()

	queue := UniqueQueue(t, "chaos_client_kill")
	defer FlushQueue(ctx, directRdb, queue)

	var processed atomic.Int32

	pool := mqworker.NewWorkerPool(proxyRdb, zap.NewNop(), queue,
		mqworker.WithGroup("kill-cg"),
		mqworker.WithConsumer("c-kill"),
		mqworker.WithConcurrency(2),
		mqworker.WithCodec(codec.JSONCodec{}),
	)
	pool.Register("job.kill", func(ctx context.Context, task *taskmodel.Task) error {
		processed.Add(1)
		return nil
	})

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	lc := DefaultTestLifecycle()
	client := mqclient.NewClient(proxyRdb, mqclient.WithClientLifecycle(lc))

	// Enqueue 3 tasks
	for i := 0; i < 3; i++ {
		require.NoError(t, client.Enqueue(ctx, taskmodel.NewTask("job.kill", []byte(`{}`), taskmodel.TaskOptions{
			ID: fmt.Sprintf("k-pre-%d", i), Queue: queue,
		})))
	}

	// Wait for the initial 3 tasks to be consumed
	require.Eventually(t, func() bool {
		return processed.Load() == 3
	}, 5*time.Second, 50*time.Millisecond)

	// Abruptly kill all upstream connections of this test on the Redis server
	for _, addr := range proxy.UpstreamAddrs() {
		_ = directRdb.Do(ctx, "CLIENT", "KILL", "ADDR", addr).Err()
	}

	// Wait 100ms
	time.Sleep(100 * time.Millisecond)

	// Enqueue 3 more tasks after CLIENT KILL
	for i := 0; i < 3; i++ {
		require.NoError(t, client.Enqueue(ctx, taskmodel.NewTask("job.kill", []byte(`{}`), taskmodel.TaskOptions{
			ID: fmt.Sprintf("k-post-%d", i), Queue: queue,
		})))
	}

	// Worker and Client should completely self-heal and process all 6 tasks
	require.Eventually(t, func() bool {
		return processed.Load() == 6
	}, 10*time.Second, 100*time.Millisecond, "failed to process all tasks after Redis CLIENT KILL")
}

// 6. TestChaos_PubSubPartitionAndStateReconciliation: tests that if a Cancel signal is issued
// while network partition prevents Pub/Sub delivery, the task is still cancelled via persistent key reconciliation.
func TestChaos_PubSubPartitionAndStateReconciliation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel, directRdb := RequireRedis(t)
	defer cancel()

	proxy := StartTCPChaosProxy(t, redisTestAddr())
	defer proxy.Close()

	proxyRdb := newProxyRedisClient(proxy)
	defer proxyRdb.Close()

	queue := UniqueQueue(t, "chaos_pubsub_recon")
	defer FlushQueue(ctx, directRdb, queue)

	var wasCancelled atomic.Bool
	pool := mqworker.NewWorkerPool(proxyRdb, zap.NewNop(), queue,
		mqworker.WithGroup("pubsub-cg"),
		mqworker.WithConsumer("c-pubsub"),
		mqworker.WithConcurrency(1),
		mqworker.WithCodec(codec.JSONCodec{}),
	)

	pool.Register("job.cancelable", func(ctx context.Context, task *taskmodel.Task) error {
		select {
		case <-ctx.Done():
			wasCancelled.Store(true)
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	})

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	// 1. Cut network to Worker so Pub/Sub broadcast cannot reach it
	proxy.Cut()
	proxy.SetBlackhole(true)

	// 2. Direct client writes task and cancellation marker directly to Redis
	lc := DefaultTestLifecycle()
	directClient := mqclient.NewClient(directRdb, mqclient.WithClientLifecycle(lc))
	require.NoError(t, directClient.Enqueue(ctx, taskmodel.NewTask("job.cancelable", []byte(`{}`), taskmodel.TaskOptions{
		ID: "cancel-task-1", Queue: queue,
	})))

	// Cancel task directly in Redis (Pub/Sub notification will not reach disconnected worker)
	require.NoError(t, directClient.CancelTask(ctx, queue, "cancel-task-1"))

	// 3. Restore network proxy
	proxy.SetBlackhole(false)

	// 4. Worker pulls message; persistent key watchdog detects cancelled key and discards it atomically
	qk := keys.KeysFor(queue)
	require.Eventually(t, func() bool {
		pending, err := directRdb.XPending(ctx, qk.Stream(), "pubsub-cg").Result()
		return err == nil && pending.Count == 0
	}, 10*time.Second, 100*time.Millisecond, "Worker failed to reconcile cancellation from persistent key after Pub/Sub partition")

	require.False(t, wasCancelled.Load(), "handler should not execute for pre-cancelled task")
}

// 7. TestChaos_E2EFlappingNetworkWithZeroLeak: validates end-to-end processing under frequent
// network flapping (cut every 300ms) with at-least-once delivery and zero goroutine leaks.
func TestChaos_E2EFlappingNetworkWithZeroLeak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel, directRdb := RequireRedis(t)
	defer cancel()

	proxy := StartTCPChaosProxy(t, redisTestAddr())
	defer proxy.Close()

	proxyRdb := newProxyRedisClient(proxy)
	defer proxyRdb.Close()

	queue := UniqueQueue(t, "chaos_flapping")
	defer FlushQueue(ctx, directRdb, queue)

	const totalTasks = 30
	var completedCount atomic.Int32

	pool := mqworker.NewWorkerPool(proxyRdb, zap.NewNop(), queue,
		mqworker.WithGroup("flap-cg"),
		mqworker.WithConsumer("c-flap"),
		mqworker.WithConcurrency(4),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithJanitorInterval(300 * time.Millisecond),
		mqworker.WithJanitorMinIdleTime(500 * time.Millisecond),
	)

	pool.Register("job.flap", func(ctx context.Context, task *taskmodel.Task) error {
		completedCount.Add(1)
		return nil
	})

	require.NoError(t, pool.Start(ctx))

	// Enqueue all tasks via direct Redis
	lc := DefaultTestLifecycle()
	directClient := mqclient.NewClient(directRdb, mqclient.WithClientLifecycle(lc))
	for i := 0; i < totalTasks; i++ {
		require.NoError(t, directClient.Enqueue(ctx, taskmodel.NewTask("job.flap", []byte(`{}`), taskmodel.TaskOptions{
			ID: fmt.Sprintf("flap-%d", i), Queue: queue,
		})))
	}

	// Flapper goroutine: Cut proxy every 300ms for 3 cycles
	stopFlapper := make(chan struct{})
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		cuts := 0
		for {
			select {
			case <-stopFlapper:
				return
			case <-ticker.C:
				if cuts < 3 {
					proxy.Cut()
					cuts++
				}
			}
		}
	}()

	// Wait for all tasks to eventually complete
	require.Eventually(t, func() bool {
		return completedCount.Load() >= totalTasks
	}, 15*time.Second, 100*time.Millisecond, "Flapping network prevented at-least-once task delivery")

	close(stopFlapper)
	pool.Stop(ctx)
}
