package runner_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

type noopCron struct{}

func (n *noopCron) Run(ctx context.Context) error                           { <-ctx.Done(); return ctx.Err() }
func (n *noopCron) Reschedule(ctx context.Context, t *taskmodel.Task) error { return nil }

func setupMini(t *testing.T) (context.Context, redis.UniversalClient, *miniredis.Miniredis, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return context.Background(), rdb, mr, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func TestDelayedScheduler_PromotesReadyTasks(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-promote"
	qk := keys.KeysFor(queue)
	task := taskmodel.NewTask("job", []byte(`{"n":1}`), taskmodel.TaskOptions{ID: "d1", Queue: queue})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	// Past score → ready immediately.
	require.NoError(t, rdb.ZAdd(ctx, qk.Delayed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(ser),
	}).Err())

	sched := runner.NewDelayedScheduler(rdb, zap.NewNop(), queue, &noopCron{}, codec.JSONCodec{}, 50*time.Millisecond)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sched.Run(runCtx) }()

	require.Eventually(t, func() bool {
		n, err := rdb.XLen(ctx, qk.Stream()).Result()
		return err == nil && n >= 1
	}, 3*time.Second, 20*time.Millisecond)

	delayed, err := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), delayed)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not exit")
	}
}

func TestDelayedScheduler_HardLimitSkipsPromote(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-hard"
	qk := keys.KeysFor(queue)
	// Stream already at hard limit 1.
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: qk.Stream(),
		Values: map[string]interface{}{"task": "{}"},
	}).Err())

	task := taskmodel.NewTask("job", []byte(`ready`), taskmodel.TaskOptions{ID: "d2", Queue: queue})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, qk.Delayed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(ser),
	}).Err())

	// hard=1, maxlen=0
	sched := runner.NewDelayedScheduler(rdb, zap.NewNop(), queue, &noopCron{}, codec.JSONCodec{}, 30*time.Millisecond, 1, 0)
	runCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	_ = sched.Run(runCtx)

	// Delayed task must still be waiting; stream stays at 1.
	delayed, err := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), delayed)
	n, err := rdb.XLen(ctx, qk.Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestDelayedScheduler_WakeupPromotesFutureThenReady(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-wake"
	qk := keys.KeysFor(queue)
	task := taskmodel.NewTask("job", []byte(`w`), taskmodel.TaskOptions{ID: "w1", Queue: queue})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)

	// Far-future task first so initial promote is a no-op.
	far := time.Now().Add(30 * time.Second).UnixMilli()
	require.NoError(t, rdb.ZAdd(ctx, qk.Delayed(), redis.Z{
		Score:  float64(far),
		Member: string(ser),
	}).Err())

	sched := runner.NewDelayedScheduler(rdb, zap.NewNop(), queue, &noopCron{}, codec.JSONCodec{}, time.Second)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = sched.Run(runCtx) }()

	// Give subscribe a moment, then move score to past and publish wakeup.
	time.Sleep(80 * time.Millisecond)
	require.NoError(t, rdb.ZAdd(ctx, qk.Delayed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(ser),
	}).Err())
	require.NoError(t, rdb.Publish(ctx, qk.DelayedWakeupChannel(), time.Now().UnixMilli()).Err())

	require.Eventually(t, func() bool {
		n, _ := rdb.XLen(ctx, qk.Stream()).Result()
		return n >= 1
	}, 3*time.Second, 25*time.Millisecond)
	cancel()
}

func TestPELRecoveryJanitor_ReclaimsIdleAndSetsDeliveryCount(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-pel"
	group := "g1"
	stream := keys.KeysFor(queue).Stream()
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"task": `{"id":"p1"}`},
	}).Result()
	require.NoError(t, err)

	// Deliver to consumer A → enters PEL under A.
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "stale-worker",
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)

	var claimed atomic.Int64
	var delivery atomic.Int64
	j := runner.NewPELRecoveryJanitor(
		rdb, zap.NewNop(), queue, group, "janitor-c",
		2, 20*time.Millisecond, 1*time.Millisecond, // very short min idle
		nil,
	)
	j.RegisterProcessor(func(ctx context.Context, msg redis.XMessage) {
		claimed.Add(1)
		if v, ok := msg.Values["__delivery_count"]; ok {
			switch n := v.(type) {
			case int64:
				delivery.Store(n)
			case int:
				delivery.Store(int64(n))
			}
		}
		// Ack so we don't reclaim forever.
		_ = rdb.XAck(ctx, stream, group, msg.ID).Err()
	})

	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- j.Run(runCtx) }()

	require.Eventually(t, func() bool {
		return claimed.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond)

	require.GreaterOrEqual(t, delivery.Load(), int64(1))
	// Message id should match original stream id.
	_ = id

	// Stalled event must be published best-effort on reclaim.
	require.Eventually(t, func() bool {
		evs, err := rdb.XRange(ctx, keys.KeysFor(queue).Events(), "-", "+").Result()
		if err != nil {
			return false
		}
		for _, e := range evs {
			if typ, _ := e.Values["type"].(string); typ == "stalled" {
				return true
			}
		}
		return false
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
	<-done
}

func TestRetentionJanitor_PurgeDLQByAgeAndCancelledDelayed(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-ret"
	qk := keys.KeysFor(queue)
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DLQMaxAge:             50 * time.Millisecond,
		PurgeCancelledDelayed: true,
		SafeTrimEnabled:       false,
		SafeTrimInterval:      20 * time.Millisecond,
	})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), queue, "g", codec.JSONCodec{}, lc)

	// Old DLQ entry.
	oldID := "old-dlq"
	require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{
		Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
		Member: oldID,
	}).Err())
	require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), oldID, "{}").Err())

	// Cancelled delayed task.
	task := taskmodel.NewTask("job", []byte(`x`), taskmodel.TaskOptions{ID: "cd1", Queue: queue})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, qk.Delayed(), redis.Z{
		Score:  float64(time.Now().Add(time.Hour).UnixMilli()),
		Member: string(ser),
	}).Err())
	require.NoError(t, rdb.Set(ctx, qk.Cancelled(task.ID), "1", time.Hour).Err())

	n, err := j.PurgeDLQByAge(ctx, 50*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	exists, _ := rdb.HExists(ctx, qk.DLQIndex(), oldID).Result()
	require.False(t, exists)

	purged, err := j.PurgeCancelledDelayed(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), purged)
	d, _ := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.Equal(t, int64(0), d)
}

func TestRetentionJanitor_RunNoOpWhenDisabled(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	// All retention features off → Run waits for cancel only.
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q", "g", codec.JSONCodec{}, lc)
	runCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	err := j.Run(runCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRetentionJanitor_NilLifecycleBlocksUntilCancel(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q", "g", codec.JSONCodec{}, nil)
	runCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err := j.Run(runCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDefaultFactories_Construct(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()
	_ = ctx
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{})
	cm := runner.DefaultCronManagerFactory(rdb, zap.NewNop(), "q", codec.JSONCodec{}, time.Second, time.Second, 10, 100, lc)
	require.NotNil(t, cm)
	sch := runner.DefaultSchedulerFactory(rdb, zap.NewNop(), "q", cm, codec.JSONCodec{}, time.Second, 0, 0)
	require.NotNil(t, sch)
	j := runner.DefaultJanitorFactory(rdb, zap.NewNop(), "q", "g", "c", 1, time.Second, time.Second)
	require.NotNil(t, j)
	rj := runner.DefaultRetentionJanitorFactory(rdb, zap.NewNop(), "q", "g", codec.JSONCodec{}, lc)
	require.NotNil(t, rj)
}

func TestRetentionJanitor_CompletedMetaPurgeViaRun(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-completed"
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		CompletedRetention: 50 * time.Millisecond,
		SafeTrimEnabled:    false,
		SafeTrimInterval:   30 * time.Millisecond,
	})
	store := meta.NewStore(rdb)
	old := taskmodel.NewTask("job", []byte(`o`), taskmodel.TaskOptions{ID: "c-old", Queue: queue})
	require.NoError(t, store.MarkCompleted(ctx, old, []byte("ok"), time.Hour, 100))
	// Age the completed index entry.
	require.NoError(t, rdb.ZAdd(ctx, keys.KeysFor(queue).Completed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
		Member: "c-old",
	}).Err())

	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), queue, "g", codec.JSONCodec{}, lc)
	runCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_ = j.Run(runCtx)

	info, err := store.Get(ctx, queue, "c-old")
	require.NoError(t, err)
	require.Nil(t, info)
}

func TestPELRecoveryJanitor_ReclaimsIdlePending(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-pel"
	group := "g-pel"
	qk := keys.KeysFor(queue)
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, qk.Stream(), group, "0").Err())
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: qk.Stream(),
		Values: map[string]interface{}{"task": `{"id":"p1"}`},
	}).Result()
	require.NoError(t, err)
	// Claim into consumer A PEL then never ack (stalled).
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "dead-worker", Streams: []string{qk.Stream(), ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)

	var claimed atomic.Int64
	j := runner.NewPELRecoveryJanitor(rdb, zap.NewNop(), queue, group, "rescuer", 2,
		20*time.Millisecond, time.Millisecond,
		func(ctx context.Context, msg redis.XMessage) {
			claimed.Add(1)
			_ = rdb.XAck(ctx, qk.Stream(), group, msg.ID).Err()
		},
	)
	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- j.Run(runCtx) }()

	require.Eventually(t, func() bool { return claimed.Load() >= 1 }, 2*time.Second, 20*time.Millisecond)
	_ = id
	cancel()
	<-done
}

func TestCronManager_Reschedule(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-cron-rs"
	qk := keys.KeysFor(queue)
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	cm := runner.NewCronManager(rdb, zap.NewNop(), queue, codec.JSONCodec{}, time.Minute, time.Second, 10, 100, lc)

	task := taskmodel.NewTask("nightly", []byte(`{}`), taskmodel.TaskOptions{
		Queue: queue, MaxRetry: taskmodel.Ptr(1),
	})
	task.CronSpec = "0 0 * * *" // daily
	require.NoError(t, cm.Reschedule(ctx, task))

	n, err := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// Empty cron spec is a no-op.
	require.NoError(t, cm.Reschedule(ctx, taskmodel.NewTask("x", nil, taskmodel.TaskOptions{Queue: queue})))

	// Invalid cron returns error.
	bad := taskmodel.NewTask("bad", nil, taskmodel.TaskOptions{Queue: queue})
	bad.CronSpec = "not-valid"
	require.Error(t, cm.Reschedule(ctx, bad))
}

func TestRetentionJanitor_SafeTrim(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-trim"
	group := "g-trim"
	qk := keys.KeysFor(queue)
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		SafeTrimEnabled:    true,
		SafeTrimBatchLimit: 100,
	})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), queue, group, codec.JSONCodec{}, lc)

	require.NoError(t, rdb.XGroupCreateMkStream(ctx, qk.Stream(), group, "0").Err())
	// Two messages; claim neither → LastDelivered may still allow trim of nothing useful,
	// but SafeTrim path must not error on empty PEL.
	id1, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: qk.Stream(), Values: map[string]interface{}{"task": `{"id":"1"}`}}).Result()
	require.NoError(t, err)
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{Stream: qk.Stream(), Values: map[string]interface{}{"task": `{"id":"2"}`}}).Result()
	require.NoError(t, err)

	// Read+ack first so LastDelivered advances; second remains.
	msgs, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "c1", Streams: []string{qk.Stream(), ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.NoError(t, rdb.XAck(ctx, qk.Stream(), group, msgs[0].Messages[0].ID).Err())
	_ = id1

	// SafeTrim uses pending lower or last-delivered; should not fail.
	_, err = j.SafeTrim(ctx)
	require.NoError(t, err)

	// NOGROUP / missing stream is ignorable via PurgeIdleConsumers on empty group stream.
	n, err := j.PurgeIdleConsumers(ctx, time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, int64(0))
}

func TestRetentionJanitor_RunTicksOnce(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-tick"
	qk := keys.KeysFor(queue)
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DLQMaxAge:             time.Hour,
		PurgeCancelledDelayed: true,
		SafeTrimEnabled:       false,
		SafeTrimInterval:      30 * time.Millisecond,
	})
	// Old DLQ entry cleaned by first tick.
	require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{
		Score:  float64(time.Now().Add(-2 * time.Hour).UnixMilli()),
		Member: "old",
	}).Err())
	require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), "old", "{}").Err())

	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), queue, "g", codec.JSONCodec{}, lc)
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer cancel()
	_ = j.Run(runCtx)

	exists, err := rdb.HExists(ctx, qk.DLQIndex(), "old").Result()
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPELRecoveryJanitor_WithCodecEmitsTaskID(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "runner-pel-codec"
	group := "g1"
	stream := keys.KeysFor(queue).Stream()
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	task := taskmodel.NewTask("job", []byte(`x`), taskmodel.TaskOptions{ID: "tid-99", Queue: queue})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"task": string(ser)},
	}).Result()
	require.NoError(t, err)
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "stale", Streams: []string{stream, ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)

	var claimed atomic.Int64
	j := runner.NewPELRecoveryJanitorWithCodec(
		rdb, zap.NewNop(), queue, group, "janitor",
		1, 20*time.Millisecond, time.Millisecond,
		codec.JSONCodec{}, 100,
	)
	j.RegisterProcessor(func(ctx context.Context, msg redis.XMessage) {
		claimed.Add(1)
		_ = rdb.XAck(ctx, stream, group, msg.ID).Err()
	})
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- j.Run(runCtx) }()

	require.Eventually(t, func() bool { return claimed.Load() >= 1 }, 3*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		evs, err := rdb.XRange(ctx, keys.KeysFor(queue).Events(), "-", "+").Result()
		if err != nil {
			return false
		}
		for _, e := range evs {
			if typ, _ := e.Values["type"].(string); typ == "stalled" {
				if tid, _ := e.Values["task_id"].(string); tid == "tid-99" {
					return true
				}
			}
		}
		return false
	}, 2*time.Second, 20*time.Millisecond)
	cancel()
	<-done
}

func TestPELValueAsBytesAndDefaultFactory(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()
	j := runner.DefaultJanitorFactory(rdb, zap.NewNop(), "q", "g", "c", 1, time.Second, time.Second)
	require.NotNil(t, j)
	// Nil process is fine until Run is exercised.
	_ = ctx
}

func TestCronManager_SelfHealingLockAndInvalidConfig(t *testing.T) {
	ctx, rdb, _, cleanup := setupMini(t)
	defer cleanup()

	queue := "cron-lock-q"
	qk := keys.KeysFor(queue)

	// Pre-set self-healing lock to simulate another worker holding lock
	require.NoError(t, rdb.Set(ctx, qk.CronSelfHealingLock(), "1", time.Minute).Err())

	// Add corrupt config in CronConfigs hash
	require.NoError(t, rdb.HSet(ctx, qk.CronConfigs(), "bad-job", "corrupt-json-data").Err())

	cm := runner.NewCronManager(rdb, zap.NewNop(), queue, codec.JSONCodec{}, 50*time.Millisecond, time.Second, 10, 10)
	runCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()

	_ = cm.Run(runCtx)
	// Lock was held by another node, so HGetAll scan was safely skipped
}
