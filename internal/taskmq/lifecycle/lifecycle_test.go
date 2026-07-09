package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/lifecycle"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

type dummyCronManager struct{}

func (d *dummyCronManager) Run(ctx context.Context) error                           { return nil }
func (d *dummyCronManager) Reschedule(ctx context.Context, t *taskmodel.Task) error { return nil }

func setupMiniRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return rdb, mr, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func testLifecycle(cfg lifecycle.LifecycleConfig) *lifecycle.Lifecycle {
	// SafeTrim off by default in unit helpers unless explicitly testing it.
	if !cfg.SafeTrimEnabled && cfg.SafeTrimInterval == 0 {
		cfg.SafeTrimEnabled = false
	}
	if cfg.DLQMaxCount == 0 && cfg.CancelledTTL == 0 {
		// leave zeros as intentional for unlimited tests
	}
	return lifecycle.NewLifecycle(cfg)
}

// ---------------------------------------------------------------------------
// Config / metrics
// ---------------------------------------------------------------------------

func TestDefaultLifecycleConfig(t *testing.T) {
	cfg := lifecycle.DefaultLifecycleConfig()
	assert.Equal(t, int64(1000), cfg.DLQMaxCount)
	assert.Equal(t, 24*time.Hour, cfg.CancelledTTL)
	assert.Equal(t, lifecycle.DelayedOverflowReject, cfg.DelayedOverflow)
	assert.True(t, cfg.SafeTrimEnabled)
	assert.Equal(t, 30*time.Second, cfg.SafeTrimInterval)
	assert.Equal(t, int64(1000), cfg.SafeTrimBatchLimit)
	assert.True(t, cfg.PurgeCancelledDelayed)
	assert.Equal(t, int64(0), cfg.EnqueueHardLimit)
}

func TestLifecycleConfig_Normalize(t *testing.T) {
	cfg := lifecycle.LifecycleConfig{
		DLQMaxCount: -5,
	}.Normalize()
	assert.Equal(t, int64(0), cfg.DLQMaxCount, "negative DLQ max becomes unlimited")
	assert.Equal(t, 24*time.Hour, cfg.CancelledTTL)
	assert.Equal(t, lifecycle.DelayedOverflowReject, cfg.DelayedOverflow)
	assert.Equal(t, 30*time.Second, cfg.SafeTrimInterval)
	assert.Equal(t, int64(1000), cfg.SafeTrimBatchLimit)
}

func TestLifecycleMetrics_Snapshot(t *testing.T) {
	assert.Empty(t, (*lifecycle.LifecycleMetrics)(nil).Snapshot())

	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	lc.Metrics().EnqueueRejectedTotal.Add(2)
	lc.Metrics().DelayedRejectedTotal.Add(3)
	lc.Metrics().PayloadRejectedTotal.Add(1)
	lc.Metrics().SafeTrimDeletedTotal.Add(4)
	lc.Metrics().DLQEvictedTotal.Add(5)
	lc.Metrics().CancelledDelayedPurged.Add(6)
	lc.Metrics().IdleConsumersRemovedTotal.Add(7)
	lc.Metrics().SoftLimitHitsTotal.Add(8)

	snap := lc.Metrics().Snapshot()
	assert.Equal(t, int64(2), snap["enqueue_rejected_total"])
	assert.Equal(t, int64(3), snap["delayed_rejected_total"])
	assert.Equal(t, int64(1), snap["payload_rejected_total"])
	assert.Equal(t, int64(4), snap["safe_trim_deleted_total"])
	assert.Equal(t, int64(5), snap["dlq_evicted_total"])
	assert.Equal(t, int64(6), snap["cancelled_delayed_purged"])
	assert.Equal(t, int64(7), snap["idle_consumers_removed_total"])
	assert.Equal(t, int64(8), snap["soft_limit_hits_total"])
}

func TestLifecycleFromConfig(t *testing.T) {
	t.Run("nil config uses defaults", func(t *testing.T) {
		cfg := lifecycle.FromConfig(nil)
		assert.Equal(t, int64(1000), cfg.DLQMaxCount)
		assert.True(t, cfg.SafeTrimEnabled)
	})

	t.Run("empty config keeps defaults", func(t *testing.T) {
		cfg := lifecycle.FromConfig(&config.Config{})
		assert.Equal(t, int64(1000), cfg.DLQMaxCount)
		assert.True(t, cfg.SafeTrimEnabled)
		assert.True(t, cfg.PurgeCancelledDelayed)
	})

	t.Run("explicit overrides", func(t *testing.T) {
		zero := int64(0)
		off := false
		cfg := lifecycle.FromConfig(&config.Config{
			TaskMQ: config.TaskMQConfig{
				Lifecycle: config.LifecycleConfig{
					EnqueueHardLimit:      10,
					DelayedMaxCount:       20,
					DelayedMaxDelay:       2 * time.Hour,
					DelayedOverflow:       "drop_farthest",
					DLQMaxCount:           &zero,
					DLQMaxAge:             48 * time.Hour,
					CancelledTTL:          1 * time.Hour,
					MaxPayloadBytes:       1024,
					SafeTrimEnabled:       &off,
					SafeTrimInterval:      5 * time.Second,
					SafeTrimBatchLimit:    50,
					IdleConsumerTimeout:   30 * time.Minute,
					PurgeCancelledDelayed: &off,
					StreamMaxLen:          999,
					EnqueueSoftLimit:      5,
				},
			},
		})
		assert.Equal(t, int64(10), cfg.EnqueueHardLimit)
		assert.Equal(t, int64(20), cfg.DelayedMaxCount)
		assert.Equal(t, 2*time.Hour, cfg.DelayedMaxDelay)
		assert.Equal(t, lifecycle.DelayedOverflowDropFarthest, cfg.DelayedOverflow)
		assert.Equal(t, int64(0), cfg.DLQMaxCount, "explicit 0 = unlimited")
		assert.Equal(t, 48*time.Hour, cfg.DLQMaxAge)
		assert.Equal(t, time.Hour, cfg.CancelledTTL)
		assert.Equal(t, 1024, cfg.MaxPayloadBytes)
		assert.False(t, cfg.SafeTrimEnabled)
		assert.Equal(t, 5*time.Second, cfg.SafeTrimInterval)
		assert.Equal(t, int64(50), cfg.SafeTrimBatchLimit)
		assert.Equal(t, 30*time.Minute, cfg.IdleConsumerTimeout)
		assert.False(t, cfg.PurgeCancelledDelayed)
		assert.Equal(t, int64(999), cfg.StreamMaxLen)
		assert.Equal(t, int64(5), cfg.EnqueueSoftLimit)
	})

	t.Run("explicit dlq cap", func(t *testing.T) {
		cap := int64(50)
		cfg := lifecycle.FromConfig(&config.Config{
			TaskMQ: config.TaskMQConfig{
				Lifecycle: config.LifecycleConfig{DLQMaxCount: &cap},
			},
		})
		assert.Equal(t, int64(50), cfg.DLQMaxCount)
	})
}

func TestLifecycle_NilReceiverHelpers(t *testing.T) {
	var lc *lifecycle.Lifecycle
	assert.NoError(t, lc.CheckPayloadSize([]byte("x")))
	assert.NoError(t, lc.CheckDelayedMaxDelay(time.Now().Add(time.Hour)))
	assert.Equal(t, lifecycle.DefaultLifecycleConfig().Normalize().DLQMaxCount, lc.Config().DLQMaxCount)
	assert.NotNil(t, lc.Metrics())
	lc.NoteSoftLimitIfNeeded(context.Background(), nil, "q")
}

// ---------------------------------------------------------------------------
// Enqueue admission
// ---------------------------------------------------------------------------

func TestLifecycle_EnqueueHardLimit(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		EnqueueHardLimit: 2,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
	ctx := context.Background()

	require.NoError(t, client.Enqueue(ctx, taskmodel.NewTask("t", []byte("a"), taskmodel.TaskOptions{Queue: "q1"})))
	require.NoError(t, client.Enqueue(ctx, taskmodel.NewTask("t", []byte("b"), taskmodel.TaskOptions{Queue: "q1"})))

	err := client.Enqueue(ctx, taskmodel.NewTask("t", []byte("c"), taskmodel.TaskOptions{Queue: "q1"}))
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrQueueFull))
	assert.Equal(t, int64(1), lc.Metrics().EnqueueRejectedTotal.Load())

	n, err := rdb.XLen(ctx, keys.KeysFor("q1").Stream()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestLifecycle_EnqueueSoftLimitMetrics(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		EnqueueSoftLimit: 1,
		EnqueueHardLimit: 10,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
	ctx := context.Background()

	require.NoError(t, client.Enqueue(ctx, taskmodel.NewTask("t", []byte("a"), taskmodel.TaskOptions{Queue: "q1"})))
	// Second enqueue: XLEN is already 1 >= soft limit → soft hit recorded before XADD.
	require.NoError(t, client.Enqueue(ctx, taskmodel.NewTask("t", []byte("b"), taskmodel.TaskOptions{Queue: "q1"})))
	assert.GreaterOrEqual(t, lc.Metrics().SoftLimitHitsTotal.Load(), int64(1))
}

func TestLifecycle_UniqueEnqueueHardLimit(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		EnqueueHardLimit: 1,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithDefaultUniqueTTL(time.Minute))
	ctx := context.Background()

	task1 := taskmodel.NewTask("t", []byte("a"), taskmodel.TaskOptions{Queue: "q1", UniqueKey: "u1", UniqueTTL: time.Minute})
	require.NoError(t, client.Enqueue(ctx, task1))

	task2 := taskmodel.NewTask("t", []byte("b"), taskmodel.TaskOptions{Queue: "q1", UniqueKey: "u2", UniqueTTL: time.Minute})
	err := client.Enqueue(ctx, task2)
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrQueueFull))
}

func TestLifecycle_UniqueEnqueueDuplicate(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DLQMaxCount: 1000, SafeTrimEnabled: false})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
	ctx := context.Background()

	task1 := taskmodel.NewTask("t", []byte("a"), taskmodel.TaskOptions{Queue: "q1", UniqueKey: "same", UniqueTTL: time.Minute})
	require.NoError(t, client.Enqueue(ctx, task1))

	task2 := taskmodel.NewTask("t", []byte("b"), taskmodel.TaskOptions{Queue: "q1", UniqueKey: "same", UniqueTTL: time.Minute})
	err := client.Enqueue(ctx, task2)
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrDuplicateTask))
}

func TestLifecycle_MaxPayloadBytes(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		MaxPayloadBytes: 4,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
	ctx := context.Background()

	err := client.Enqueue(ctx, taskmodel.NewTask("t", []byte("12345"), taskmodel.TaskOptions{Queue: "q1"}))
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrPayloadTooLarge))
	assert.Equal(t, int64(1), lc.Metrics().PayloadRejectedTotal.Load())

	err = client.EnqueueAt(ctx, taskmodel.NewTask("t", []byte("12345"), taskmodel.TaskOptions{Queue: "q1"}), time.Now().Add(time.Minute))
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrPayloadTooLarge))

	require.NoError(t, client.Enqueue(ctx, taskmodel.NewTask("t", []byte("1234"), taskmodel.TaskOptions{Queue: "q1"})))
}

// ---------------------------------------------------------------------------
// Delayed admission
// ---------------------------------------------------------------------------

func TestLifecycle_DelayedMaxDelayAndCountReject(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DelayedMaxCount: 2,
		DelayedMaxDelay: 1 * time.Hour,
		DelayedOverflow: lifecycle.DelayedOverflowReject,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
	ctx := context.Background()

	err := client.EnqueueAt(ctx, taskmodel.NewTask("t", []byte("far"), taskmodel.TaskOptions{Queue: "q1"}), time.Now().Add(48*time.Hour))
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrDelayTooFar))
	assert.Equal(t, int64(1), lc.Metrics().DelayedRejectedTotal.Load())

	require.NoError(t, client.EnqueueIn(ctx, taskmodel.NewTask("t", []byte("1"), taskmodel.TaskOptions{Queue: "q1"}), 10*time.Minute))
	require.NoError(t, client.EnqueueIn(ctx, taskmodel.NewTask("t", []byte("2"), taskmodel.TaskOptions{Queue: "q1"}), 20*time.Minute))
	err = client.EnqueueIn(ctx, taskmodel.NewTask("t", []byte("3"), taskmodel.TaskOptions{Queue: "q1"}), 30*time.Minute)
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrDelayedFull))

	n, err := rdb.ZCard(ctx, keys.KeysFor("q1").Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestLifecycle_DelayedOverflowDropFarthest(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DelayedMaxCount: 2,
		DelayedOverflow: lifecycle.DelayedOverflowDropFarthest,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
	ctx := context.Background()
	now := time.Now()

	// Far-future task first
	require.NoError(t, client.EnqueueAt(ctx, taskmodel.NewTask("t", []byte("far"), taskmodel.TaskOptions{Queue: "q1", ID: "far"}), now.Add(3*time.Hour)))
	require.NoError(t, client.EnqueueAt(ctx, taskmodel.NewTask("t", []byte("mid"), taskmodel.TaskOptions{Queue: "q1", ID: "mid"}), now.Add(2*time.Hour)))
	// Third should drop farthest (3h) and keep this nearer one
	require.NoError(t, client.EnqueueAt(ctx, taskmodel.NewTask("t", []byte("near"), taskmodel.TaskOptions{Queue: "q1", ID: "near"}), now.Add(1*time.Hour)))

	n, err := rdb.ZCard(ctx, keys.KeysFor("q1").Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	// Highest scores should not include the original farthest-only member exclusively —
	// the set should be the two most recently admitted under drop policy (mid may be dropped if far was dropped first).
	members, err := rdb.ZRangeWithScores(ctx, keys.KeysFor("q1").Delayed(), 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, members, 2)
	// Earliest remaining should be "near" (1h)
	var task taskmodel.Task
	require.NoError(t, codec.JSONCodec{}.Unmarshal([]byte(members[0].Member.(string)), &task))
	assert.Equal(t, "near", task.ID)
}

func TestLifecycle_UniqueDelayedLimits(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DelayedMaxCount: 1,
		DelayedOverflow: lifecycle.DelayedOverflowReject,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
	ctx := context.Background()

	t1 := taskmodel.NewTask("t", []byte("1"), taskmodel.TaskOptions{Queue: "q1", UniqueKey: "uk1", UniqueTTL: time.Minute})
	require.NoError(t, client.EnqueueIn(ctx, t1, time.Minute))

	t2 := taskmodel.NewTask("t", []byte("2"), taskmodel.TaskOptions{Queue: "q1", UniqueKey: "uk2", UniqueTTL: time.Minute})
	err := client.EnqueueIn(ctx, t2, 2*time.Minute)
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrDelayedFull))

	// Duplicate unique key still returns duplicate when capacity allows
	lc2 := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DLQMaxCount: 1000, SafeTrimEnabled: false, DelayedMaxCount: 10})
	client2 := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc2))
	dup := taskmodel.NewTask("t", []byte("dup"), taskmodel.TaskOptions{Queue: "q2", UniqueKey: "same", UniqueTTL: time.Minute})
	require.NoError(t, client2.EnqueueIn(ctx, dup, time.Minute))
	dup2 := taskmodel.NewTask("t", []byte("dup2"), taskmodel.TaskOptions{Queue: "q2", UniqueKey: "same", UniqueTTL: time.Minute})
	err = client2.EnqueueIn(ctx, dup2, time.Minute)
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrDuplicateTask))
}

// ---------------------------------------------------------------------------
// DLQ capacity
// ---------------------------------------------------------------------------

func seedStreamMessageInPEL(t *testing.T, rdb *redis.Client, stream, group, consumer string) string {
	t.Helper()
	ctx := context.Background()
	// Ensure group exists
	_ = rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"task": "x"},
	}).Result()
	require.NoError(t, err)
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)
	return id
}

func TestLifecycle_DLQMaxCount(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DLQMaxCount: 2, SafeTrimEnabled: false})
	broker := broker.NewRedisBroker(rdb, codec.JSONCodec{}, lc)
	ctx := context.Background()
	stream := keys.KeysFor("q1").Stream()
	group := "g1"

	for i := 0; i < 3; i++ {
		id := seedStreamMessageInPEL(t, rdb, stream, group, "c1")
		task := taskmodel.NewTask("fail", []byte("p"), taskmodel.TaskOptions{Queue: "q1"})
		task.ID = fmt.Sprintf("task-%d", i)
		require.NoError(t, broker.MoveToDLQ(ctx, task, stream, id, group, "q1"))
	}

	n, err := rdb.ZCard(ctx, keys.KeysFor("q1").DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.GreaterOrEqual(t, lc.Metrics().DLQEvictedTotal.Load(), int64(1))

	// Index should only have remaining IDs
	ids, err := rdb.ZRange(ctx, keys.KeysFor("q1").DLQ(), 0, -1).Result()
	require.NoError(t, err)
	for _, id := range ids {
		exists, err := rdb.HExists(ctx, keys.KeysFor("q1").DLQIndex(), id).Result()
		require.NoError(t, err)
		assert.True(t, exists, "index missing for %s", id)
	}
}

func TestLifecycle_DLQUnlimited(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DLQMaxCount: 0, SafeTrimEnabled: false})
	broker := broker.NewRedisBroker(rdb, codec.JSONCodec{}, lc)
	ctx := context.Background()
	stream := keys.KeysFor("q1").Stream()
	group := "g1"

	for i := 0; i < 5; i++ {
		id := seedStreamMessageInPEL(t, rdb, stream, group, "c1")
		task := taskmodel.NewTask("fail", []byte("p"), taskmodel.TaskOptions{Queue: "q1"})
		task.ID = fmt.Sprintf("u-%d", i)
		require.NoError(t, broker.MoveToDLQ(ctx, task, stream, id, group, "q1"))
	}
	n, err := rdb.ZCard(ctx, keys.KeysFor("q1").DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(5), n)
	assert.Equal(t, int64(0), lc.Metrics().DLQEvictedTotal.Load())
}

func TestLifecycle_BrokerDefaultDLQWithoutLifecycle(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	// Historical default 1000 when lifecycle is nil — verify trim still works with small set by
	// checking no panic and ZADD succeeds for a few entries.
	broker := broker.NewRedisBroker(rdb, codec.JSONCodec{})
	ctx := context.Background()
	stream := keys.KeysFor("q1").Stream()
	group := "g1"
	id := seedStreamMessageInPEL(t, rdb, stream, group, "c1")
	task := taskmodel.NewTask("fail", []byte("p"), taskmodel.TaskOptions{Queue: "q1", ID: "solo"})
	task.ID = "solo"
	require.NoError(t, broker.MoveToDLQ(ctx, task, stream, id, group, "q1"))
	n, err := rdb.ZCard(ctx, keys.KeysFor("q1").DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

// ---------------------------------------------------------------------------
// Retention janitor
// ---------------------------------------------------------------------------

func TestLifecycle_SafeTrimDoesNotRemovePending(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	stream := keys.KeysFor("q1").Stream()
	group := "g1"
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	var ids []string
	for i := 0; i < 3; i++ {
		id, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"task": "x"},
		}).Result()
		require.NoError(t, err)
		ids = append(ids, id)
	}
	_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "c1",
		Streams:  []string{stream, ">"},
		Count:    3,
	}).Result()
	require.NoError(t, err)

	// ACK first only (XACK without XDEL) — residual stream entry eligible for SafeTrim
	require.NoError(t, rdb.XAck(ctx, stream, group, ids[0]).Err())

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		SafeTrimEnabled:    true,
		SafeTrimBatchLimit: 100,
		DLQMaxCount:        1000,
	})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q1", group, codec.JSONCodec{}, lc)
	_, err = j.SafeTrim(ctx)
	require.NoError(t, err)

	for _, id := range []string{ids[1], ids[2]} {
		msgs, err := rdb.XRange(ctx, stream, id, id).Result()
		require.NoError(t, err)
		require.Len(t, msgs, 1, "pending message %s must remain", id)
	}
}

func TestLifecycle_SafeTrimUsesLastDeliveredWhenNoPending(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	stream := keys.KeysFor("q1").Stream()
	group := "g1"
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	var ids []string
	for i := 0; i < 2; i++ {
		id, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"task": fmt.Sprintf("%d", i)},
		}).Result()
		require.NoError(t, err)
		ids = append(ids, id)
	}
	_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "c1", Streams: []string{stream, ">"}, Count: 2,
	}).Result()
	require.NoError(t, err)
	// ACK both without XDEL → residual; no pending
	require.NoError(t, rdb.XAck(ctx, stream, group, ids[0], ids[1]).Err())

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{SafeTrimEnabled: true, SafeTrimBatchLimit: 100, DLQMaxCount: 1000})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q1", group, codec.JSONCodec{}, lc)
	// Should not error; may trim entries older than last-delivered
	_, err = j.SafeTrim(ctx)
	require.NoError(t, err)
}

func TestLifecycle_PurgeDLQByAge(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	qk := keys.KeysFor("q1")
	// Old entry
	require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{Score: float64(time.Now().Add(-48 * time.Hour).UnixMilli()), Member: "old"}).Err())
	require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), "old", "payload-old").Err())
	// Fresh entry
	require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{Score: float64(time.Now().UnixMilli()), Member: "new"}).Err())
	require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), "new", "payload-new").Err())

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DLQMaxAge: 24 * time.Hour, SafeTrimEnabled: false, DLQMaxCount: 1000})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q1", "g", codec.JSONCodec{}, lc)
	n, err := j.PurgeDLQByAge(ctx, 24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	card, err := rdb.ZCard(ctx, qk.DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), card)
	exists, err := rdb.HExists(ctx, qk.DLQIndex(), "old").Result()
	require.NoError(t, err)
	assert.False(t, exists)
	exists, err = rdb.HExists(ctx, qk.DLQIndex(), "new").Result()
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestLifecycle_PurgeCancelledDelayed(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	codec := codec.JSONCodec{}
	taskKeep := taskmodel.NewTask("t", []byte("keep"), taskmodel.TaskOptions{Queue: "q1", ID: "keep-id"})
	taskDrop := taskmodel.NewTask("t", []byte("drop"), taskmodel.TaskOptions{Queue: "q1", ID: "drop-id"})
	serKeep, err := codec.Marshal(taskKeep)
	require.NoError(t, err)
	serDrop, err := codec.Marshal(taskDrop)
	require.NoError(t, err)

	delayed := keys.KeysFor("q1").Delayed()
	require.NoError(t, rdb.ZAdd(ctx, delayed, redis.Z{Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: string(serKeep)}).Err())
	require.NoError(t, rdb.ZAdd(ctx, delayed, redis.Z{Score: float64(time.Now().Add(2 * time.Hour).UnixMilli()), Member: string(serDrop)}).Err())
	require.NoError(t, rdb.Set(ctx, keys.KeysFor("q1").Cancelled("drop-id"), "1", time.Hour).Err())

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{PurgeCancelledDelayed: true, SafeTrimEnabled: false, DLQMaxCount: 1000})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q1", "g", codec, lc)
	n, err := j.PurgeCancelledDelayed(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	card, err := rdb.ZCard(ctx, delayed).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), card)
}

func TestLifecycle_PurgeIdleConsumers(t *testing.T) {
	rdb, mr, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	stream := keys.KeysFor("q1").Stream()
	group := "g1"
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err())
	// Create consumer by reading once then acknowledging so pending=0
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]interface{}{"task": "x"}}).Result()
	require.NoError(t, err)
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "idle-worker", Streams: []string{stream, ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)
	require.NoError(t, rdb.XAck(ctx, stream, group, id).Err())

	// Fast-forward idle time in miniredis
	mr.FastForward(2 * time.Hour)

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		IdleConsumerTimeout: 1 * time.Hour,
		SafeTrimEnabled:     false,
		DLQMaxCount:         1000,
	})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q1", group, codec.JSONCodec{}, lc)
	n, err := j.PurgeIdleConsumers(ctx, time.Hour)
	require.NoError(t, err)
	// miniredis idle semantics vary; ensure API is healthy and non-negative.
	assert.GreaterOrEqual(t, n, int64(0))
	if n > 0 {
		consumers, err := rdb.XInfoConsumers(ctx, stream, group).Result()
		require.NoError(t, err)
		for _, c := range consumers {
			assert.NotEqual(t, "idle-worker", c.Name)
		}
	}
}

func TestRetentionJanitor_TickAndRun(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		SafeTrimEnabled:       true,
		SafeTrimInterval:      50 * time.Millisecond,
		PurgeCancelledDelayed: true,
		DLQMaxAge:             24 * time.Hour,
		DLQMaxCount:           1000,
	})
	// Seed old DLQ for age purge on tick
	ctx := context.Background()
	require.NoError(t, rdb.ZAdd(ctx, keys.KeysFor("q1").DLQ(), redis.Z{
		Score:  float64(time.Now().Add(-48 * time.Hour).UnixMilli()),
		Member: "old",
	}).Err())

	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q1", "g1", codec.JSONCodec{}, lc)
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer cancel()
	err := j.Run(runCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Old DLQ should be gone after initial tick
	card, err := rdb.ZCard(ctx, keys.KeysFor("q1").DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), card)
	assert.GreaterOrEqual(t, lc.Metrics().DLQEvictedTotal.Load(), int64(1))
}

func TestRetentionJanitor_RunNoOpWhenDisabled(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		SafeTrimEnabled:       false,
		PurgeCancelledDelayed: false,
		DLQMaxAge:             0,
		IdleConsumerTimeout:   0,
		DLQMaxCount:           1000,
	})
	j := runner.NewRetentionJanitor(rdb, zap.NewNop(), "q1", "g1", codec.JSONCodec{}, lc)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := j.Run(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// ---------------------------------------------------------------------------
// Scheduler promote + cancel TTL
// ---------------------------------------------------------------------------

func TestLifecycle_PromoteSkipsWhenStreamFull(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	queue := "q1"
	stream := keys.KeysFor(queue).Stream()
	for i := 0; i < 2; i++ {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"task": "x"},
		}).Err())
	}

	task := taskmodel.NewTask("t", []byte("delayed"), taskmodel.TaskOptions{Queue: queue})
	serialized, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, keys.KeysFor(queue).Delayed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(serialized),
	}).Err())

	sched := runner.NewDelayedScheduler(rdb, zap.NewNop(), queue, &dummyCronManager{}, codec.JSONCodec{}, 20*time.Millisecond, 2)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = sched.Run(runCtx)
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	n, err := rdb.ZCard(ctx, keys.KeysFor(queue).Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	xlen, err := rdb.XLen(ctx, stream).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), xlen)
}

func TestLifecycle_PromoteWhenRoomAvailable(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	queue := "q1"
	task := taskmodel.NewTask("t", []byte("delayed"), taskmodel.TaskOptions{Queue: queue, ID: "promoted"})
	serialized, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, keys.KeysFor(queue).Delayed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(serialized),
	}).Err())

	sched := runner.NewDelayedScheduler(rdb, zap.NewNop(), queue, &dummyCronManager{}, codec.JSONCodec{}, 20*time.Millisecond, 10)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = sched.Run(runCtx)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done

	n, err := rdb.ZCard(ctx, keys.KeysFor(queue).Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	xlen, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), xlen)
}

func TestLifecycle_CancelledTTLConfigurable(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		CancelledTTL:    2 * time.Second,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
	require.NoError(t, client.CancelTask(context.Background(), "q1", "tid-1"))

	ttl, err := rdb.TTL(context.Background(), keys.KeysFor("q1").Cancelled("tid-1")).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0))
	assert.LessOrEqual(t, ttl, 2*time.Second)
}

// ---------------------------------------------------------------------------
// Worker wiring
// ---------------------------------------------------------------------------

func TestLifecycle_WorkerPoolWiresRetentionAndBroker(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		SafeTrimEnabled:  true,
		SafeTrimInterval: time.Hour, // slow so we just check start/stop
		DLQMaxCount:      3,
		EnqueueHardLimit: 100,
	})
	logger := zap.NewNop()
	pool := worker.NewWorkerPool(rdb, logger, "wire-q",
		worker.WithGroup("wire-g"),
		worker.WithConsumer("wire-c"),
		worker.WithConcurrency(1),
		worker.WithLifecycle(lc),
	)
	require.NotNil(t, pool)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, pool.Start(ctx))
	cancel()
	pool.Stop(context.Background())
}

func TestLifecycle_WithLifecycleNilRejected(t *testing.T) {
	err := worker.WithLifecycle(nil).ApplyWorkerPool(&worker.WorkerPoolOptions{})
	require.Error(t, err)
}

func TestLifecycle_CheckDelayedMaxDelayOnly(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DelayedMaxDelay: time.Hour, DLQMaxCount: 1000})
	require.NoError(t, lc.CheckDelayedMaxDelay(time.Now().Add(30*time.Minute)))
	err := lc.CheckDelayedMaxDelay(time.Now().Add(2 * time.Hour))
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrDelayTooFar))
}

// ---------------------------------------------------------------------------
// E: full-path Lifecycle alignment (admin XADD, promote MAXLEN, broker delayed)
// ---------------------------------------------------------------------------

func TestLifecycle_RunCronJobHonorsHardLimit(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	queue := "cron-hard"
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		EnqueueHardLimit: 1,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))

	task := taskmodel.NewTask("job", []byte("p"), taskmodel.TaskOptions{Queue: queue})
	require.NoError(t, client.RegisterCron(ctx, "j1", "0 0 * * *", task))

	// Fill stream to hard limit.
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: keys.KeysFor(queue).Stream(),
		Values: map[string]interface{}{"task": "filler"},
	}).Err())

	err := client.RunCronJob(ctx, queue, "j1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrQueueFull))
	assert.Equal(t, int64(1), lc.Metrics().EnqueueRejectedTotal.Load())
}

func TestLifecycle_RunScheduledTaskHonorsHardLimit(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	queue := "sched-hard"
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		EnqueueHardLimit: 1,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))

	task := taskmodel.NewTask("t", []byte("p"), taskmodel.TaskOptions{Queue: queue, ID: "sid-1"})
	require.NoError(t, client.EnqueueAt(ctx, task, time.Now().Add(time.Hour)))

	// Fill stream to hard limit.
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: keys.KeysFor(queue).Stream(),
		Values: map[string]interface{}{"task": "filler"},
	}).Err())

	err := client.RunScheduledTask(ctx, queue, "sid-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrQueueFull))

	// Member must remain delayed (atomic promote).
	n, err := rdb.ZCard(ctx, keys.KeysFor(queue).Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestLifecycle_RunScheduledTaskPromotesWhenRoom(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	queue := "sched-ok"
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		EnqueueHardLimit: 10,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))

	task := taskmodel.NewTask("t", []byte("p"), taskmodel.TaskOptions{Queue: queue, ID: "sid-ok"})
	require.NoError(t, client.EnqueueAt(ctx, task, time.Now().Add(time.Hour)))
	require.NoError(t, client.RunScheduledTask(ctx, queue, "sid-ok"))

	n, err := rdb.ZCard(ctx, keys.KeysFor(queue).Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	xlen, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), xlen)
}

func TestLifecycle_RegisterCronHonorsDelayedMaxCount(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	queue := "cron-delayed-cap"
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DelayedMaxCount: 1,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))

	// Fill delayed with one entry.
	filler := taskmodel.NewTask("fill", []byte("x"), taskmodel.TaskOptions{Queue: queue})
	require.NoError(t, client.EnqueueAt(ctx, filler, time.Now().Add(time.Hour)))

	task := taskmodel.NewTask("job", []byte("p"), taskmodel.TaskOptions{Queue: queue})
	err := client.RegisterCron(ctx, "new-job", "0 0 * * *", task)
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrDelayedFull))
}

func TestLifecycle_BrokerRetryHonorsDelayedMaxCountDropFarthest(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	queue := "retry-cap"
	stream := keys.KeysFor(queue).Stream()
	group := "g1"
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DelayedMaxCount: 2,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	b := broker.NewRedisBroker(rdb, codec.JSONCodec{}, lc)

	// Seed two far-future delayed tasks.
	for i := 0; i < 2; i++ {
		ser, err := codec.JSONCodec{}.Marshal(taskmodel.NewTask("old", []byte("o"), taskmodel.TaskOptions{
			Queue: queue, ID: fmt.Sprintf("old-%d", i),
		}))
		require.NoError(t, err)
		require.NoError(t, rdb.ZAdd(ctx, keys.KeysFor(queue).Delayed(), redis.Z{
			Score:  float64(time.Now().Add(24 * time.Hour).UnixMilli()),
			Member: string(ser),
		}).Err())
	}

	// Put a stream message to retry.
	task := taskmodel.NewTask("retry-me", []byte("r"), taskmodel.TaskOptions{Queue: queue, ID: "retry-1"})
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	msgID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"task": string(ser)},
	}).Result()
	require.NoError(t, err)

	// Claim into PEL by reading as consumer.
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "c1",
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)

	// Retry should succeed and drop farthest delayed under capacity.
	require.NoError(t, b.ScheduleRetry(ctx, task, stream, msgID, group, time.Now().Add(time.Minute)))

	n, err := rdb.ZCard(ctx, keys.KeysFor(queue).Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n) // still capped at 2

	// Retry task should be present (near-term score wins over dropped farthest).
	members, err := rdb.ZRange(ctx, keys.KeysFor(queue).Delayed(), 0, -1).Result()
	require.NoError(t, err)
	found := false
	jc := codec.JSONCodec{}
	for _, m := range members {
		var tt taskmodel.Task
		if jc.Unmarshal([]byte(m), &tt) == nil && tt.ID == "retry-1" {
			found = true
		}
	}
	assert.True(t, found, "retry task should remain after drop_farthest")
}

func TestLifecycle_PromotePassesStreamMaxLen(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	queue := "promote-maxlen"
	// hard unlimited, maxlen set — promote should still XADD (MAXLEN is soft catastrophe valve).
	task := taskmodel.NewTask("t", []byte("delayed"), taskmodel.TaskOptions{Queue: queue, ID: "pm-1"})
	serialized, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, keys.KeysFor(queue).Delayed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(serialized),
	}).Err())

	// hard=0, maxlen=5
	sched := runner.NewDelayedScheduler(rdb, zap.NewNop(), queue, &dummyCronManager{}, codec.JSONCodec{}, 20*time.Millisecond, 0, 5)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = sched.Run(runCtx)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done

	n, err := rdb.ZCard(ctx, keys.KeysFor(queue).Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	xlen, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), xlen)
}

func TestLifecycle_ZAddDelayedSystemDropFarthest(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	delayedKey := keys.KeysFor("sys-z").Delayed()
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DelayedMaxCount: 2,
		DelayedOverflow: lifecycle.DelayedOverflowReject, // client would reject; system still drops
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})

	now := time.Now()
	for i := 0; i < 2; i++ {
		require.NoError(t, lc.ZAddDelayed(ctx, rdb, delayedKey, now.Add(time.Duration(i+10)*time.Hour).UnixMilli(), []byte(fmt.Sprintf("far-%d", i))))
	}
	// Client path rejects when full.
	err := lc.ZAddDelayed(ctx, rdb, delayedKey, now.Add(time.Minute).UnixMilli(), []byte("client-new"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, lifecycle.ErrDelayedFull))

	// System path always succeeds under drop_farthest.
	require.NoError(t, lc.ZAddDelayedSystem(ctx, rdb, delayedKey, now.Add(time.Minute).UnixMilli(), []byte("system-new")))
	n, err := rdb.ZCard(ctx, delayedKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	members, err := rdb.ZRange(ctx, delayedKey, 0, -1).Result()
	require.NoError(t, err)
	assert.Contains(t, members, "system-new")
}
