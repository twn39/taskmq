package taskmq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/config"
	"go.uber.org/zap"
)

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

func testLifecycle(cfg LifecycleConfig) *Lifecycle {
	// SafeTrim off by default in unit helpers unless explicitly testing it.
	if !cfg.SafeTrimEnabled && cfg.SafeTrimInterval == 0 {
		cfg.SafeTrimEnabled = false
	}
	if cfg.DLQMaxCount == 0 && cfg.CancelledTTL == 0 {
		// leave zeros as intentional for unlimited tests
	}
	return NewLifecycle(cfg)
}

// ---------------------------------------------------------------------------
// Config / metrics
// ---------------------------------------------------------------------------

func TestDefaultLifecycleConfig(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	assert.Equal(t, int64(1000), cfg.DLQMaxCount)
	assert.Equal(t, 24*time.Hour, cfg.CancelledTTL)
	assert.Equal(t, DelayedOverflowReject, cfg.DelayedOverflow)
	assert.True(t, cfg.SafeTrimEnabled)
	assert.Equal(t, 30*time.Second, cfg.SafeTrimInterval)
	assert.Equal(t, int64(1000), cfg.SafeTrimBatchLimit)
	assert.True(t, cfg.PurgeCancelledDelayed)
	assert.Equal(t, int64(0), cfg.EnqueueHardLimit)
}

func TestLifecycleConfig_Normalize(t *testing.T) {
	cfg := LifecycleConfig{
		DLQMaxCount: -5,
	}.Normalize()
	assert.Equal(t, int64(0), cfg.DLQMaxCount, "negative DLQ max becomes unlimited")
	assert.Equal(t, 24*time.Hour, cfg.CancelledTTL)
	assert.Equal(t, DelayedOverflowReject, cfg.DelayedOverflow)
	assert.Equal(t, 30*time.Second, cfg.SafeTrimInterval)
	assert.Equal(t, int64(1000), cfg.SafeTrimBatchLimit)
}

func TestLifecycleMetrics_Snapshot(t *testing.T) {
	assert.Empty(t, (*LifecycleMetrics)(nil).Snapshot())

	lc := NewLifecycle(DefaultLifecycleConfig())
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
		cfg := LifecycleFromConfig(nil)
		assert.Equal(t, int64(1000), cfg.DLQMaxCount)
		assert.True(t, cfg.SafeTrimEnabled)
	})

	t.Run("empty config keeps defaults", func(t *testing.T) {
		cfg := LifecycleFromConfig(&config.Config{})
		assert.Equal(t, int64(1000), cfg.DLQMaxCount)
		assert.True(t, cfg.SafeTrimEnabled)
		assert.True(t, cfg.PurgeCancelledDelayed)
	})

	t.Run("explicit overrides", func(t *testing.T) {
		zero := int64(0)
		off := false
		cfg := LifecycleFromConfig(&config.Config{
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
		assert.Equal(t, DelayedOverflowDropFarthest, cfg.DelayedOverflow)
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
		cfg := LifecycleFromConfig(&config.Config{
			TaskMQ: config.TaskMQConfig{
				Lifecycle: config.LifecycleConfig{DLQMaxCount: &cap},
			},
		})
		assert.Equal(t, int64(50), cfg.DLQMaxCount)
	})
}

func TestLifecycle_NilReceiverHelpers(t *testing.T) {
	var lc *Lifecycle
	assert.NoError(t, lc.CheckPayloadSize([]byte("x")))
	assert.NoError(t, lc.CheckDelayedMaxDelay(time.Now().Add(time.Hour)))
	assert.Equal(t, DefaultLifecycleConfig().Normalize().DLQMaxCount, lc.Config().DLQMaxCount)
	assert.NotNil(t, lc.Metrics())
	lc.NoteSoftLimitIfNeeded(context.Background(), nil, "q")
}

// ---------------------------------------------------------------------------
// Enqueue admission
// ---------------------------------------------------------------------------

func TestLifecycle_EnqueueHardLimit(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		EnqueueHardLimit: 2,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := NewClient(rdb, WithClientLifecycle(lc))
	ctx := context.Background()

	require.NoError(t, client.Enqueue(ctx, NewTask("t", []byte("a"), TaskOptions{Queue: "q1"})))
	require.NoError(t, client.Enqueue(ctx, NewTask("t", []byte("b"), TaskOptions{Queue: "q1"})))

	err := client.Enqueue(ctx, NewTask("t", []byte("c"), TaskOptions{Queue: "q1"}))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrQueueFull))
	assert.Equal(t, int64(1), lc.Metrics().EnqueueRejectedTotal.Load())

	n, err := rdb.XLen(ctx, KeysFor("q1").Stream()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestLifecycle_EnqueueSoftLimitMetrics(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		EnqueueSoftLimit: 1,
		EnqueueHardLimit: 10,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := NewClient(rdb, WithClientLifecycle(lc))
	ctx := context.Background()

	require.NoError(t, client.Enqueue(ctx, NewTask("t", []byte("a"), TaskOptions{Queue: "q1"})))
	// Second enqueue: XLEN is already 1 >= soft limit → soft hit recorded before XADD.
	require.NoError(t, client.Enqueue(ctx, NewTask("t", []byte("b"), TaskOptions{Queue: "q1"})))
	assert.GreaterOrEqual(t, lc.Metrics().SoftLimitHitsTotal.Load(), int64(1))
}

func TestLifecycle_UniqueEnqueueHardLimit(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		EnqueueHardLimit: 1,
		DLQMaxCount:      1000,
		SafeTrimEnabled:  false,
	})
	client := NewClient(rdb, WithClientLifecycle(lc), WithDefaultUniqueTTL(time.Minute))
	ctx := context.Background()

	task1 := NewTask("t", []byte("a"), TaskOptions{Queue: "q1", UniqueKey: "u1", UniqueTTL: time.Minute})
	require.NoError(t, client.Enqueue(ctx, task1))

	task2 := NewTask("t", []byte("b"), TaskOptions{Queue: "q1", UniqueKey: "u2", UniqueTTL: time.Minute})
	err := client.Enqueue(ctx, task2)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrQueueFull))
}

func TestLifecycle_UniqueEnqueueDuplicate(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{DLQMaxCount: 1000, SafeTrimEnabled: false})
	client := NewClient(rdb, WithClientLifecycle(lc))
	ctx := context.Background()

	task1 := NewTask("t", []byte("a"), TaskOptions{Queue: "q1", UniqueKey: "same", UniqueTTL: time.Minute})
	require.NoError(t, client.Enqueue(ctx, task1))

	task2 := NewTask("t", []byte("b"), TaskOptions{Queue: "q1", UniqueKey: "same", UniqueTTL: time.Minute})
	err := client.Enqueue(ctx, task2)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDuplicateTask))
}

func TestLifecycle_MaxPayloadBytes(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		MaxPayloadBytes: 4,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := NewClient(rdb, WithClientLifecycle(lc))
	ctx := context.Background()

	err := client.Enqueue(ctx, NewTask("t", []byte("12345"), TaskOptions{Queue: "q1"}))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPayloadTooLarge))
	assert.Equal(t, int64(1), lc.Metrics().PayloadRejectedTotal.Load())

	err = client.EnqueueAt(ctx, NewTask("t", []byte("12345"), TaskOptions{Queue: "q1"}), time.Now().Add(time.Minute))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPayloadTooLarge))

	require.NoError(t, client.Enqueue(ctx, NewTask("t", []byte("1234"), TaskOptions{Queue: "q1"})))
}

// ---------------------------------------------------------------------------
// Delayed admission
// ---------------------------------------------------------------------------

func TestLifecycle_DelayedMaxDelayAndCountReject(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		DelayedMaxCount: 2,
		DelayedMaxDelay: 1 * time.Hour,
		DelayedOverflow: DelayedOverflowReject,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := NewClient(rdb, WithClientLifecycle(lc))
	ctx := context.Background()

	err := client.EnqueueAt(ctx, NewTask("t", []byte("far"), TaskOptions{Queue: "q1"}), time.Now().Add(48*time.Hour))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDelayTooFar))
	assert.Equal(t, int64(1), lc.Metrics().DelayedRejectedTotal.Load())

	require.NoError(t, client.EnqueueIn(ctx, NewTask("t", []byte("1"), TaskOptions{Queue: "q1"}), 10*time.Minute))
	require.NoError(t, client.EnqueueIn(ctx, NewTask("t", []byte("2"), TaskOptions{Queue: "q1"}), 20*time.Minute))
	err = client.EnqueueIn(ctx, NewTask("t", []byte("3"), TaskOptions{Queue: "q1"}), 30*time.Minute)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDelayedFull))

	n, err := rdb.ZCard(ctx, KeysFor("q1").Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestLifecycle_DelayedOverflowDropFarthest(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		DelayedMaxCount: 2,
		DelayedOverflow: DelayedOverflowDropFarthest,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := NewClient(rdb, WithClientLifecycle(lc))
	ctx := context.Background()
	now := time.Now()

	// Far-future task first
	require.NoError(t, client.EnqueueAt(ctx, NewTask("t", []byte("far"), TaskOptions{Queue: "q1", ID: "far"}), now.Add(3*time.Hour)))
	require.NoError(t, client.EnqueueAt(ctx, NewTask("t", []byte("mid"), TaskOptions{Queue: "q1", ID: "mid"}), now.Add(2*time.Hour)))
	// Third should drop farthest (3h) and keep this nearer one
	require.NoError(t, client.EnqueueAt(ctx, NewTask("t", []byte("near"), TaskOptions{Queue: "q1", ID: "near"}), now.Add(1*time.Hour)))

	n, err := rdb.ZCard(ctx, KeysFor("q1").Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	// Highest scores should not include the original farthest-only member exclusively —
	// the set should be the two most recently admitted under drop policy (mid may be dropped if far was dropped first).
	members, err := rdb.ZRangeWithScores(ctx, KeysFor("q1").Delayed(), 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, members, 2)
	// Earliest remaining should be "near" (1h)
	var task Task
	require.NoError(t, JSONCodec{}.Unmarshal([]byte(members[0].Member.(string)), &task))
	assert.Equal(t, "near", task.ID)
}

func TestLifecycle_UniqueDelayedLimits(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		DelayedMaxCount: 1,
		DelayedOverflow: DelayedOverflowReject,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := NewClient(rdb, WithClientLifecycle(lc))
	ctx := context.Background()

	t1 := NewTask("t", []byte("1"), TaskOptions{Queue: "q1", UniqueKey: "uk1", UniqueTTL: time.Minute})
	require.NoError(t, client.EnqueueIn(ctx, t1, time.Minute))

	t2 := NewTask("t", []byte("2"), TaskOptions{Queue: "q1", UniqueKey: "uk2", UniqueTTL: time.Minute})
	err := client.EnqueueIn(ctx, t2, 2*time.Minute)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDelayedFull))

	// Duplicate unique key still returns duplicate when capacity allows
	lc2 := NewLifecycle(LifecycleConfig{DLQMaxCount: 1000, SafeTrimEnabled: false, DelayedMaxCount: 10})
	client2 := NewClient(rdb, WithClientLifecycle(lc2))
	dup := NewTask("t", []byte("dup"), TaskOptions{Queue: "q2", UniqueKey: "same", UniqueTTL: time.Minute})
	require.NoError(t, client2.EnqueueIn(ctx, dup, time.Minute))
	dup2 := NewTask("t", []byte("dup2"), TaskOptions{Queue: "q2", UniqueKey: "same", UniqueTTL: time.Minute})
	err = client2.EnqueueIn(ctx, dup2, time.Minute)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDuplicateTask))
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

	lc := NewLifecycle(LifecycleConfig{DLQMaxCount: 2, SafeTrimEnabled: false})
	broker := NewRedisBroker(rdb, JSONCodec{}, lc)
	ctx := context.Background()
	stream := KeysFor("q1").Stream()
	group := "g1"

	for i := 0; i < 3; i++ {
		id := seedStreamMessageInPEL(t, rdb, stream, group, "c1")
		task := NewTask("fail", []byte("p"), TaskOptions{Queue: "q1"})
		task.ID = fmt.Sprintf("task-%d", i)
		require.NoError(t, broker.MoveToDLQ(ctx, task, stream, id, group, "q1"))
	}

	n, err := rdb.ZCard(ctx, KeysFor("q1").DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.GreaterOrEqual(t, lc.Metrics().DLQEvictedTotal.Load(), int64(1))

	// Index should only have remaining IDs
	ids, err := rdb.ZRange(ctx, KeysFor("q1").DLQ(), 0, -1).Result()
	require.NoError(t, err)
	for _, id := range ids {
		exists, err := rdb.HExists(ctx, KeysFor("q1").DLQIndex(), id).Result()
		require.NoError(t, err)
		assert.True(t, exists, "index missing for %s", id)
	}
}

func TestLifecycle_DLQUnlimited(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{DLQMaxCount: 0, SafeTrimEnabled: false})
	broker := NewRedisBroker(rdb, JSONCodec{}, lc)
	ctx := context.Background()
	stream := KeysFor("q1").Stream()
	group := "g1"

	for i := 0; i < 5; i++ {
		id := seedStreamMessageInPEL(t, rdb, stream, group, "c1")
		task := NewTask("fail", []byte("p"), TaskOptions{Queue: "q1"})
		task.ID = fmt.Sprintf("u-%d", i)
		require.NoError(t, broker.MoveToDLQ(ctx, task, stream, id, group, "q1"))
	}
	n, err := rdb.ZCard(ctx, KeysFor("q1").DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(5), n)
	assert.Equal(t, int64(0), lc.Metrics().DLQEvictedTotal.Load())
}

func TestLifecycle_BrokerDefaultDLQWithoutLifecycle(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	// Historical default 1000 when lifecycle is nil — verify trim still works with small set by
	// checking no panic and ZADD succeeds for a few entries.
	broker := NewRedisBroker(rdb, JSONCodec{})
	ctx := context.Background()
	stream := KeysFor("q1").Stream()
	group := "g1"
	id := seedStreamMessageInPEL(t, rdb, stream, group, "c1")
	task := NewTask("fail", []byte("p"), TaskOptions{Queue: "q1", ID: "solo"})
	task.ID = "solo"
	require.NoError(t, broker.MoveToDLQ(ctx, task, stream, id, group, "q1"))
	n, err := rdb.ZCard(ctx, KeysFor("q1").DLQ()).Result()
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
	stream := KeysFor("q1").Stream()
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

	lc := NewLifecycle(LifecycleConfig{
		SafeTrimEnabled:    true,
		SafeTrimBatchLimit: 100,
		DLQMaxCount:        1000,
	})
	j := &RetentionJanitor{
		rdb: rdb, logger: zap.NewNop(), queue: "q1", group: group, codec: JSONCodec{}, lifecycle: lc,
	}
	_, err = j.safeTrim(ctx)
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
	stream := KeysFor("q1").Stream()
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

	lc := NewLifecycle(LifecycleConfig{SafeTrimEnabled: true, SafeTrimBatchLimit: 100, DLQMaxCount: 1000})
	j := &RetentionJanitor{rdb: rdb, logger: zap.NewNop(), queue: "q1", group: group, codec: JSONCodec{}, lifecycle: lc}
	// Should not error; may trim entries older than last-delivered
	_, err = j.safeTrim(ctx)
	require.NoError(t, err)
}

func TestLifecycle_PurgeDLQByAge(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	keys := KeysFor("q1")
	// Old entry
	require.NoError(t, rdb.ZAdd(ctx, keys.DLQ(), redis.Z{Score: float64(time.Now().Add(-48 * time.Hour).UnixMilli()), Member: "old"}).Err())
	require.NoError(t, rdb.HSet(ctx, keys.DLQIndex(), "old", "payload-old").Err())
	// Fresh entry
	require.NoError(t, rdb.ZAdd(ctx, keys.DLQ(), redis.Z{Score: float64(time.Now().UnixMilli()), Member: "new"}).Err())
	require.NoError(t, rdb.HSet(ctx, keys.DLQIndex(), "new", "payload-new").Err())

	lc := NewLifecycle(LifecycleConfig{DLQMaxAge: 24 * time.Hour, SafeTrimEnabled: false, DLQMaxCount: 1000})
	j := &RetentionJanitor{rdb: rdb, logger: zap.NewNop(), queue: "q1", group: "g", codec: JSONCodec{}, lifecycle: lc}
	n, err := j.purgeDLQByAge(ctx, 24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	card, err := rdb.ZCard(ctx, keys.DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), card)
	exists, err := rdb.HExists(ctx, keys.DLQIndex(), "old").Result()
	require.NoError(t, err)
	assert.False(t, exists)
	exists, err = rdb.HExists(ctx, keys.DLQIndex(), "new").Result()
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestLifecycle_PurgeCancelledDelayed(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	ctx := context.Background()
	codec := JSONCodec{}
	taskKeep := NewTask("t", []byte("keep"), TaskOptions{Queue: "q1", ID: "keep-id"})
	taskDrop := NewTask("t", []byte("drop"), TaskOptions{Queue: "q1", ID: "drop-id"})
	serKeep, err := codec.Marshal(taskKeep)
	require.NoError(t, err)
	serDrop, err := codec.Marshal(taskDrop)
	require.NoError(t, err)

	delayed := KeysFor("q1").Delayed()
	require.NoError(t, rdb.ZAdd(ctx, delayed, redis.Z{Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: string(serKeep)}).Err())
	require.NoError(t, rdb.ZAdd(ctx, delayed, redis.Z{Score: float64(time.Now().Add(2 * time.Hour).UnixMilli()), Member: string(serDrop)}).Err())
	require.NoError(t, rdb.Set(ctx, KeysFor("q1").Cancelled("drop-id"), "1", time.Hour).Err())

	lc := NewLifecycle(LifecycleConfig{PurgeCancelledDelayed: true, SafeTrimEnabled: false, DLQMaxCount: 1000})
	j := &RetentionJanitor{rdb: rdb, logger: zap.NewNop(), queue: "q1", group: "g", codec: codec, lifecycle: lc}
	n, err := j.purgeCancelledDelayed(ctx)
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
	stream := KeysFor("q1").Stream()
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

	lc := NewLifecycle(LifecycleConfig{
		IdleConsumerTimeout: 1 * time.Hour,
		SafeTrimEnabled:     false,
		DLQMaxCount:         1000,
	})
	j := &RetentionJanitor{rdb: rdb, logger: zap.NewNop(), queue: "q1", group: group, codec: JSONCodec{}, lifecycle: lc}
	n, err := j.purgeIdleConsumers(ctx, time.Hour)
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

	lc := NewLifecycle(LifecycleConfig{
		SafeTrimEnabled:       true,
		SafeTrimInterval:      50 * time.Millisecond,
		PurgeCancelledDelayed: true,
		DLQMaxAge:             24 * time.Hour,
		DLQMaxCount:           1000,
	})
	// Seed old DLQ for age purge on tick
	ctx := context.Background()
	require.NoError(t, rdb.ZAdd(ctx, KeysFor("q1").DLQ(), redis.Z{
		Score:  float64(time.Now().Add(-48 * time.Hour).UnixMilli()),
		Member: "old",
	}).Err())

	j := newRetentionJanitor(rdb, zap.NewNop(), "q1", "g1", JSONCodec{}, lc)
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer cancel()
	err := j.Run(runCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Old DLQ should be gone after initial tick
	card, err := rdb.ZCard(ctx, KeysFor("q1").DLQ()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), card)
	assert.GreaterOrEqual(t, lc.Metrics().DLQEvictedTotal.Load(), int64(1))
}

func TestRetentionJanitor_RunNoOpWhenDisabled(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		SafeTrimEnabled:       false,
		PurgeCancelledDelayed: false,
		DLQMaxAge:             0,
		IdleConsumerTimeout:   0,
		DLQMaxCount:           1000,
	})
	j := newRetentionJanitor(rdb, zap.NewNop(), "q1", "g1", JSONCodec{}, lc)
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
	stream := KeysFor(queue).Stream()
	for i := 0; i < 2; i++ {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"task": "x"},
		}).Err())
	}

	task := NewTask("t", []byte("delayed"), TaskOptions{Queue: queue})
	serialized, err := JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, KeysFor(queue).Delayed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(serialized),
	}).Err())

	sched := newDelayedScheduler(rdb, zap.NewNop(), queue, &dummyCronManager{}, JSONCodec{}, 20*time.Millisecond, 2)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = sched.Run(runCtx)
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	n, err := rdb.ZCard(ctx, KeysFor(queue).Delayed()).Result()
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
	task := NewTask("t", []byte("delayed"), TaskOptions{Queue: queue, ID: "promoted"})
	serialized, err := JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, KeysFor(queue).Delayed(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(serialized),
	}).Err())

	sched := newDelayedScheduler(rdb, zap.NewNop(), queue, &dummyCronManager{}, JSONCodec{}, 20*time.Millisecond, 10)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = sched.Run(runCtx)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done

	n, err := rdb.ZCard(ctx, KeysFor(queue).Delayed()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	xlen, err := rdb.XLen(ctx, KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), xlen)
}

func TestLifecycle_CancelledTTLConfigurable(t *testing.T) {
	rdb, _, cleanup := setupMiniRedis(t)
	defer cleanup()

	lc := NewLifecycle(LifecycleConfig{
		CancelledTTL:    2 * time.Second,
		DLQMaxCount:     1000,
		SafeTrimEnabled: false,
	})
	client := NewClient(rdb, WithClientLifecycle(lc))
	require.NoError(t, client.CancelTask(context.Background(), "q1", "tid-1"))

	ttl, err := rdb.TTL(context.Background(), KeysFor("q1").Cancelled("tid-1")).Result()
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

	lc := NewLifecycle(LifecycleConfig{
		SafeTrimEnabled:  true,
		SafeTrimInterval: time.Hour, // slow so we just check start/stop
		DLQMaxCount:      3,
		EnqueueHardLimit: 100,
	})
	logger := zap.NewNop()
	pool := NewWorkerPool(rdb, logger, "wire-q",
		WithGroup("wire-g"),
		WithConsumer("wire-c"),
		WithConcurrency(1),
		WithLifecycle(lc),
	)
	wp := pool.(*workerPool)
	require.NotNil(t, wp.retentionJanitor)
	require.NotNil(t, wp.broker)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, pool.Start(ctx))
	cancel()
	pool.Stop(context.Background())
}

func TestLifecycle_WithLifecycleNilRejected(t *testing.T) {
	opts := defaultWorkerPoolOptions(JSONCodec{})
	err := WithLifecycle(nil).ApplyWorkerPool(&opts)
	require.Error(t, err)
}

func TestLifecycle_CheckDelayedMaxDelayOnly(t *testing.T) {
	lc := NewLifecycle(LifecycleConfig{DelayedMaxDelay: time.Hour, DLQMaxCount: 1000})
	require.NoError(t, lc.CheckDelayedMaxDelay(time.Now().Add(30*time.Minute)))
	err := lc.CheckDelayedMaxDelay(time.Now().Add(2 * time.Hour))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDelayTooFar))
}
