package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	"github.com/twn39/taskmq/internal/taskmq/metricsq"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func setupBroker(t *testing.T, lc *lifecycle.Lifecycle) (context.Context, redis.UniversalClient, broker.TaskBroker, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	b := broker.NewRedisBroker(rdb, codec.JSONCodec{}, lc)
	return ctx, rdb, b, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

// seedStreamMessage creates a consumer group, XADDs a payload, and reads it into the PEL.
func seedStreamMessage(t *testing.T, ctx context.Context, rdb redis.UniversalClient, stream, group, consumer string, body string) string {
	t.Helper()
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"task": body},
	}).Result()
	require.NoError(t, err)
	msgs, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0].Messages, 1)
	require.Equal(t, id, msgs[0].Messages[0].ID)
	return id
}

func pendingCount(t *testing.T, ctx context.Context, rdb redis.UniversalClient, stream, group string) int64 {
	t.Helper()
	n, err := rdb.XPending(ctx, stream, group).Result()
	require.NoError(t, err)
	return n.Count
}

func sampleTask(queue, id, unique string) *taskmodel.Task {
	t := taskmodel.NewTask("job", []byte(`{"x":1}`), taskmodel.TaskOptions{
		ID: id, Queue: queue,
	})
	t.LastError = "boom"
	if unique != "" {
		t.UniqueKey = unique
		t.UniqueTTLMs = 60000
	}
	return t
}

func TestBroker_CompleteTask_AcksAndReleasesUnique(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		CompletedRetention: time.Hour,
		CompletedMaxCount:  100,
	})
	ctx, rdb, b, cleanup := setupBroker(t, lc)
	defer cleanup()

	queue := "broker-complete-q"
	group := "g1"
	qk := keys.KeysFor(queue)
	task := sampleTask(queue, "c-1", "uniq-c")
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)

	// Own the unique lock.
	require.NoError(t, rdb.Set(ctx, qk.Unique(task.UniqueKey), task.ID, time.Minute).Err())
	msgID := seedStreamMessage(t, ctx, rdb, qk.Stream(), group, "c1", string(ser))
	require.Equal(t, int64(1), pendingCount(t, ctx, rdb, qk.Stream(), group))

	require.NoError(t, b.CompleteTask(ctx, task, qk.Stream(), msgID, group))

	require.Equal(t, int64(0), pendingCount(t, ctx, rdb, qk.Stream(), group))
	n, err := rdb.Exists(ctx, qk.Unique(task.UniqueKey)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), n, "unique lock should be released")

	info, err := meta.NewStore(rdb).Get(ctx, queue, task.ID)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, meta.StateCompleted, info.State)

	snap, err := metricsq.NewStore(rdb).Snapshot(ctx, queue)
	require.NoError(t, err)
	require.GreaterOrEqual(t, snap[metricsq.FieldCompleted], int64(1))
	require.GreaterOrEqual(t, snap[metricsq.FieldProcessed], int64(1))
}

func TestBroker_CompleteTask_NoRetentionDeletesMeta(t *testing.T) {
	ctx, rdb, b, cleanup := setupBroker(t, nil) // default / no completed retention
	defer cleanup()

	queue := "broker-complete-noret"
	group := "g1"
	qk := keys.KeysFor(queue)
	task := sampleTask(queue, "c-noret", "")
	// Pre-seed meta so we can observe deletion.
	require.NoError(t, meta.NewStore(rdb).Put(ctx, task, meta.StateActive))
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	msgID := seedStreamMessage(t, ctx, rdb, qk.Stream(), group, "c1", string(ser))

	require.NoError(t, b.CompleteTask(ctx, task, qk.Stream(), msgID, group))
	info, err := meta.NewStore(rdb).Get(ctx, queue, task.ID)
	require.NoError(t, err)
	require.Nil(t, info, "meta should be deleted when retention is off")
}

func TestBroker_ScheduleRetry_MovesToDelayedAndClearsPEL(t *testing.T) {
	ctx, rdb, b, cleanup := setupBroker(t, lifecycle.NewLifecycle(lifecycle.LifecycleConfig{}))
	defer cleanup()

	queue := "broker-retry-q"
	group := "g1"
	qk := keys.KeysFor(queue)
	task := sampleTask(queue, "r-1", "")
	task.Retry = 1
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	msgID := seedStreamMessage(t, ctx, rdb, qk.Stream(), group, "c1", string(ser))

	runAt := time.Now().Add(5 * time.Second)
	require.NoError(t, b.ScheduleRetry(ctx, task, qk.Stream(), msgID, group, runAt))

	require.Equal(t, int64(0), pendingCount(t, ctx, rdb, qk.Stream(), group))
	n, err := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	info, err := meta.NewStore(rdb).Get(ctx, queue, task.ID)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, meta.StateRetry, info.State)

	snap, _ := metricsq.NewStore(rdb).Snapshot(ctx, queue)
	require.GreaterOrEqual(t, snap[metricsq.FieldRetried], int64(1))
}

func TestBroker_MoveToDLQ_IndexesAndReleasesUnique(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DLQMaxCount: 100})
	ctx, rdb, b, cleanup := setupBroker(t, lc)
	defer cleanup()

	queue := "broker-dlq-q"
	group := "g1"
	qk := keys.KeysFor(queue)
	task := sampleTask(queue, "d-1", "uniq-d")
	require.NoError(t, rdb.Set(ctx, qk.Unique(task.UniqueKey), task.ID, time.Minute).Err())
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	msgID := seedStreamMessage(t, ctx, rdb, qk.Stream(), group, "c1", string(ser))

	require.NoError(t, b.MoveToDLQ(ctx, task, qk.Stream(), msgID, group, queue))

	require.Equal(t, int64(0), pendingCount(t, ctx, rdb, qk.Stream(), group))
	n, err := rdb.ZCard(ctx, qk.DLQ()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	raw, err := rdb.HGet(ctx, qk.DLQIndex(), task.ID).Result()
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	exists, err := rdb.Exists(ctx, qk.Unique(task.UniqueKey)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), exists)

	info, err := meta.NewStore(rdb).Get(ctx, queue, task.ID)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, meta.StateDLQ, info.State)
}

func TestBroker_MoveToDLQ_CapacityEvictsOldest(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DLQMaxCount: 2})
	ctx, rdb, b, cleanup := setupBroker(t, lc)
	defer cleanup()

	queue := "broker-dlq-cap"
	group := "g1"
	qk := keys.KeysFor(queue)

	// Pre-fill two DLQ entries so next MoveToDLQ evicts the oldest.
	for i, id := range []string{"old-1", "old-2"} {
		task := sampleTask(queue, id, "")
		ser, err := codec.JSONCodec{}.Marshal(task)
		require.NoError(t, err)
		require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{Score: float64(i + 1), Member: id}).Err())
		require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), id, ser).Err())
	}

	task := sampleTask(queue, "new-3", "")
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	msgID := seedStreamMessage(t, ctx, rdb, qk.Stream(), group, "c1", string(ser))
	require.NoError(t, b.MoveToDLQ(ctx, task, qk.Stream(), msgID, group, queue))

	n, err := rdb.ZCard(ctx, qk.DLQ()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	// oldest "old-1" should be gone
	exists, err := rdb.HExists(ctx, qk.DLQIndex(), "old-1").Result()
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = rdb.HExists(ctx, qk.DLQIndex(), "new-3").Result()
	require.NoError(t, err)
	require.True(t, exists)
}

func TestBroker_DeferRateLimitedTask_MovesToDelayed(t *testing.T) {
	ctx, rdb, b, cleanup := setupBroker(t, lifecycle.NewLifecycle(lifecycle.LifecycleConfig{}))
	defer cleanup()

	queue := "broker-defer-q"
	group := "g1"
	qk := keys.KeysFor(queue)
	task := sampleTask(queue, "def-1", "")
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	msgID := seedStreamMessage(t, ctx, rdb, qk.Stream(), group, "c1", string(ser))

	runAt := time.Now().Add(2 * time.Second)
	require.NoError(t, b.DeferRateLimitedTask(ctx, msgID, task, group, runAt))

	require.Equal(t, int64(0), pendingCount(t, ctx, rdb, qk.Stream(), group))
	n, err := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	info, err := meta.NewStore(rdb).Get(ctx, queue, task.ID)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, meta.StateDelayed, info.State)

	snap, _ := metricsq.NewStore(rdb).Snapshot(ctx, queue)
	require.GreaterOrEqual(t, snap[metricsq.FieldDeferred], int64(1))
}

func TestBroker_UniqueLock_ReleaseAndRenew(t *testing.T) {
	ctx, rdb, b, cleanup := setupBroker(t, nil)
	defer cleanup()

	queue := "broker-lock-q"
	qk := keys.KeysFor(queue)
	task := sampleTask(queue, "lock-1", "u-key")
	lockKey := qk.Unique(task.UniqueKey)

	// Wrong owner: Release should not delete.
	require.NoError(t, rdb.Set(ctx, lockKey, "other", time.Minute).Err())
	require.NoError(t, b.ReleaseUniqueLock(ctx, task))
	val, err := rdb.Get(ctx, lockKey).Result()
	require.NoError(t, err)
	require.Equal(t, "other", val)

	// Correct owner: Release deletes.
	require.NoError(t, rdb.Set(ctx, lockKey, task.ID, time.Minute).Err())
	require.NoError(t, b.ReleaseUniqueLock(ctx, task))
	n, err := rdb.Exists(ctx, lockKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	// Renew extends TTL when owned.
	require.NoError(t, rdb.Set(ctx, lockKey, task.ID, 2*time.Second).Err())
	require.NoError(t, b.RenewUniqueLock(ctx, task, 30*time.Second))
	ttl, err := rdb.PTTL(ctx, lockKey).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, 5*time.Second)

	// Empty unique key is a no-op.
	require.NoError(t, b.ReleaseUniqueLock(ctx, sampleTask(queue, "x", "")))
	require.NoError(t, b.RenewUniqueLock(ctx, sampleTask(queue, "x", ""), time.Second))
}

func TestBroker_CompleteTask_DoesNotStealForeignUniqueLock(t *testing.T) {
	ctx, rdb, b, cleanup := setupBroker(t, nil)
	defer cleanup()

	queue := "broker-foreign-lock"
	group := "g1"
	qk := keys.KeysFor(queue)
	task := sampleTask(queue, "me", "shared")
	// Lock held by another task id.
	require.NoError(t, rdb.Set(ctx, qk.Unique(task.UniqueKey), "someone-else", time.Minute).Err())
	ser, err := codec.JSONCodec{}.Marshal(task)
	require.NoError(t, err)
	msgID := seedStreamMessage(t, ctx, rdb, qk.Stream(), group, "c1", string(ser))

	require.NoError(t, b.CompleteTask(ctx, task, qk.Stream(), msgID, group))
	val, err := rdb.Get(ctx, qk.Unique(task.UniqueKey)).Result()
	require.NoError(t, err)
	require.Equal(t, "someone-else", val, "must not delete lock owned by another task")
}
