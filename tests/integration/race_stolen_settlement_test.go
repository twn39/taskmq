package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// TestTaskMQ_PEL_StolenConcurrentSettlement simulates the classic distributed split-brain race:
// 1. Worker 1 reads a message into its PEL.
// 2. Worker 1 hangs (simulating GC pause or network stall).
// 3. Janitor reclaims the pending message via XAutoClaim and assigns it to Worker 2.
// 4. Worker 1 wakes up, and BOTH Worker 1 and Worker 2 concurrently invoke settlement
//    (Worker 1 calls CompleteTask, Worker 2 calls MoveToDLQ or CompleteTask).
// 5. Invariant:
//    - Redis Lua scripts execute without error.
//    - Final state is clean and consistent.
//    - PEL is completely cleared (0 pending).
//    - UniqueKey lock is safely released without double-deletion corruption.
func TestTaskMQ_PEL_StolenConcurrentSettlement(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "pel_stolen")
	group := "pel_grp"
	qk := keys.KeysFor(queue)
	streamKey := qk.Stream()
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	jsonCodec := codec.JSONCodec{}
	b := broker.NewRedisBroker(rdb, jsonCodec, lc)

	// 1. Pre-create stream and consumer group
	err := rdb.XGroupCreateMkStream(ctx, streamKey, group, "$").Err()
	require.NoError(t, err)

	uniqueKey := "pel-unique-job-42"
	lockKey := qk.Unique(uniqueKey)

	tsk := &taskmodel.Task{
		ID:          "task-stolen-001",
		Queue:       queue,
		Name:        "email.send",
		UniqueKey:   uniqueKey,
		UniqueTTLMs: 60000,
		MaxRetry:    3,
		Payload:     []byte(`{"to":"user@example.com"}`),
	}
	serialized, err := jsonCodec.Marshal(tsk)
	require.NoError(t, err)

	// Set unique lock in Redis for this task
	require.NoError(t, rdb.Set(ctx, lockKey, tsk.ID, time.Minute).Err())

	// Write message directly into Redis stream
	msgID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{"task": serialized},
	}).Result()
	require.NoError(t, err)

	// Worker 1 reads the message (message is now in Worker 1's PEL)
	res1, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "worker-slow-1",
		Streams:  []string{streamKey, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, res1, 1)
	require.Len(t, res1[0].Messages, 1)
	require.Equal(t, msgID, res1[0].Messages[0].ID)

	// Verify PEL has 1 pending message owned by worker-slow-1
	pend, err := rdb.XPending(ctx, streamKey, group).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), pend.Count)

	// Janitor / Worker 2 reclaims message via XAutoClaim
	claimRes, _, err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    group,
		Consumer: "worker-rescuer-2",
		MinIdle:  0, // immediate reclaim
		Start:    "0-0",
		Count:    1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, claimRes, 1)
	require.Equal(t, msgID, claimRes[0].ID)

	// Now Worker 1 wakes up! Both Worker 1 and Worker 2 try to settle simultaneously!
	// Worker 1 calls CompleteTask; Worker 2 calls CompleteTask
	barrier := make(chan struct{})
	var wg sync.WaitGroup
	var errW1, errW2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-barrier
		errW1 = b.CompleteTask(ctx, tsk, streamKey, msgID, group)
	}()

	go func() {
		defer wg.Done()
		<-barrier
		errW2 = b.CompleteTask(ctx, tsk, streamKey, msgID, group)
	}()

	close(barrier)
	wg.Wait()

	// Invariant 1: Neither call should return unexpected Redis execution errors
	require.NoError(t, errW1, "Worker 1 settlement must not fail")
	require.NoError(t, errW2, "Worker 2 settlement must not fail")

	// Invariant 2: PEL must be completely drained (XACK succeeded)
	pendAfter, err := rdb.XPending(ctx, streamKey, group).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), pendAfter.Count, "PEL must have 0 pending messages after double settlement")

	// Invariant 3: Unique lock must be released
	WaitKeyGone(t, ctx, rdb, lockKey, 2*time.Second)
}

// TestTaskMQ_PEL_PoisonPillDeliveryLimitCutoff tests that when a poison pill task
// has repeatedly crashed workers and its delivery count in PEL exceeds MaxRetry:
// 1. The worker recognizes __delivery_count and determines Retry > MaxRetry.
// 2. The user business handler is NEVER called (0 invocations).
// 3. The poison task is routed directly to DLQ.
// 4. The stream PEL is cleanly acknowledged.
func TestTaskMQ_PEL_PoisonPillDeliveryLimitCutoff(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "pel_poison")
	group := "poison_grp"
	qk := keys.KeysFor(queue)
	streamKey := qk.Stream()
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	jsonCodec := codec.JSONCodec{}

	// Create consumer group
	err := rdb.XGroupCreateMkStream(ctx, streamKey, group, "0").Err()
	require.NoError(t, err)

	// Task configured with MaxRetry = 1
	poisonTask := &taskmodel.Task{
		ID:        "poison-task-999",
		Queue:     queue,
		Name:      "dangerous.exec",
		MaxRetry:  1,
		Payload:   []byte(`{"crash":true}`),
	}
	serialized, err := jsonCodec.Marshal(poisonTask)
	require.NoError(t, err)

	// Seed into stream
	msgID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{"task": serialized},
	}).Result()
	require.NoError(t, err)

	// Read into PEL by worker-crash-1
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "worker-crash-1",
		Streams:  []string{streamKey, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)

	// Simulate delivery count reaching 3 (Retry becomes 3-1 = 2, which is > MaxRetry 1)
	// We build a WorkerPool and let its MessageProcessor process this reclaimed message with __delivery_count = 3
	var handlerCalled atomic.Int64
	w := NewTestWorkerPool(rdb, queue, group, "worker-janitor-consumer", 1, lc)
	w.Register("dangerous.exec", func(c context.Context, t *taskmodel.Task) error {
		handlerCalled.Add(1)
		return nil
	})

	// Use worker pool processor directly to process the reclaimed message with __delivery_count
	type messageProcessor interface {
		ProcessMessage(ctx context.Context, msg redis.XMessage)
	}

	mp, ok := w.(messageProcessor)
	require.True(t, ok, "worker pool must implement ProcessMessage")

	reclaimedMsg := redis.XMessage{
		ID: msgID,
		Values: map[string]interface{}{
			"task":             serialized,
			"__delivery_count": int64(3), // delivery count > MaxRetry (1)
		},
	}

	mp.ProcessMessage(ctx, reclaimedMsg)

	// Assert: Handler was NEVER called
	require.Equal(t, int64(0), handlerCalled.Load(), "User handler must NOT be called for poison pill exceeding MaxRetry")

	// Assert: Task is in DLQ
	dlqKey := qk.DLQ()
	dlqCount, err := rdb.ZCard(ctx, dlqKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), dlqCount, "Poison pill task must be routed directly to DLQ")

	// Assert: PEL was cleared
	pend, err := rdb.XPending(ctx, streamKey, group).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), pend.Count, "Poison pill must be acknowledged and removed from PEL")
}
