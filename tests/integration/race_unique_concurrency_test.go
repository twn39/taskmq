package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/task"
)

// TestTaskMQ_UniqueKey_ConcurrentStampede verifies that when 100 goroutines concurrently
// attempt to enqueue a task with the exact same UniqueKey at the exact same millisecond:
// 1. Strictly exactly 1 enqueue succeeds, and 99 fail with ErrDuplicateTask.
// 2. Redis Stream strictly contains 1 message.
// 3. Unique lock in Redis strictly stores the successful task's ID.
// 4. After the task is processed by a worker, the unique lock is released.
// 5. A second 100-goroutine stampede succeeds again for exactly 1 task.
func TestTaskMQ_UniqueKey_ConcurrentStampede(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "race_uk")
	group := "race_uk_grp"
	qk := keys.KeysFor(queue)
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	cli := NewTestClient(rdb, lc)

	const concurrency = 100
	const sharedUniqueKey = "biz-order-unique-10086"

	startBarrier := make(chan struct{})
	var wg sync.WaitGroup
	var successCount int64
	var duplicateCount int64
	var otherErrCount int64

	var successfulTaskID string
	var successMu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		taskID := fmt.Sprintf("task-stampede-%d", i)
		go func(id string) {
			defer wg.Done()
			<-startBarrier // wait for synchronized blast

			tsk := &task.Task{
				ID:          id,
				Queue:       queue,
				Name:        "order.payment",
				UniqueKey:   sharedUniqueKey,
				UniqueTTLMs: 30000,
				Payload:     []byte(`{"amount":100}`),
			}
			err := cli.Enqueue(ctx, tsk)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
				successMu.Lock()
				successfulTaskID = id
				successMu.Unlock()
			} else if errors.Is(err, lifecycle.ErrDuplicateTask) {
				atomic.AddInt64(&duplicateCount, 1)
			} else {
				atomic.AddInt64(&otherErrCount, 1)
			}
		}(taskID)
	}

	// Unleash all 100 goroutines simultaneously
	close(startBarrier)
	wg.Wait()

	require.Equal(t, int64(1), successCount, "strictly exactly 1 task must succeed")
	require.Equal(t, int64(concurrency-1), duplicateCount, "all 99 competing tasks must receive ErrDuplicateTask")
	require.Equal(t, int64(0), otherErrCount, "no other unexpected errors should occur")

	// Verify Redis physical state
	xlen, err := rdb.XLen(ctx, qk.Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), xlen, "Stream must only contain 1 message")

	lockKey := qk.Unique(sharedUniqueKey)
	storedID, err := rdb.Get(ctx, lockKey).Result()
	require.NoError(t, err)
	require.Equal(t, successfulTaskID, storedID, "Unique lock must be held by the single successful task ID")

	// Pre-create consumer group at 0 so messages enqueued prior to worker start are consumed
	_ = rdb.XGroupCreateMkStream(ctx, qk.Stream(), group, "0").Err()

	// Start worker to consume and complete the task
	var processedID string
	var processedMu sync.Mutex
	doneCh := make(chan struct{})

	w := NewTestWorkerPool(rdb, queue, group, "w-uk-1", 2, lc)
	w.Register("order.payment", func(c context.Context, t *task.Task) error {
		processedMu.Lock()
		processedID = t.ID
		processedMu.Unlock()
		close(doneCh)
		return nil
	})

	require.NoError(t, w.Start(ctx))
	defer w.Stop(ctx)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker to process unique task")
	}

	processedMu.Lock()
	require.Equal(t, successfulTaskID, processedID)
	processedMu.Unlock()

	// Verify the lock was automatically released after completion
	WaitKeyGone(t, ctx, rdb, lockKey, 3*time.Second)

	// Round 2: 100-goroutine stampede again after lock release
	var r2SuccessCount int64
	var r2DuplicateCount int64
	r2Barrier := make(chan struct{})
	var wg2 sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg2.Add(1)
		taskID := fmt.Sprintf("task-r2-stampede-%d", i)
		go func(id string) {
			defer wg2.Done()
			<-r2Barrier

			tsk := &task.Task{
				ID:          id,
				Queue:       queue,
				Name:        "order.payment",
				UniqueKey:   sharedUniqueKey,
				UniqueTTLMs: 30000,
			}
			err := cli.Enqueue(ctx, tsk)
			if err == nil {
				atomic.AddInt64(&r2SuccessCount, 1)
			} else if errors.Is(err, lifecycle.ErrDuplicateTask) {
				atomic.AddInt64(&r2DuplicateCount, 1)
			}
		}(taskID)
	}

	close(r2Barrier)
	wg2.Wait()

	require.Equal(t, int64(1), r2SuccessCount, "Round 2 must strictly allow exactly 1 task after previous task released lock")
	require.Equal(t, int64(concurrency-1), r2DuplicateCount, "Round 2 remaining 99 tasks must be rejected")
}

// TestTaskMQ_UniqueKey_ForeignLockProtection verifies that when a slow task finishes after
// its unique lock has already expired (or transferred) and been acquired by another task ID,
// the old task's settlement (CompleteTask / ReleaseUniqueLock) will NOT accidentally delete
// the new task's active lock (cascading lock release prevention).
func TestTaskMQ_UniqueKey_ForeignLockProtection(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "race_foreign_uk")
	qk := keys.KeysFor(queue)
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	b := broker.NewRedisBroker(rdb, codec.JSONCodec{}, DefaultTestLifecycle())

	sharedKey := "biz-key-cross-tenant"
	lockKey := qk.Unique(sharedKey)

	oldTask := &task.Task{
		ID:        "task-old-zombie",
		Queue:     queue,
		Name:      "sync.data",
		UniqueKey: sharedKey,
	}

	newTask := &task.Task{
		ID:        "task-new-legit",
		Queue:     queue,
		Name:      "sync.data",
		UniqueKey: sharedKey,
	}

	// Simulate: New task owns the lock now
	err := rdb.Set(ctx, lockKey, newTask.ID, 30*time.Second).Err()
	require.NoError(t, err)

	// Old zombie task finishes and attempts to release the unique lock
	err = b.ReleaseUniqueLock(ctx, oldTask)
	require.NoError(t, err)

	// Verify new task's lock is still intact in Redis
	currentLockVal, err := rdb.Get(ctx, lockKey).Result()
	require.NoError(t, err)
	require.Equal(t, newTask.ID, currentLockVal, "Foreign lock must NOT be deleted by old task ID")
}
