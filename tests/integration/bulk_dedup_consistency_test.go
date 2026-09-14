package integration

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// TestTaskMQ_EnqueueBulk_DeduplicationConsistency tests large-scale batch ingestion
// with complex overlapping deduplication scenarios:
// - 200 fresh unique tasks (must all succeed)
// - 200 tasks colliding with pre-existing unique keys already in Redis (must all fail)
// - 100 tasks containing intra-batch duplicates (50 unique keys repeated twice: exactly 50 succeed, 50 fail)
// Invariants:
// 1. Exactly 250 tasks succeed, and exactly 250 tasks fail with ErrDuplicateTask.
// 2. FailedIndexes strictly records all 250 conflicting positions.
// 3. Redis Stream length strictly increments by exactly 250.
// 4. Redis unique locks are precisely allocated without leaking or orphan locks.
func TestTaskMQ_EnqueueBulk_DeduplicationConsistency(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "bulk_dedup")
	qk := keys.KeysFor(queue)
	FlushQueue(ctx, rdb, queue)
	defer FlushQueue(ctx, rdb, queue)

	lc := DefaultTestLifecycle()
	cli := NewTestClient(rdb, lc)

	// Step 1: Pre-populate Redis with 200 existing tasks
	const preCount = 200
	preTasks := make([]*taskmodel.Task, preCount)
	for i := 0; i < preCount; i++ {
		preTasks[i] = &taskmodel.Task{
			ID:          fmt.Sprintf("pre-task-%d", i),
			Queue:       queue,
			Name:        "bulk.item",
			UniqueKey:   fmt.Sprintf("existing-uk-%d", i),
			UniqueTTLMs: 60000,
			Payload:     []byte(fmt.Sprintf(`{"idx":%d}`, i)),
		}
	}
	preRes, preErr := cli.EnqueueBulk(ctx, preTasks)
	require.NoError(t, preErr)
	require.Len(t, preRes.Succeeded, preCount)

	streamKey := qk.Stream()
	initialLen, err := rdb.XLen(ctx, streamKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(preCount), initialLen)

	// Step 2: Assemble 500 tasks with mixed conflicts:
	// - 0..199: 200 fresh unique tasks
	// - 200..399: 200 tasks conflicting with pre-existing keys
	// - 400..449: 50 intra-batch unique tasks (first occurrence)
	// - 450..499: 50 intra-batch duplicates of 400..449 (second occurrence)
	const freshCount = 200
	const conflictCount = 200
	const intraUniqueCount = 50
	const intraDuplicateCount = 50
	const totalBatch = freshCount + conflictCount + intraUniqueCount + intraDuplicateCount // 500

	batch := make([]*taskmodel.Task, totalBatch)

	for i := 0; i < freshCount; i++ {
		batch[i] = &taskmodel.Task{
			ID:          fmt.Sprintf("fresh-task-%d", i),
			Queue:       queue,
			Name:        "bulk.item",
			UniqueKey:   fmt.Sprintf("fresh-uk-%d", i),
			UniqueTTLMs: 60000,
		}
	}

	for i := 0; i < conflictCount; i++ {
		idx := freshCount + i
		batch[idx] = &taskmodel.Task{
			ID:          fmt.Sprintf("conflict-task-%d", i),
			Queue:       queue,
			Name:        "bulk.item",
			UniqueKey:   fmt.Sprintf("existing-uk-%d", i), // Conflicts with pre-existing!
			UniqueTTLMs: 60000,
		}
	}

	for i := 0; i < intraUniqueCount; i++ {
		idx := freshCount + conflictCount + i
		batch[idx] = &taskmodel.Task{
			ID:          fmt.Sprintf("intra-first-%d", i),
			Queue:       queue,
			Name:        "bulk.item",
			UniqueKey:   fmt.Sprintf("intra-uk-%d", i),
			UniqueTTLMs: 60000,
		}
	}

	for i := 0; i < intraDuplicateCount; i++ {
		idx := freshCount + conflictCount + intraUniqueCount + i
		batch[idx] = &taskmodel.Task{
			ID:          fmt.Sprintf("intra-second-%d", i),
			Queue:       queue,
			Name:        "bulk.item",
			UniqueKey:   fmt.Sprintf("intra-uk-%d", i), // Same UniqueKey as intra-first-i!
			UniqueTTLMs: 60000,
		}
	}

	// Step 3: Execute EnqueueBulk
	bulkRes, bulkErr := cli.EnqueueBulk(ctx, batch)
	require.Error(t, bulkErr, "Bulk with failures must return an error summarizing failures")
	require.NotNil(t, bulkRes)

	// Step 4: Verify Result Consistency
	const expectedSuccess = freshCount + intraUniqueCount        // 200 + 50 = 250
	const expectedFailures = conflictCount + intraDuplicateCount // 200 + 50 = 250

	require.Len(t, bulkRes.Succeeded, expectedSuccess, "Strictly 250 tasks must succeed")
	require.Len(t, bulkRes.FailedIndexes, expectedFailures, "Strictly 250 tasks must fail")

	// Verify FailedIndexes mappings
	failedSet := make(map[int]bool)
	for _, fIdx := range bulkRes.FailedIndexes {
		failedSet[fIdx] = true
		require.True(t, errors.Is(bulkRes.Errors[fIdx], lifecycle.ErrDuplicateTask),
			"failure at index %d must be ErrDuplicateTask, got: %v", fIdx, bulkRes.Errors[fIdx])
	}

	// All conflictCount tasks (indexes 200..399) must be in failedSet
	for i := freshCount; i < freshCount+conflictCount; i++ {
		require.True(t, failedSet[i], "pre-existing conflict at index %d should have failed", i)
	}

	// All intra-duplicate tasks (indexes 450..499) must be in failedSet
	for i := freshCount + conflictCount + intraUniqueCount; i < totalBatch; i++ {
		require.True(t, failedSet[i], "intra-batch duplicate at index %d should have failed", i)
	}

	// Step 5: Verify Redis Physical Stream State
	afterLen, err := rdb.XLen(ctx, streamKey).Result()
	require.NoError(t, err)
	expectedTotalStreamLen := int64(preCount + expectedSuccess) // 200 + 250 = 450
	require.Equal(t, expectedTotalStreamLen, afterLen, "Stream length must strictly equal 450")

	// Step 6: Verify Unique Lock states for a sample of keys
	// Sample fresh key must exist
	freshLockVal, err := rdb.Get(ctx, qk.Unique("fresh-uk-10")).Result()
	require.NoError(t, err)
	require.Equal(t, "fresh-task-10", freshLockVal)

	// Sample pre-existing key must still belong to the original pre-task, NOT the conflicting task
	preLockVal, err := rdb.Get(ctx, qk.Unique("existing-uk-10")).Result()
	require.NoError(t, err)
	require.Equal(t, "pre-task-10", preLockVal, "Original lock must not have been overwritten by conflicting task")
}
