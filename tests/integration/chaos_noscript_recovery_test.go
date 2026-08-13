package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// TestChaos_ScriptFlush_NOSCRIPT_Recovery verifies that if `SCRIPT FLUSH` is executed
// on Redis while the application is running, pre-warmed scripts automatically reload
// or fallback cleanly without failing future enqueue operations.
func TestChaos_ScriptFlush_NOSCRIPT_Recovery(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "chaos-noscript")
	lc := DefaultTestLifecycle()
	client := NewTestClient(rdb, lc)

	// Pre-warm scripts
	require.NoError(t, lifecycle.LoadScripts(ctx, rdb))

	// 1. Initial Enqueue
	task1 := taskmodel.NewTask("job:pre-flush", []byte(`{}`), taskmodel.TaskOptions{ID: "t1", Queue: queue})
	require.NoError(t, client.Enqueue(ctx, task1))

	// 2. Issue SCRIPT FLUSH on Redis to simulate script cache purge (NOSCRIPT condition)
	require.NoError(t, rdb.ScriptFlush(ctx).Err())

	// 3. Enqueue after SCRIPT FLUSH — must succeed by automatically reloading or falling back
	task2 := taskmodel.NewTask("job:post-flush", []byte(`{}`), taskmodel.TaskOptions{ID: "t2", Queue: queue})
	require.NoError(t, client.Enqueue(ctx, task2))

	// 4. EnqueueBulk after SCRIPT FLUSH
	tasksBulk := []*taskmodel.Task{
		taskmodel.NewTask("job:bulk-1", []byte(`{}`), taskmodel.TaskOptions{ID: "tb1", Queue: queue}),
		taskmodel.NewTask("job:bulk-2", []byte(`{}`), taskmodel.TaskOptions{ID: "tb2", Queue: queue}),
	}
	res, err := client.EnqueueBulk(ctx, tasksBulk)
	require.NoError(t, err)
	require.Len(t, res.FailedIndexes, 0)
}
