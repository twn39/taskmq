package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// TestChaos_MalformedPayload_Isolation verifies that when invalid/corrupt stream data
// (e.g. non-JSON payload bytes) is manually injected into the Redis Stream,
// the worker handles the unmarshal failure gracefully without dropping other valid tasks.
func TestChaos_MalformedPayload_Isolation(t *testing.T) {
	ctx, cancel, rdb := RequireRedis(t)
	defer cancel()

	queue := UniqueQueue(t, "chaos-malformed")
	qk := keys.KeysFor(queue)
	lc := DefaultTestLifecycle()
	client := NewTestClient(rdb, lc)

	var validProcessed atomic.Bool

	pool := NewTestWorkerPool(rdb, queue, "group-corrupt", "consumer-corrupt", 1, lc)
	pool.Register("task:normal", func(taskCtx context.Context, task *taskmodel.Task) error {
		validProcessed.Store(true)
		return nil
	})

	// 1. Manually inject a completely malformed message into the Redis stream
	stream := qk.Stream()
	group := "group-corrupt"
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"task": "invalid-non-json-binary-junk-{{{"},
	}).Result()
	require.NoError(t, err)

	// 2. Enqueue a normal valid task via official client
	normalTask := taskmodel.NewTask("task:normal", []byte(`{"ok":true}`), taskmodel.TaskOptions{
		ID:    "task-normal-100",
		Queue: queue,
	})
	require.NoError(t, client.Enqueue(ctx, normalTask))

	// 3. Start worker pool
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	// 4. Verify worker processes the valid task despite encountering malformed data
	WaitUntil(t, func() bool {
		return validProcessed.Load()
	}, 10*time.Second, 40*time.Millisecond, "valid task was blocked by malformed stream payload")
}
