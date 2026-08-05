package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestEnqueueBulk_Pipeline(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))
	ctx := context.Background()
	queue := "bulk-q"
	var tasks []*taskmodel.Task
	for i := 0; i < 20; i++ {
		tasks = append(tasks, taskmodel.NewTask("job", []byte(fmt.Sprintf(`{"n":%d}`, i)), taskmodel.TaskOptions{
			Queue: queue,
		}))
	}
	res, err := c.EnqueueBulk(ctx, tasks)
	require.NoError(t, err)
	require.Len(t, res.Succeeded, 20)
	n, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(20), n)
	evs, err := c.ListEvents(ctx, queue, 50)
	require.NoError(t, err)
	require.NotEmpty(t, evs)
}

func TestEnqueueBulk_NilTaskContinueAndFailFast(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))
	ctx := context.Background()
	queue := "bulk-partial"
	ok1 := taskmodel.NewTask("job", []byte(`1`), taskmodel.TaskOptions{Queue: queue, ID: "ok-1"})
	ok2 := taskmodel.NewTask("job", []byte(`2`), taskmodel.TaskOptions{Queue: queue, ID: "ok-2"})

	// Continue on error (default): nil entry fails, others succeed.
	res, err := c.EnqueueBulk(ctx, []*taskmodel.Task{ok1, nil, ok2})
	require.Error(t, err)
	require.Contains(t, err.Error(), "1 failure")
	require.Len(t, res.Succeeded, 2)
	require.Equal(t, []int{1}, res.FailedIndexes)
	require.Error(t, res.Errors[1])

	// Fail-fast: stops at first nil.
	ok3 := taskmodel.NewTask("job", []byte(`3`), taskmodel.TaskOptions{Queue: queue, ID: "ok-3"})
	ok4 := taskmodel.NewTask("job", []byte(`4`), taskmodel.TaskOptions{Queue: queue, ID: "ok-4"})
	res2, err := c.EnqueueBulk(ctx, []*taskmodel.Task{nil, ok3, ok4}, WithBulkFailFast(true))
	require.Error(t, err)
	require.Empty(t, res2.Succeeded)
	require.Equal(t, []int{0}, res2.FailedIndexes)
}

func TestEnqueueBulk_PayloadLimitPartial(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{MaxPayloadBytes: 8})
	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}), WithClientLifecycle(lc))
	ctx := context.Background()
	queue := "bulk-payload"
	small := taskmodel.NewTask("job", []byte("tiny"), taskmodel.TaskOptions{Queue: queue, ID: "small"})
	large := taskmodel.NewTask("job", []byte("this-is-too-large-for-limit"), taskmodel.TaskOptions{Queue: queue, ID: "large"})
	small2 := taskmodel.NewTask("job", []byte("ok"), taskmodel.TaskOptions{Queue: queue, ID: "small2"})

	res, err := c.EnqueueBulk(ctx, []*taskmodel.Task{small, large, small2})
	require.Error(t, err)
	require.Contains(t, res.FailedIndexes, 1)
	require.Len(t, res.Succeeded, 2)
	n, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
}

func TestEnqueueBulk_LargeChunkedAndUnique(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))
	ctx := context.Background()
	queue := "large-bulk-q"

	// 1500 tasks (exceeds maxBulkChunkSize 1000, testing chunking logic + unique keys)
	var tasks []*taskmodel.Task
	for i := 0; i < 1500; i++ {
		opts := taskmodel.TaskOptions{Queue: queue, ID: fmt.Sprintf("t-%d", i)}
		if i%2 == 0 {
			opts.UniqueKey = fmt.Sprintf("unique-key-%d", i)
		}
		tasks = append(tasks, taskmodel.NewTask("job", []byte(fmt.Sprintf(`{"n":%d}`, i)), opts))
	}

	res, err := c.EnqueueBulk(ctx, tasks)
	require.NoError(t, err)
	require.Len(t, res.Succeeded, 1500)
	require.Empty(t, res.FailedIndexes)

	n, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1500), n)
}
