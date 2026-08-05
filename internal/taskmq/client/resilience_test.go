package client

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
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// Client / broker must surface Redis errors when the connection is closed
// (Asynq-style resilience: fail fast, do not hang forever).

func TestClient_EnqueueAfterRedisClosed(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))
	ctx := context.Background()

	// Sanity: works while up.
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("j", []byte(`{}`), taskmodel.TaskOptions{
		Queue: "resilience-q", ID: "ok-1",
	})))

	require.NoError(t, rdb.Close())
	mr.Close()

	err = c.Enqueue(ctx, taskmodel.NewTask("j", []byte(`{}`), taskmodel.TaskOptions{
		Queue: "resilience-q", ID: "fail-1",
	}))
	require.Error(t, err)
}

func TestBroker_SettlementAfterRedisClosed(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	b := broker.NewRedisBroker(rdb, codec.JSONCodec{}, lifecycle.NewLifecycle(lifecycle.LifecycleConfig{}))
	ctx := context.Background()

	task := taskmodel.NewTask("j", []byte(`{}`), taskmodel.TaskOptions{Queue: "b-res", ID: "t1"})
	stream := keys.KeysFor("b-res").Stream()
	require.NoError(t, rdb.Close())
	mr.Close()

	require.Error(t, b.CompleteTask(ctx, task, stream, "1-0", "g"))
	require.Error(t, b.ScheduleRetry(ctx, task, stream, "1-0", "g", time.Now().Add(time.Second)))
	require.Error(t, b.MoveToDLQ(ctx, task, stream, "1-0", "g", "b-res"))
	require.Error(t, b.DeferRateLimitedTask(ctx, "1-0", task, "g", time.Now().Add(time.Second)))
}

func TestClient_ListOpsAfterRedisClosed(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))
	ctx := context.Background()

	require.NoError(t, rdb.Close())
	mr.Close()

	_, err = c.GetTaskInfo(ctx, "q", "id")
	require.Error(t, err)
	_, err = c.ListEvents(ctx, "q", 10)
	require.Error(t, err)
	_, err = c.ListScheduledTasks(ctx, "q", 10)
	require.Error(t, err)
	_, err = c.ListDeadLetters(ctx, "q", 10)
	require.Error(t, err)
	_, err = c.ListActiveTasks(ctx, "q", 10)
	require.Error(t, err)
	_, err = c.ListCronJobs(ctx, "q")
	require.Error(t, err)
	_, err = c.ListWorkers(ctx, "q")
	require.Error(t, err)
	require.Error(t, c.Pause(ctx, "q"))
	require.Error(t, c.Resume(ctx, "q"))
	require.Error(t, c.CancelTask(ctx, "q", "tid"))
	require.Error(t, c.EnqueueIn(ctx, taskmodel.NewTask("j", []byte(`{}`), taskmodel.TaskOptions{
		Queue: "q", ID: "late",
	}), time.Second))
}

func TestClient_EnqueueContextDeadline(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))

	// Already-expired context must fail fast.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err = c.Enqueue(ctx, taskmodel.NewTask("j", []byte(`{}`), taskmodel.TaskOptions{
		Queue: "deadline-q", ID: "d1",
	}))
	require.Error(t, err)
}

func TestClient_EnqueueBulkAfterRedisClosed(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))
	ctx := context.Background()

	require.NoError(t, rdb.Close())
	mr.Close()

	tasks := []*taskmodel.Task{
		taskmodel.NewTask("j", []byte(`1`), taskmodel.TaskOptions{Queue: "bulk-res", ID: "b1"}),
		taskmodel.NewTask("j", []byte(`2`), taskmodel.TaskOptions{Queue: "bulk-res", ID: "b2"}),
	}
	_, err = c.EnqueueBulk(ctx, tasks)
	require.Error(t, err)
}

func TestClient_ControlAndDLQAfterRedisClosed(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))
	ctx := context.Background()

	require.NoError(t, rdb.Close())
	mr.Close()

	require.Error(t, c.RetryDeadLetter(ctx, "q", "id"))
	require.Error(t, c.DeleteDeadLetter(ctx, "q", "id"))
	_, err = c.PurgeAllDeadLetters(ctx, "q")
	require.Error(t, err)
	require.Error(t, c.RegisterCron(ctx, "job", "0 * * * *", taskmodel.NewTask("j", nil, taskmodel.TaskOptions{Queue: "q"})))
}
