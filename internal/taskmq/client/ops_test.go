package client

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/heartbeat"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func setupClient(t *testing.T, lc *lifecycle.Lifecycle) (context.Context, redis.UniversalClient, Client, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	opts := []ClientOption{WithClientCodec(codec.JSONCodec{})}
	if lc != nil {
		opts = append(opts, WithClientLifecycle(lc))
	}
	c := NewClient(rdb, opts...)
	return context.Background(), rdb, c, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func TestClient_Enqueue_StreamAndMeta(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-enq"
	task := taskmodel.NewTask("job", []byte(`{"a":1}`), taskmodel.TaskOptions{
		ID: "e1", Queue: queue, MaxRetry: taskmodel.Ptr(2),
	})
	require.NoError(t, c.Enqueue(ctx, task))

	n, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	info, err := c.GetTaskInfo(ctx, queue, "e1")
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, meta.StatePending, info.State)

	// Facade Lifecycle accessor.
	f, ok := c.(*facade)
	require.True(t, ok)
	require.NotNil(t, f.Lifecycle())
}

func TestClient_Enqueue_UniqueDuplicate(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, _, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-uniq"
	mk := func(id string) *taskmodel.Task {
		t := taskmodel.NewTask("job", []byte(`{}`), taskmodel.TaskOptions{ID: id, Queue: queue})
		t.UniqueKey = "same"
		t.UniqueTTLMs = 60000
		return t
	}
	require.NoError(t, c.Enqueue(ctx, mk("u1")))
	err := c.Enqueue(ctx, mk("u2"))
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrDuplicateTask)
}

func TestClient_EnqueueIn_And_EnqueueAt(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-delay"
	qk := keys.KeysFor(queue)

	require.NoError(t, c.EnqueueIn(ctx, taskmodel.NewTask("job", []byte(`1`), taskmodel.TaskOptions{
		ID: "d-in", Queue: queue,
	}), 2*time.Second))

	require.NoError(t, c.EnqueueAt(ctx, taskmodel.NewTask("job", []byte(`2`), taskmodel.TaskOptions{
		ID: "d-at", Queue: queue,
	}), time.Now().Add(5*time.Second)))

	n, err := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	info, err := c.GetTaskInfo(ctx, queue, "d-in")
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, meta.StateDelayed, info.State)
}

func TestClient_Enqueue_PayloadTooLarge(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{MaxPayloadBytes: 4})
	ctx, _, c, cleanup := setupClient(t, lc)
	defer cleanup()

	err := c.Enqueue(ctx, taskmodel.NewTask("job", []byte("too-big"), taskmodel.TaskOptions{
		ID: "p1", Queue: "ops-payload",
	}))
	require.ErrorIs(t, err, lifecycle.ErrPayloadTooLarge)
}

func TestClient_Enqueue_HardLimit(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{EnqueueHardLimit: 1})
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-hard"
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`1`), taskmodel.TaskOptions{
		ID: "h1", Queue: queue,
	})))
	// Fill stream to hard limit already; second enqueue should fail.
	// First enqueue already at hard=1, so second fails.
	err := c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`2`), taskmodel.TaskOptions{
		ID: "h2", Queue: queue,
	}))
	require.ErrorIs(t, err, lifecycle.ErrQueueFull)

	n, _ := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.Equal(t, int64(1), n)
}

func TestClient_Control_PauseResumeCancel(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-ctl"
	qk := keys.KeysFor(queue)

	paused, err := c.IsPaused(ctx, queue)
	require.NoError(t, err)
	require.False(t, paused)

	require.NoError(t, c.Pause(ctx, queue))
	paused, err = c.IsPaused(ctx, queue)
	require.NoError(t, err)
	require.True(t, paused)
	n, err := rdb.Exists(ctx, qk.Paused()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	require.NoError(t, c.Resume(ctx, queue))
	paused, err = c.IsPaused(ctx, queue)
	require.NoError(t, err)
	require.False(t, paused)

	require.NoError(t, c.CancelTask(ctx, queue, "tid-1"))
	exists, err := rdb.Exists(ctx, qk.Cancelled("tid-1")).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), exists)
}

func TestClient_DLQ_ListRetryDeletePurge(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-dlq"
	qk := keys.KeysFor(queue)
	codecJ := codec.JSONCodec{}

	seed := func(id string, score int64) {
		task := taskmodel.NewTask("job", []byte(`dlq`), taskmodel.TaskOptions{ID: id, Queue: queue})
		task.LastError = "boom"
		ser, err := codecJ.Marshal(task)
		require.NoError(t, err)
		require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{Score: float64(score), Member: id}).Err())
		require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), id, string(ser)).Err())
	}
	seed("dead-1", 100)
	seed("dead-2", 200)

	listed, err := c.ListDeadLetters(ctx, queue, 10)
	require.NoError(t, err)
	require.Len(t, listed, 2)
	// ZRevRange → newest first
	require.Equal(t, "dead-2", listed[0].ID)

	require.NoError(t, c.RetryDeadLetter(ctx, queue, "dead-1"))
	// Removed from DLQ and re-enqueued to stream.
	n, err := rdb.ZCard(ctx, qk.DLQ()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	streamLen, err := rdb.XLen(ctx, qk.Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), streamLen)

	require.NoError(t, c.DeleteDeadLetter(ctx, queue, "dead-2"))
	n, err = rdb.ZCard(ctx, qk.DLQ()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	// Re-seed for RetryAll + Purge.
	seed("dead-3", 300)
	seed("dead-4", 400)
	retried, err := c.RetryAllDeadLetters(ctx, queue)
	require.NoError(t, err)
	require.Equal(t, int64(2), retried)
	n, _ = rdb.ZCard(ctx, qk.DLQ()).Result()
	require.Equal(t, int64(0), n)

	seed("dead-5", 500)
	purged, err := c.PurgeAllDeadLetters(ctx, queue)
	require.NoError(t, err)
	require.Equal(t, int64(1), purged)

	// Not found path.
	err = c.RetryDeadLetter(ctx, queue, "missing")
	require.Error(t, err)
}

func TestClient_Cron_RegisterListRunDelete(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-cron"
	task := taskmodel.NewTask("unused", []byte(`cron-payload`), taskmodel.TaskOptions{Queue: queue})
	require.NoError(t, c.RegisterCron(ctx, "job-a", "0 * * * *", task))

	// Invalid spec.
	err := c.RegisterCron(ctx, "bad", "not a cron", taskmodel.NewTask("x", nil, taskmodel.TaskOptions{Queue: queue}))
	require.Error(t, err)

	jobs, err := c.ListCronJobs(ctx, queue)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "job-a", jobs[0].JobName)
	require.Equal(t, "0 * * * *", jobs[0].Task.CronSpec)
	require.False(t, jobs[0].NextRunTime.IsZero())

	// Delayed schedule entry created for first run.
	n, err := rdb.ZCard(ctx, keys.KeysFor(queue).Delayed()).Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, int64(1))

	require.NoError(t, c.RunCronJob(ctx, queue, "job-a"))
	streamLen, err := rdb.XLen(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, streamLen, int64(1))

	require.NoError(t, c.DeleteCronJob(ctx, queue, "job-a"))
	jobs, err = c.ListCronJobs(ctx, queue)
	require.NoError(t, err)
	require.Empty(t, jobs)

	err = c.RunCronJob(ctx, queue, "missing")
	require.Error(t, err)
	err = c.DeleteCronJob(ctx, queue, "missing")
	require.Error(t, err)
}

func TestClient_Scheduled_ListRunDelete(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-sched"
	qk := keys.KeysFor(queue)

	require.NoError(t, c.EnqueueAt(ctx, taskmodel.NewTask("job", []byte(`s`), taskmodel.TaskOptions{
		ID: "s1", Queue: queue,
	}), time.Now().Add(time.Hour)))

	// Unique delayed so Delete can clear lock.
	u := taskmodel.NewTask("job", []byte(`u`), taskmodel.TaskOptions{ID: "s2", Queue: queue})
	u.UniqueKey = "uk-s2"
	u.UniqueTTLMs = 60000
	require.NoError(t, c.EnqueueAt(ctx, u, time.Now().Add(2*time.Hour)))

	listed, err := c.ListScheduledTasks(ctx, queue, 10)
	require.NoError(t, err)
	require.Len(t, listed, 2)
	require.False(t, listed[0].RunAt.IsZero())

	require.NoError(t, c.RunScheduledTask(ctx, queue, "s1"))
	// Promoted to stream.
	streamLen, err := rdb.XLen(ctx, qk.Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), streamLen)
	delayed, err := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), delayed)

	require.NoError(t, c.DeleteScheduledTask(ctx, queue, "s2"))
	delayed, _ = rdb.ZCard(ctx, qk.Delayed()).Result()
	require.Equal(t, int64(0), delayed)
	// Unique lock released.
	exists, _ := rdb.Exists(ctx, qk.Unique("uk-s2")).Result()
	require.Equal(t, int64(0), exists)

	err = c.RunScheduledTask(ctx, queue, "gone")
	require.Error(t, err)
	err = c.DeleteScheduledTask(ctx, queue, "gone")
	require.Error(t, err)
}

func TestClient_Active_ListAndDelete(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-active"
	qk := keys.KeysFor(queue)
	group := "g1"

	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`a`), taskmodel.TaskOptions{
		ID: "a1", Queue: queue,
	})))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`b`), taskmodel.TaskOptions{
		ID: "a2", Queue: queue,
	})))

	// Claim one into PEL so ListActive shows Processing.
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, qk.Stream(), group, "0").Err())
	msgs, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "c1", Streams: []string{qk.Stream(), ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0].Messages, 1)
	processingID := msgs[0].Messages[0].ID

	actives, err := c.ListActiveTasks(ctx, queue, 10)
	require.NoError(t, err)
	require.Len(t, actives, 2)
	var sawProcessing, sawPending bool
	for _, a := range actives {
		if a.StreamID == processingID {
			require.Equal(t, "Processing", a.Status)
			require.Equal(t, "c1", a.Consumer)
			sawProcessing = true
		} else {
			require.Equal(t, "Pending", a.Status)
			sawPending = true
		}
		require.False(t, a.EnqueuedAt.IsZero())
	}
	require.True(t, sawProcessing)
	require.True(t, sawPending)

	require.NoError(t, c.DeleteActiveTask(ctx, queue, processingID))
	n, err := rdb.XLen(ctx, qk.Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	err = c.DeleteActiveTask(ctx, queue, "999-0")
	require.Error(t, err)
}

func TestClient_Inspect_ProgressAndWorkers(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, rdb, c, cleanup := setupClient(t, lc)
	defer cleanup()

	queue := "ops-insp"
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`{}`), taskmodel.TaskOptions{
		ID: "i1", Queue: queue,
	})))

	require.NoError(t, c.UpdateProgress(ctx, queue, "i1", 40, "halfway"))
	info, err := c.GetTaskInfo(ctx, queue, "i1")
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, 40, info.Progress)
	require.Equal(t, "halfway", info.ProgressData)

	// Seed a live heartbeat key (SCAN pattern) for ListWorkers.
	key := keys.KeysFor(queue).Heartbeat("worker-1")
	require.NoError(t, rdb.Set(ctx, key, `{"queue":"ops-insp","consumer":"worker-1","concurrency":4,"host":"h","pid":1}`, time.Minute).Err())
	// Exercise reporter construct path as well.
	_ = heartbeat.NewReporter(rdb, queue, "worker-2", 2)

	workers, err := c.ListWorkers(ctx, queue)
	require.NoError(t, err)
	require.NotEmpty(t, workers)
	require.Equal(t, "worker-1", workers[0].Consumer)

	evs, err := c.ListEvents(ctx, queue, 20)
	require.NoError(t, err)
	require.NotEmpty(t, evs) // enqueued + progress
}

func TestClient_Options_DeadlineAndUniqueTTL(t *testing.T) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	ctx, _, c, cleanup := setupClient(t, lc)
	defer cleanup()

	// Client-level default unique TTL when task.UniqueTTLMs is unset.
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	c2 := NewClient(rdb,
		WithClientCodec(codec.JSONCodec{}),
		WithClientLifecycle(lc),
		WithDefaultUniqueTTL(45*time.Second),
	)

	task := taskmodel.NewTask("job", []byte(`{}`), taskmodel.TaskOptions{ID: "opt1", Queue: "ops-opt"})
	task.UniqueKey = "uk-opt"
	// UniqueTTLMs left 0 → deps.uniqueTTL uses WithDefaultUniqueTTL.
	require.NoError(t, c2.Enqueue(ctx, task, WithTaskDeadline(time.Now().Add(time.Minute))))
	require.NotZero(t, task.DeadlineMs)
	require.Equal(t, "uk-opt", task.UniqueKey)
	// Lock key exists with client-default TTL.
	exists, err := rdb.Exists(ctx, keys.KeysFor("ops-opt").Unique("uk-opt")).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), exists)

	// WithTaskDeadline + WithTaskUnique on facade client.
	task2 := taskmodel.NewTask("job", []byte(`{}`), taskmodel.TaskOptions{ID: "opt2", Queue: "ops-opt2"})
	require.NoError(t, c.Enqueue(ctx, task2,
		WithTaskDeadline(time.Now().Add(30*time.Second)),
		WithTaskUnique("uk-2", 30*time.Second, taskmodel.UniqueUntilStart),
	))
	require.NotZero(t, task2.DeadlineMs)
	require.Equal(t, "uk-2", task2.UniqueKey)
}

func TestParseStreamTime(t *testing.T) {
	ts := parseStreamTime("1700000000000-0")
	require.Equal(t, int64(1700000000000), ts.UnixMilli())
	require.True(t, parseStreamTime("bad").IsZero())
}
