package grpcserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/grpcserver"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setupAPI(t *testing.T) (context.Context, redis.UniversalClient, *grpcserver.GRPCServer, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{DLQMaxCount: 100})
	api := mqclient.NewClient(rdb,
		mqclient.WithClientCodec(codec.JSONCodec{}),
		mqclient.WithClientLifecycle(lc),
	)
	srv := grpcserver.NewGRPCServer(api, zap.NewNop())
	return context.Background(), rdb, srv, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func protoTask(queue, name, id string, payload []byte) *taskmqv1.Task {
	return &taskmqv1.Task{
		Id: id, Queue: queue, Name: name, Payload: payload,
		MaxRetry: 3, TimeoutMs: 5000,
	}
}

func TestGRPC_InvalidArgumentNilTask(t *testing.T) {
	ctx, _, srv, cleanup := setupAPI(t)
	defer cleanup()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"Enqueue", func() error {
			_, err := srv.Enqueue(ctx, &taskmqv1.EnqueueRequest{})
			return err
		}},
		{"EnqueueIn", func() error {
			_, err := srv.EnqueueIn(ctx, &taskmqv1.EnqueueInRequest{})
			return err
		}},
		{"EnqueueAt", func() error {
			_, err := srv.EnqueueAt(ctx, &taskmqv1.EnqueueAtRequest{})
			return err
		}},
		{"RegisterCron", func() error {
			_, err := srv.RegisterCron(ctx, &taskmqv1.RegisterCronRequest{JobName: "j", CronSpec: "* * * * *"})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestGRPC_MethodMatrix_EnqueueVariantsAndDLQ(t *testing.T) {
	ctx, rdb, srv, cleanup := setupAPI(t)
	defer cleanup()

	queue := "grpc-unit-q"
	qk := keys.KeysFor(queue)

	// Enqueue immediate
	resp, err := srv.Enqueue(ctx, &taskmqv1.EnqueueRequest{
		Task: protoTask(queue, "job", "e1", []byte(`immed`)),
	})
	require.NoError(t, err)
	require.Equal(t, "e1", resp.TaskId)
	n, err := rdb.XLen(ctx, qk.Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// EnqueueIn
	resp, err = srv.EnqueueIn(ctx, &taskmqv1.EnqueueInRequest{
		Task:    protoTask(queue, "job", "d1", []byte(`del`)),
		DelayMs: 60_000,
	})
	require.NoError(t, err)
	require.Equal(t, "d1", resp.TaskId)
	delayed, err := rdb.ZCard(ctx, qk.Delayed()).Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, delayed, int64(1))

	// EnqueueAt
	at := time.Now().Add(2 * time.Hour).UnixMilli()
	resp, err = srv.EnqueueAt(ctx, &taskmqv1.EnqueueAtRequest{
		Task:        protoTask(queue, "job", "a1", []byte(`at`)),
		TimestampMs: at,
	})
	require.NoError(t, err)
	require.Equal(t, "a1", resp.TaskId)

	// EnqueueBulk
	bulk, err := srv.EnqueueBulk(ctx, &taskmqv1.EnqueueBulkRequest{
		Tasks: []*taskmqv1.Task{
			protoTask(queue, "job", "b1", []byte(`1`)),
			protoTask(queue, "job", "b2", []byte(`2`)),
		},
	})
	require.NoError(t, err)
	require.Len(t, bulk.TaskIds, 2)

	// Empty bulk
	empty, err := srv.EnqueueBulk(ctx, &taskmqv1.EnqueueBulkRequest{})
	require.NoError(t, err)
	require.Empty(t, empty.TaskIds)

	// Bulk with nil task
	_, err = srv.EnqueueBulk(ctx, &taskmqv1.EnqueueBulkRequest{
		Tasks: []*taskmqv1.Task{nil},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// RegisterCron
	cronResp, err := srv.RegisterCron(ctx, &taskmqv1.RegisterCronRequest{
		JobName:  "nightly",
		CronSpec: "0 0 * * *",
		Task:     protoTask(queue, "cron-job", "", []byte(`{}`)),
	})
	require.NoError(t, err)
	require.True(t, cronResp.Success)

	// Seed DLQ entry for list/retry/delete
	dead := taskmodel.NewTask("dead", []byte(`x`), taskmodel.TaskOptions{ID: "dead-1", Queue: queue})
	ser, err := codec.JSONCodec{}.Marshal(dead)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{Score: float64(time.Now().UnixMilli()), Member: dead.ID}).Err())
	require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), dead.ID, ser).Err())

	list, err := srv.ListDeadLetters(ctx, &taskmqv1.ListDeadLettersRequest{Queue: queue, Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, list.Tasks)

	// Retry moves back to stream
	retryResp, err := srv.RetryDeadLetter(ctx, &taskmqv1.RetryDeadLetterRequest{Queue: queue, TaskId: dead.ID})
	require.NoError(t, err)
	require.True(t, retryResp.Success)

	// Re-seed and delete
	require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{Score: float64(time.Now().UnixMilli()), Member: "dead-2"}).Err())
	require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), "dead-2", ser).Err())
	delResp, err := srv.DeleteDeadLetter(ctx, &taskmqv1.DeleteDeadLetterRequest{Queue: queue, TaskId: "dead-2"})
	require.NoError(t, err)
	require.True(t, delResp.Success)
	exists, err := rdb.HExists(ctx, qk.DLQIndex(), "dead-2").Result()
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGRPC_OpsPauseCancelInspect(t *testing.T) {
	ctx, rdb, srv, cleanup := setupAPI(t)
	defer cleanup()

	queue := "grpc-ops-q"
	qk := keys.KeysFor(queue)

	// Invalid args
	_, err := srv.PauseQueue(ctx, &taskmqv1.PauseQueueRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = srv.CancelTask(ctx, &taskmqv1.CancelTaskRequest{Queue: queue})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = srv.GetTask(ctx, &taskmqv1.GetTaskRequest{Queue: queue})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = srv.ListScheduledTasks(ctx, &taskmqv1.ListScheduledTasksRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = srv.ListActiveTasks(ctx, &taskmqv1.ListActiveTasksRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// Pause / IsPaused / Resume
	pr, err := srv.PauseQueue(ctx, &taskmqv1.PauseQueueRequest{Queue: queue})
	require.NoError(t, err)
	require.True(t, pr.Success)
	paused, err := srv.IsQueuePaused(ctx, &taskmqv1.IsQueuePausedRequest{Queue: queue})
	require.NoError(t, err)
	require.True(t, paused.Paused)
	rr, err := srv.ResumeQueue(ctx, &taskmqv1.ResumeQueueRequest{Queue: queue})
	require.NoError(t, err)
	require.True(t, rr.Success)
	paused, err = srv.IsQueuePaused(ctx, &taskmqv1.IsQueuePausedRequest{Queue: queue})
	require.NoError(t, err)
	require.False(t, paused.Paused)

	// Enqueue + ListActive
	_, err = srv.Enqueue(ctx, &taskmqv1.EnqueueRequest{
		Task: protoTask(queue, "job", "act-1", []byte(`a`)),
	})
	require.NoError(t, err)
	active, err := srv.ListActiveTasks(ctx, &taskmqv1.ListActiveTasksRequest{Queue: queue, Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, active.Tasks)

	// Delayed + ListScheduled
	_, err = srv.EnqueueIn(ctx, &taskmqv1.EnqueueInRequest{
		Task:    protoTask(queue, "job", "sched-1", []byte(`s`)),
		DelayMs: 3600_000,
	})
	require.NoError(t, err)
	sched, err := srv.ListScheduledTasks(ctx, &taskmqv1.ListScheduledTasksRequest{Queue: queue, Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, sched.Tasks)
	foundSched := false
	for _, st := range sched.Tasks {
		if st.Task != nil && st.Task.Id == "sched-1" {
			foundSched = true
			require.Greater(t, st.RunAtMs, int64(0))
		}
	}
	require.True(t, foundSched)

	// Cancel
	cr, err := srv.CancelTask(ctx, &taskmqv1.CancelTaskRequest{Queue: queue, TaskId: "act-1"})
	require.NoError(t, err)
	require.True(t, cr.Success)
	n, err := rdb.Exists(ctx, qk.Cancelled("act-1")).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// GetTask not found
	gt, err := srv.GetTask(ctx, &taskmqv1.GetTaskRequest{Queue: queue, TaskId: "missing"})
	require.NoError(t, err)
	require.False(t, gt.Found)

	// Enqueue writes meta → GetTask found
	_, err = srv.Enqueue(ctx, &taskmqv1.EnqueueRequest{
		Task: protoTask(queue, "job", "meta-1", []byte(`m`)),
	})
	require.NoError(t, err)
	gt, err = srv.GetTask(ctx, &taskmqv1.GetTaskRequest{Queue: queue, TaskId: "meta-1"})
	require.NoError(t, err)
	require.True(t, gt.Found)
	require.Equal(t, "meta-1", gt.Task.Id)
	require.NotEmpty(t, gt.Task.State)
}
