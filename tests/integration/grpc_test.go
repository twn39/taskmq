package integration

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/grpcserver"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
)

func TestTaskMQ_GRPCFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := UniqueQueue(t, "grpc")

	runChan := make(chan string, 1)

	var rdb goredis.UniversalClient
	var client mqclient.Client
	var worker mqworker.Worker
	var cfg *config.Config

	app := fxtest.New(t,
		mqclient.ProvideISP,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			mqclient.NewClient,
			func(c mqclient.Client) grpcserver.API { return c },
			grpcserver.NewGRPCServer,
			func(rdb goredis.UniversalClient, logger *zap.Logger) mqworker.Worker {
				pool := mqworker.NewWorkerPool(rdb, logger, queueName,
					mqworker.WithConcurrency(2),
				)
				pool.Register("task:grpc-test", func(ctx context.Context, task *taskmodel.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Invoke(taskmq.RegisterGRPCServerLifecycle),
		fx.Populate(&rdb, &client, &worker, &cfg),
	)

	// Clean up Redis
	FlushQueue(ctx, rdb, queueName)
	defer FlushQueue(ctx, rdb, queueName)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Establish gRPC Client Connection to localhost + dynamic port in config
	conn, err := grpc.Dial("localhost"+cfg.Server.GRPCPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	grpcClient := taskmqv1.NewTaskMQServiceClient(conn)

	// 2. Perform Enqueue via gRPC
	grpcTask := &taskmqv1.Task{
		Queue:   queueName,
		Name:    "task:grpc-test",
		Payload: []byte("grpc-payload"),
	}

	resp, err := grpcClient.Enqueue(ctx, &taskmqv1.EnqueueRequest{Task: grpcTask})
	if err != nil {
		t.Fatalf("gRPC Enqueue failed: %v", err)
	}
	assert.NotEmpty(t, resp.TaskId)

	// 3. Verify task is executed
	select {
	case result := <-runChan:
		assert.Equal(t, "grpc-payload", result)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task to execute via gRPC Enqueue")
	}

	// 4. EnqueueIn → delayed ZSET
	_, err = grpcClient.EnqueueIn(ctx, &taskmqv1.EnqueueInRequest{
		Task: &taskmqv1.Task{
			Id: "grpc-delayed-1", Queue: queueName, Name: "task:grpc-test", Payload: []byte("later"),
		},
		DelayMs: 3600_000,
	})
	assert.NoError(t, err)
	delayedN, err := rdb.ZCard(ctx, keys.KeysFor(queueName).Delayed()).Result()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, delayedN, int64(1))

	// 5. EnqueueBulk (drain results so shutdown is not waiting on in-flight work)
	bulk, err := grpcClient.EnqueueBulk(ctx, &taskmqv1.EnqueueBulkRequest{
		Tasks: []*taskmqv1.Task{
			{Id: "gb1", Queue: queueName, Name: "task:grpc-test", Payload: []byte("b1")},
			{Id: "gb2", Queue: queueName, Name: "task:grpc-test", Payload: []byte("b2")},
		},
	})
	require.NoError(t, err)
	require.Len(t, bulk.TaskIds, 2)
	for i := 0; i < 2; i++ {
		select {
		case <-runChan:
		case <-ctx.Done():
			t.Fatal("timeout draining bulk tasks")
		}
	}

	// 6. DLQ list + delete via gRPC (retry path covered in grpcserver unit tests).
	// Avoid RetryDeadLetter here: it re-enqueues into a live worker and races Fx stop.
	qk := keys.KeysFor(queueName)
	dead := taskmodel.NewTask("dead", []byte("d"), taskmodel.TaskOptions{ID: "grpc-dead-1", Queue: queueName})
	payload, err := codec.JSONCodec{}.Marshal(dead)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), goredis.Z{Score: float64(time.Now().UnixMilli()), Member: "grpc-dead-1"}).Err())
	require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), "grpc-dead-1", payload).Err())

	list, err := grpcClient.ListDeadLetters(ctx, &taskmqv1.ListDeadLettersRequest{Queue: queueName, Limit: 20})
	require.NoError(t, err)
	require.NotEmpty(t, list.Tasks)

	_, err = grpcClient.DeleteDeadLetter(ctx, &taskmqv1.DeleteDeadLetterRequest{Queue: queueName, TaskId: "grpc-dead-1"})
	require.NoError(t, err)
	exists, err := rdb.HExists(ctx, qk.DLQIndex(), "grpc-dead-1").Result()
	require.NoError(t, err)
	require.False(t, exists)

	// 7. Ops: pause / resume / isPaused
	_, err = grpcClient.PauseQueue(ctx, &taskmqv1.PauseQueueRequest{Queue: queueName})
	require.NoError(t, err)
	paused, err := grpcClient.IsQueuePaused(ctx, &taskmqv1.IsQueuePausedRequest{Queue: queueName})
	require.NoError(t, err)
	require.True(t, paused.Paused)
	_, err = grpcClient.ResumeQueue(ctx, &taskmqv1.ResumeQueueRequest{Queue: queueName})
	require.NoError(t, err)

	// 8. ListScheduled / ListActive / Cancel / GetTask
	sched, err := grpcClient.ListScheduledTasks(ctx, &taskmqv1.ListScheduledTasksRequest{Queue: queueName, Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, sched.Tasks)

	active, err := grpcClient.ListActiveTasks(ctx, &taskmqv1.ListActiveTasksRequest{Queue: queueName, Limit: 50})
	require.NoError(t, err)
	// stream may be empty if workers drained; still must succeed
	_ = active

	cancelID := "grpc-cancel-me"
	_, err = grpcClient.Enqueue(ctx, &taskmqv1.EnqueueRequest{
		Task: &taskmqv1.Task{Id: cancelID, Queue: queueName, Name: "task:grpc-test", Payload: []byte("c")},
	})
	require.NoError(t, err)
	// Drain or cancel before run — cancel marker is authoritative either way.
	_, err = grpcClient.CancelTask(ctx, &taskmqv1.CancelTaskRequest{Queue: queueName, TaskId: cancelID})
	require.NoError(t, err)
	n, err := rdb.Exists(ctx, qk.Cancelled(cancelID)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// GetTask after enqueue (meta may still exist if completed_retention or pending)
	_, err = grpcClient.GetTask(ctx, &taskmqv1.GetTaskRequest{Queue: queueName, TaskId: cancelID})
	require.NoError(t, err)
}
