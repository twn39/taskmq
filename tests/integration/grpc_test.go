package integration

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestTaskMQ_GRPCFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "grpc_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	runChan := make(chan string, 1)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			taskmq.NewGRPCServer,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				opts := taskmq.NewDefaultWorkerOptions(rdb, logger, queueName, taskmq.JSONCodec{}, taskmq.WorkerOptions{
					Concurrency: 2,
				})
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, opts)
				pool.Register("task:grpc-test", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Invoke(taskmq.RegisterGRPCServerLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	err := rdb.Del(ctx, streamKey).Err()
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Establish gRPC Client Connection to localhost:50051 (default port in config)
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
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
}
