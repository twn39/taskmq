package integration

import (
	"context"
	"testing"
	"time"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/logger"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/grpcserver"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	internalredis "github.com/twn39/taskmq/internal/redis"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
)

func TestTaskMQ_GRPCFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "grpc_test_queue"
	streamKey := keys.StreamKey(queueName)

	runChan := make(chan string, 1)

	var rdb *goredis.Client
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
			func(rdb *goredis.Client, logger *zap.Logger) mqworker.Worker {
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
	err := rdb.Del(ctx, streamKey).Err()
	assert.NoError(t, err)

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
}
