package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

func TestTaskMQ_MVPFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "mvp_test_queue"
	streamKey := fmt.Sprintf("taskmq:queue:%s", queueName)

	type WelcomeEmail struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}

	runChan := make(chan *WelcomeEmail, 1)

	var rdb *goredis.Client
	var client *taskmq.Client
	var worker *taskmq.WorkerPool

	// Initialize Fx App for testing
	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			// Custom WorkerPool constructor for test queue name
			func(rdb *goredis.Client, logger *zap.Logger) *taskmq.WorkerPool {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Group:       "test-group",
					Consumer:    "test-consumer",
					Concurrency: 2,
				})
				// Register handler
				pool.Register("email:welcome", func(ctx context.Context, task *taskmq.Task) error {
					var email WelcomeEmail
					if err := json.Unmarshal(task.Payload, &email); err != nil {
						return err
					}
					runChan <- &email
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up old keys
	rdb.Del(ctx, streamKey)
	defer rdb.Del(ctx, streamKey)

	// Clean up Redis keys before running
	app.RequireStart()
	defer app.RequireStop()

	// 5. Enqueue Task
	emailData := WelcomeEmail{
		Email: "test@example.com",
		Name:  "John Doe",
	}
	payloadBytes, err := json.Marshal(emailData)
	assert.NoError(t, err)

	task := taskmq.NewTask("email:welcome", payloadBytes, taskmq.TaskOptions{
		Queue: queueName,
	})

	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// 6. Assert task execution
	select {
	case result := <-runChan:
		assert.Equal(t, "test@example.com", result.Email)
		assert.Equal(t, "John Doe", result.Name)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task execution")
	}

	// Wait briefly to allow ACK to complete
	time.Sleep(100 * time.Millisecond)

	// 7. Verify task has been ACKed (no pending messages in group)
	pending, err := rdb.XPending(ctx, streamKey, "test-group").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "Pending count should be 0 after successful execution and ACK")
}
