package integration

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestTaskMQ_CLI_Operations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueName := "cli_integration_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	pausedKey := taskmq.PausedKey(queueName)
	delayedKey := taskmq.DelayedKey(queueName)
	dlqKey := taskmq.DLQKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
		),
		fx.Populate(&rdb, &client),
	)

	// Clean up keys before test
	rdb.Del(ctx, streamKey, pausedKey, delayedKey, dlqKey)
	defer rdb.Del(ctx, streamKey, pausedKey, delayedKey, dlqKey)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Compile the taskmq-cli binary dynamically to run subprocess tests
	tempDir, err := os.MkdirTemp("", "taskmq-cli-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	binaryPath := filepath.Join(tempDir, "taskmq-cli")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "../../cmd/taskmq-cli/main.go")
	// Make sure we run build in the integration directory context
	buildCmd.Dir = "."
	buildOutput, err := buildCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build taskmq-cli: %v\nOutput: %s", err, string(buildOutput))
	}

	// Helper to execute taskmq-cli command
	runCLI := func(args ...string) (string, error) {
		cmd := exec.Command(binaryPath, args...)
		cmd.Env = append(os.Environ(), "REDIS_ADDR=localhost:6379")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		runErr := cmd.Run()
		if runErr != nil {
			return stdout.String() + stderr.String(), runErr
		}
		return stdout.String(), nil
	}

	// 2. Test Pause command
	output, err := runCLI("pause", queueName)
	assert.NoError(t, err)
	assert.Contains(t, output, "paused successfully")

	// Verify in Redis
	isPaused, err := client.IsPaused(ctx, queueName)
	assert.NoError(t, err)
	assert.True(t, isPaused)

	// 3. Test Resume command
	output, err = runCLI("resume", queueName)
	assert.NoError(t, err)
	assert.Contains(t, output, "resumed successfully")

	// Verify in Redis
	isPaused, err = client.IsPaused(ctx, queueName)
	assert.NoError(t, err)
	assert.False(t, isPaused)

	// 4. Populate some tasks for statistics
	task1 := taskmq.NewTask("task:cli_test", []byte("1"), taskmq.TaskOptions{Queue: queueName})
	err = client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	task2 := taskmq.NewTask("task:cli_test", []byte("2"), taskmq.TaskOptions{Queue: queueName})
	err = client.EnqueueIn(ctx, task2, 5*time.Second)
	assert.NoError(t, err)

	// Put a mock dead letter task
	deadTask := taskmq.NewTask("task:cli_test", []byte("dead"), taskmq.TaskOptions{Queue: queueName})
	deadTask.ID = "test-dead-task-id"
	deadTask.Retry = 3
	deadTask.LastError = "connection failure"
	serializedDead, _ := taskmq.JSONCodec{}.Marshal(deadTask)
	err = rdb.ZAdd(ctx, dlqKey, goredis.Z{Score: float64(time.Now().UnixMilli()), Member: serializedDead}).Err()
	assert.NoError(t, err)

	// 5. Test stats command
	output, err = runCLI("stats")
	assert.NoError(t, err)
	assert.Contains(t, output, queueName)
	assert.Contains(t, output, "Active") // status
	// Validate counts are printed
	assert.True(t, strings.Contains(output, "1") || strings.Contains(output, "2"), "Should contain metrics in columns")

	// 6. Test dlq list command
	output, err = runCLI("dlq", "list", queueName)
	assert.NoError(t, err)
	assert.Contains(t, output, "test-dead-task-id")
	assert.Contains(t, output, "task:cli_test")
	assert.Contains(t, output, "connection failure")

	// 7. Test dlq retry command
	output, err = runCLI("dlq", "retry", queueName, "test-dead-task-id")
	assert.NoError(t, err)
	assert.Contains(t, output, "re-enqueued for retry")

	// Verify it was re-enqueued to active stream and removed from DLQ
	dlqCount, err := rdb.ZCard(ctx, dlqKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), dlqCount, "Task should be removed from DLQ")

	// Put it back to test DLQ deletion
	err = rdb.ZAdd(ctx, dlqKey, goredis.Z{Score: float64(time.Now().UnixMilli()), Member: serializedDead}).Err()
	assert.NoError(t, err)

	// 8. Test dlq delete command
	output, err = runCLI("dlq", "delete", queueName, "test-dead-task-id")
	assert.NoError(t, err)
	assert.Contains(t, output, "deleted from DLQ")

	// Verify it was deleted from DLQ
	dlqCount, err = rdb.ZCard(ctx, dlqKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), dlqCount, "Task should be deleted from DLQ")
}
