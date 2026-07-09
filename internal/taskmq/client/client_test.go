package client

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestClient_TaskOptions(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	client := NewClient(rdb)

	t.Run("Option execution success - options correctly applied", func(t *testing.T) {
		task := taskmodel.NewTask("test:option", []byte("payload"))

		err := client.Enqueue(context.Background(), task,
			WithTaskID("custom-id-123"),
			WithTaskMaxRetry(7),
			WithTaskTimeout(10*time.Second),
			WithTaskQueue("custom-queue"),
			WithTaskGroupKey("my-group"),
			WithTaskUnique("my-unique-key", 30*time.Second, taskmodel.UniqueUntilStart),
		)

		assert.NoError(t, err)
		assert.Equal(t, "custom-id-123", task.ID)
		assert.Equal(t, 7, task.MaxRetry)
		assert.Equal(t, 10000, task.TimeoutMs)
		assert.Equal(t, "custom-queue", task.Queue)
		assert.Equal(t, "my-group", task.GroupKey)
		assert.Equal(t, "my-unique-key", task.UniqueKey)
		assert.Equal(t, 30000, task.UniqueTTLMs)
		assert.Equal(t, taskmodel.UniqueUntilStart, task.UniqueScope)
	})

	t.Run("Option execution failure - validation error returned", func(t *testing.T) {
		task := taskmodel.NewTask("test:option:fail", []byte("payload"))

		err := client.Enqueue(context.Background(), task, WithTaskMaxRetry(-3))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max retry must be non-negative")

		err = client.Enqueue(context.Background(), task, WithTaskTimeout(0))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout must be positive")

		err = client.Enqueue(context.Background(), task, WithTaskUnique("", 10*time.Second, taskmodel.UniqueUntilSucceeded))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unique key cannot be empty")

		err = client.Enqueue(context.Background(), task, WithTaskQueue(""))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "queue name cannot be empty")

		err = client.Enqueue(context.Background(), task, WithTaskID(""))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "task ID cannot be empty")
	})
}
