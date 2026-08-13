package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestNewTask_WithOptions(t *testing.T) {
	now := time.Now()
	past := now.Add(-10 * time.Second)

	opts := taskmodel.TaskOptions{
		ID:        "custom-task-001",
		Queue:     "high-priority",
		MaxRetry:  taskmodel.Ptr(5),
		Timeout:   15 * time.Second,
		UniqueKey: "uniq-user-42",
		UniqueTTL: 60 * time.Second,
		GroupKey:  "group-email",
		Deadline:  past,
	}

	task := taskmodel.NewTask("task:send:notification", []byte(`{"user_id":42}`), opts)

	assert.Equal(t, "custom-task-001", task.ID)
	assert.Equal(t, "high-priority", task.Queue)
	assert.Equal(t, "task:send:notification", task.Name)
	assert.Equal(t, []byte(`{"user_id":42}`), task.Payload)
	assert.Equal(t, 0, task.Retry)
	assert.Equal(t, 5, task.MaxRetry)
	assert.Equal(t, 15000, task.TimeoutMs)
	assert.Equal(t, "uniq-user-42", task.UniqueKey)
	assert.Equal(t, 60000, task.UniqueTTLMs)
	assert.Equal(t, "group-email", task.GroupKey)
	assert.Equal(t, past.UnixMilli(), task.DeadlineMs)
}

func TestNewTask_Defaults(t *testing.T) {
	task := taskmodel.NewTask("default:task", nil)

	assert.Empty(t, task.ID, "ID is assigned by broker/client upon enqueue")
	assert.Equal(t, "default", task.Queue, "Queue should default to 'default'")
	assert.Equal(t, "default:task", task.Name)
	assert.Nil(t, task.Payload)
	assert.Equal(t, 0, task.Retry)
	assert.Equal(t, 3, task.MaxRetry, "MaxRetry default should be 3")
	assert.Equal(t, 30000, task.TimeoutMs, "TimeoutMs default should be 30000 (30s)")
}

func TestTask_EffectiveTimeout(t *testing.T) {
	now := time.Now()
	task := &taskmodel.Task{
		TimeoutMs: 30000,
	}
	assert.Equal(t, 30*time.Second, task.EffectiveTimeout(now))

	// Expired deadline
	pastDeadline := &taskmodel.Task{
		TimeoutMs:  30000,
		DeadlineMs: now.Add(-2 * time.Second).UnixMilli(),
	}
	assert.Equal(t, time.Nanosecond, pastDeadline.EffectiveTimeout(now))

	var nilTask *taskmodel.Task
	assert.Equal(t, time.Duration(0), nilTask.EffectiveTimeout(now))
}

func TestPtrHelpers(t *testing.T) {
	assert.Equal(t, 10, *taskmodel.Ptr(10))
	assert.Equal(t, "hello", *taskmodel.Ptr("hello"))

	dur := 5 * time.Minute
	assert.Equal(t, dur, *taskmodel.Ptr(dur))

	now := time.Now()
	assert.True(t, now.Equal(*taskmodel.Ptr(now)))
}

func TestTaskError_Sentinel(t *testing.T) {
	require.Error(t, taskmodel.ErrNoHandler)

	assert.True(t, taskmodel.IsSkipRetry(taskmodel.ErrSkipRetry))
	assert.False(t, taskmodel.IsSkipRetry(assert.AnError))
}
