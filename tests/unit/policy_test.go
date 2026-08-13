package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestExponentialBackoffPolicy(t *testing.T) {
	p := policy.NewExponentialBackoff(1*time.Second, 30*time.Second, false) // No jitter for deterministic test

	taskUnder := &taskmodel.Task{Retry: 1, MaxRetry: 3}
	taskAtMax := &taskmodel.Task{Retry: 3, MaxRetry: 3}

	assert.True(t, p.ShouldRetry(taskUnder, assert.AnError))
	assert.False(t, p.ShouldRetry(taskAtMax, assert.AnError))
	assert.False(t, p.ShouldRetry(taskUnder, taskmodel.ErrSkipRetry), "ErrSkipRetry must not retry")

	// Backoff durations
	t1 := &taskmodel.Task{Retry: 0}
	t2 := &taskmodel.Task{Retry: 1}
	t3 := &taskmodel.Task{Retry: 2}
	t10 := &taskmodel.Task{Retry: 10}

	assert.Equal(t, 1*time.Second, p.NextBackoff(t1))
	assert.Equal(t, 2*time.Second, p.NextBackoff(t2))
	assert.Equal(t, 4*time.Second, p.NextBackoff(t3))
	assert.Equal(t, 30*time.Second, p.NextBackoff(t10), "Max delay cap")
}

func TestErrorFilterRetryPolicy(t *testing.T) {
	base := policy.NewExponentialBackoff(1*time.Second, 10*time.Second, false)
	customNonRetry := errors.New("custom non-retryable error")

	filter := policy.NewErrorFilterRetryPolicy(base, []error{customNonRetry})
	task := &taskmodel.Task{Retry: 0, MaxRetry: 3}

	assert.True(t, filter.ShouldRetry(task, assert.AnError))
	assert.False(t, filter.ShouldRetry(task, customNonRetry))
	assert.False(t, filter.ShouldRetry(task, taskmodel.ErrSkipRetry))
}

func TestStandardDeadLetterPolicy(t *testing.T) {
	var hookInvoked bool
	hook := func(ctx context.Context, t *taskmodel.Task, err error) {
		hookInvoked = true
	}

	dlp := policy.NewStandardDeadLetterPolicy("custom-dlq", hook)
	task := &taskmodel.Task{ID: "task-100", Queue: "origin-queue"}

	assert.Equal(t, "custom-dlq", dlp.DLQQueueName(task))

	dlp.BeforeDeadLetter(context.Background(), task, errors.New("fatal exception"))
	assert.Equal(t, "fatal exception", task.LastError)
	assert.True(t, hookInvoked)

	// Default queue name when customDLQ is empty
	dlpDefault := policy.NewStandardDeadLetterPolicy("", nil)
	assert.Equal(t, "origin-queue", dlpDefault.DLQQueueName(task))
}
