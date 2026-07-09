package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestTaskMQ_ProcessMessage_TableDriven(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	logger := zap.NewNop()

	tests := []struct {
		name                string
		task                *taskmodel.Task
		handler             func(ctx context.Context, task *taskmodel.Task) error
		retryShould         bool
		cancelledPreExec    bool
		expectedComplete    int
		expectedRetry       int
		expectedDLQ         int
		expectedLockRelease int
		expectedHookTrigger bool
	}{
		{
			name: "Success path",
			task: &taskmodel.Task{
				ID:        "t-success",
				Queue:     "test-q",
				Name:      "task:success",
				TimeoutMs: 1000,
			},
			handler: func(ctx context.Context, task *taskmodel.Task) error {
				return nil
			},
			expectedComplete: 1,
		},
		{
			name: "Success path with unique key",
			task: &taskmodel.Task{
				ID:        "t-success-lock",
				Queue:     "test-q",
				Name:      "task:success-lock",
				TimeoutMs: 1000,
				UniqueKey: "lock-1",
			},
			handler: func(ctx context.Context, task *taskmodel.Task) error {
				return nil
			},
			expectedComplete:    1,
			expectedLockRelease: 1,
		},
		{
			name: "Retry path on error",
			task: &taskmodel.Task{
				ID:        "t-retry",
				Queue:     "test-q",
				Name:      "task:retry",
				TimeoutMs: 1000,
			},
			handler: func(ctx context.Context, task *taskmodel.Task) error {
				return errors.New("temporary error")
			},
			retryShould:   true,
			expectedRetry: 1,
		},
		{
			name: "DLQ path when retry exhausted",
			task: &taskmodel.Task{
				ID:        "t-dlq",
				Queue:     "test-q",
				Name:      "task:dlq",
				TimeoutMs: 1000,
			},
			handler: func(ctx context.Context, task *taskmodel.Task) error {
				return errors.New("fatal error")
			},
			retryShould:         false,
			expectedDLQ:         1,
			expectedHookTrigger: true,
		},
		{
			name: "Panic path in handler",
			task: &taskmodel.Task{
				ID:        "t-panic",
				Queue:     "test-q",
				Name:      "task:panic",
				TimeoutMs: 1000,
			},
			handler: func(ctx context.Context, task *taskmodel.Task) error {
				panic("something went wrong")
			},
			retryShould:         false,
			expectedDLQ:         1,
			expectedHookTrigger: true,
		},
		{
			name: "Timeout path",
			task: &taskmodel.Task{
				ID:        "t-timeout",
				Queue:     "test-q",
				Name:      "task:timeout",
				TimeoutMs: 5, // 5 milliseconds timeout
			},
			handler: func(ctx context.Context, task *taskmodel.Task) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(50 * time.Millisecond):
					return nil
				}
			},
			retryShould:         false,
			expectedDLQ:         1,
			expectedHookTrigger: true,
		},
		{
			name: "Pre-execution cancellation path",
			task: &taskmodel.Task{
				ID:        "t-cancel",
				Queue:     "test-q",
				Name:      "task:cancel",
				TimeoutMs: 1000,
			},
			handler: func(ctx context.Context, task *taskmodel.Task) error {
				return nil
			},
			cancelledPreExec: true,
			expectedComplete: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mb := &mockBroker{}
			mp := &mockRetryPolicy{should: tt.retryShould}
			hookTriggered := false
			dlHook := func(ctx context.Context, task *taskmodel.Task, err error) {
				hookTriggered = true
			}
			dlPolicy := policy.NewStandardDeadLetterPolicy("dlq-q", dlHook)

			// Clean/reset miniredis keys
			mr.FlushAll()

			// Create base worker with mocks
			pool := NewWorkerPool(rdb, logger, "test-q",
				WithBroker(mb),
				WithRetryPolicy(mp),
				WithDeadLetterPolicy(dlPolicy),
			).(*workerPool)

			// Wrap handler to track invocations
			handlerCalled := false
			handler := func(ctx context.Context, task *taskmodel.Task) error {
				handlerCalled = true
				return tt.handler(ctx, task)
			}

			// Register the handler
			pool.Register(tt.task.Name, handler)

			// Set pre-execution cancellation key in Redis if requested
			if tt.cancelledPreExec {
				cancelledKey := keys.KeysFor(tt.task.Queue).Cancelled(tt.task.ID)
				err := rdb.Set(context.Background(), cancelledKey, "1", 0).Err()
				if err != nil {
					t.Fatalf("failed to set pre-execution cancellation key: %v", err)
				}
			}

			// Prepare msg XMessage
			taskBytes, err := tt.task.Serialize()
			if err != nil {
				t.Fatalf("failed to serialize task: %v", err)
			}
			msg := redis.XMessage{
				ID: "123-0",
				Values: map[string]interface{}{
					"task": []byte(taskBytes),
				},
			}

			// Call processMessage
			pool.ProcessMessage(context.Background(), msg)

			// Assert results
			mb.mu.Lock()
			defer mb.mu.Unlock()

			if mb.completedCnt != tt.expectedComplete {
				t.Errorf("expected CompleteTask call count: %d, got: %d", tt.expectedComplete, mb.completedCnt)
			}
			if mb.schedRetry != tt.expectedRetry {
				t.Errorf("expected ScheduleRetry call count: %d, got: %d", tt.expectedRetry, mb.schedRetry)
			}
			if mb.moveToDLQCnt != tt.expectedDLQ {
				t.Errorf("expected MoveToDLQ call count: %d, got: %d", tt.expectedDLQ, mb.moveToDLQCnt)
			}
			if mb.releasedLock != tt.expectedLockRelease {
				t.Errorf("expected ReleaseUniqueLock call count: %d, got: %d", tt.expectedLockRelease, mb.releasedLock)
			}
			if hookTriggered != tt.expectedHookTrigger {
				t.Errorf("expected DLQ hook triggered: %t, got: %t", tt.expectedHookTrigger, hookTriggered)
			}
			if tt.cancelledPreExec && handlerCalled {
				t.Error("expected handler to not be called for pre-execution cancelled task")
			}
			if !tt.cancelledPreExec && !handlerCalled {
				t.Error("expected handler to be called")
			}
		})
	}
}
