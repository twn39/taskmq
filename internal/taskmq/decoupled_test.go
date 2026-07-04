package taskmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type mockBroker struct {
	mu           sync.Mutex
	moveToDLQCnt int
	schedRetry   int
	deferCnt     int
	releasedLock int
	lastDLQName  string
}

func (m *mockBroker) MoveToDLQ(ctx context.Context, task *Task, streamKey, msgID, group string, dlqQueueName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.moveToDLQCnt++
	m.lastDLQName = dlqQueueName
	return nil
}

func (m *mockBroker) ScheduleRetry(ctx context.Context, task *Task, streamKey, msgID, group string, runAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schedRetry++
	return nil
}

func (m *mockBroker) DeferRateLimitedTask(ctx context.Context, msgID string, task *Task, group string, runAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deferCnt++
	return nil
}

func (m *mockBroker) ReleaseUniqueLock(ctx context.Context, task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releasedLock++
	return nil
}

type mockRetryPolicy struct {
	should bool
}

func (m *mockRetryPolicy) ShouldRetry(task *Task, err error) bool {
	return m.should
}

func (m *mockRetryPolicy) NextBackoff(task *Task) time.Duration {
	return 1 * time.Millisecond
}

func TestWorkerPool_DecoupledAbtractionAndFailureHandling(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	logger := zap.NewNop()

	t.Run("Task exhausts retries - Should move to custom DLQ via DeadLetterPolicy", func(t *testing.T) {
		mb := &mockBroker{}
		mp := &mockRetryPolicy{should: false} // do not retry

		var hookTriggered bool
		dlHook := func(ctx context.Context, task *Task, err error) {
			hookTriggered = true
		}
		dlPolicy := NewStandardDeadLetterPolicy("custom-dead-letters", dlHook)

		opts := WorkerOptions{
			Broker:           mb,
			RetryPolicy:      mp,
			DeadLetterPolicy: dlPolicy,
		}
		opts.ApplyDefaults(rdb, logger, "test-q", JSONCodec{})

		pool := NewWorkerPool(rdb, logger, "test-q", opts).(*workerPool)

		msg := redis.XMessage{
			ID:     "1-0",
			Values: map[string]interface{}{},
		}

		// Use the new ProcessMessage flow to invoke middleware chain & failure handling
		pool.Register("test-task", func(ctx context.Context, task *Task) error {
			return errors.New("fatal billing error")
		})
		
		taskBytesWithWrongName := []byte(`{"id":"t-1","queue":"test-q","name":"test-task","retry":2,"max_retry":3}`)
		msg.Values["payload"] = taskBytesWithWrongName

		pool.ProcessMessage(context.Background(), msg)

		mb.mu.Lock()
		if mb.moveToDLQCnt != 1 {
			t.Errorf("expected MoveToDLQ to be called 1 time, got %d", mb.moveToDLQCnt)
		}
		if mb.lastDLQName != "custom-dead-letters" {
			t.Errorf("expected DLQ target to be 'custom-dead-letters', got '%s'", mb.lastDLQName)
		}
		mb.mu.Unlock()

		if !hookTriggered {
			t.Error("expected Dead-Letter hook to be triggered, but was not")
		}
	})

	t.Run("Task filtered out by ErrorFilterRetryPolicy", func(t *testing.T) {
		mb := &mockBroker{}
		
		// Base says yes, but filter says no because error matches non-retryable list
		nonRetryableErr := errors.New("invalid signature")
		basePolicy := &mockRetryPolicy{should: true}
		filterPolicy := NewErrorFilterRetryPolicy(basePolicy, []error{nonRetryableErr})

		opts := WorkerOptions{
			Broker:      mb,
			RetryPolicy: filterPolicy,
		}
		opts.ApplyDefaults(rdb, logger, "test-q", JSONCodec{})

		pool := NewWorkerPool(rdb, logger, "test-q", opts).(*workerPool)

		msg := redis.XMessage{
			ID:     "3-0",
			Values: map[string]interface{}{},
		}
		taskBytes := []byte(`{"id":"t-3","queue":"test-q","name":"test-task","retry":1,"max_retry":3}`)
		msg.Values["payload"] = taskBytes

		pool.Register("test-task", func(ctx context.Context, task *Task) error {
			return nonRetryableErr
		})

		pool.ProcessMessage(context.Background(), msg)

		mb.mu.Lock()
		defer mb.mu.Unlock()
		if mb.moveToDLQCnt != 1 {
			t.Errorf("expected task matching non-retryable error to go straight to DLQ, but did not")
		}
	})
}

func TestPriorityWorker_DecoupledAbstractionAndFailureHandling(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	logger := zap.NewNop()

	t.Run("Task fails on priority worker - Should move to custom DLQ via injected Broker & Policies", func(t *testing.T) {
		mb := &mockBroker{}
		mp := &mockRetryPolicy{should: false} // do not retry

		var hookTriggered bool
		dlHook := func(ctx context.Context, task *Task, err error) {
			hookTriggered = true
		}
		dlPolicy := NewStandardDeadLetterPolicy("priority-dead-letters", dlHook)

		opts := WorkerOptions{
			Broker:           mb,
			RetryPolicy:      mp,
			DeadLetterPolicy: dlPolicy,
			PriorityQueues: []QueuePriority{
				{Name: "high", Weight: 10},
			},
		}
		opts.ApplyDefaults(rdb, logger, "", JSONCodec{})

		pw := NewPriorityWorker(rdb, logger, opts).(*priorityWorker)

		msg := redis.XMessage{
			ID:     "4-0",
			Values: map[string]interface{}{},
		}
		pw.Register("test-task", func(ctx context.Context, task *Task) error {
			return errors.New("priority worker fatal error")
		})

		taskBytes := []byte(`{"id":"t-4","queue":"high","name":"test-task","retry":2,"max_retry":3}`)
		msg.Values["payload"] = taskBytes

		pw.processMessage(context.Background(), "taskmq:{high}:queue", msg)

		mb.mu.Lock()
		if mb.moveToDLQCnt != 1 {
			t.Errorf("expected MoveToDLQ to be called 1 time, got %d", mb.moveToDLQCnt)
		}
		if mb.lastDLQName != "priority-dead-letters" {
			t.Errorf("expected DLQ target to be 'priority-dead-letters', got '%s'", mb.lastDLQName)
		}
		mb.mu.Unlock()

		if !hookTriggered {
			t.Error("expected Dead-Letter hook to be triggered, but was not")
		}
	})
}
