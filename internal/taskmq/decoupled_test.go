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
}

func (m *mockBroker) MoveToDLQ(ctx context.Context, task *Task, streamKey, msgID, group string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.moveToDLQCnt++
	return nil
}

func (m *mockBroker) ScheduleRetry(ctx context.Context, task *Task, streamKey, msgID, group string, runAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schedRetry++
	return nil
}

func (m *mockBroker) DeferRateLimitedTask(ctx context.Context, msgID string, task *Task, runAt time.Time) error {
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

func (m *mockRetryPolicy) ShouldRetry(task *Task) bool {
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

	t.Run("Task exhausts retries - Should move to DLQ via injected Broker", func(t *testing.T) {
		mb := &mockBroker{}
		mp := &mockRetryPolicy{should: false} // do not retry

		opts := WorkerOptions{
			Broker:      mb,
			RetryPolicy: mp,
		}
		opts.ApplyDefaults(rdb, logger, "test-q", JSONCodec{})

		pool := NewWorkerPool(rdb, logger, "test-q", opts).(*workerPool)
		task := &Task{ID: "t-1", Queue: "test-q", MaxRetry: 3, Retry: 2}

		msg := redis.XMessage{
			ID:     "1-0",
			Values: map[string]interface{}{"task": `{"id":"t-1","queue":"test-q"}`},
		}

		pool.handleFailure(context.Background(), msg, task, errors.New("fatal err"))

		mb.mu.Lock()
		defer mb.mu.Unlock()
		if mb.moveToDLQCnt != 1 {
			t.Errorf("expected MoveToDLQ to be called 1 time, got %d", mb.moveToDLQCnt)
		}
		if mb.schedRetry != 0 {
			t.Errorf("expected ScheduleRetry to be called 0 times, got %d", mb.schedRetry)
		}
	})

	t.Run("Task does not exhaust retries - Should reschedule via injected Broker", func(t *testing.T) {
		mb := &mockBroker{}
		mp := &mockRetryPolicy{should: true} // trigger retry

		opts := WorkerOptions{
			Broker:      mb,
			RetryPolicy: mp,
		}
		opts.ApplyDefaults(rdb, logger, "test-q", JSONCodec{})

		pool := NewWorkerPool(rdb, logger, "test-q", opts).(*workerPool)
		task := &Task{ID: "t-2", Queue: "test-q", MaxRetry: 3, Retry: 1}

		msg := redis.XMessage{
			ID:     "2-0",
			Values: map[string]interface{}{"task": `{"id":"t-2","queue":"test-q"}`},
		}

		pool.handleFailure(context.Background(), msg, task, errors.New("transient err"))

		mb.mu.Lock()
		defer mb.mu.Unlock()
		if mb.schedRetry != 1 {
			t.Errorf("expected ScheduleRetry to be called 1 time, got %d", mb.schedRetry)
		}
		if mb.moveToDLQCnt != 0 {
			t.Errorf("expected MoveToDLQ to be called 0 times, got %d", mb.moveToDLQCnt)
		}
	})
}
