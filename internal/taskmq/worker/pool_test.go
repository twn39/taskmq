package worker

import (
	"context"
	"testing"
	"time"

	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"

	"github.com/twn39/taskmq/internal/taskmq/codec"
)

func TestAcquireReleaseConsumeContext(t *testing.T) {
	ctx := context.Background()
	task := &taskmodel.Task{
		ID:   "task-123",
		Name: "test-task",
	}
	msgID := "msg-123"
	queue := "test-queue"
	group := "test-group"
	handlers := []CoreHandlerFunc{
		func(c *ConsumeContext) error {
			return nil
		},
	}

	// 1. Acquire context
	c := AcquireConsumeContext(ctx, task, msgID, queue, group, handlers)
	if c.Context != ctx {
		t.Errorf("expected Context to be %v, got %v", ctx, c.Context)
	}
	if c.Task != task {
		t.Errorf("expected taskmodel.Task to be %v, got %v", task, c.Task)
	}
	if c.MessageID != msgID {
		t.Errorf("expected MessageID to be %s, got %s", msgID, c.MessageID)
	}
	if c.Queue != queue {
		t.Errorf("expected Queue to be %s, got %s", queue, c.Queue)
	}
	if c.Group != group {
		t.Errorf("expected Group to be %s, got %s", group, c.Group)
	}
	if len(c.handlers) != 1 {
		t.Errorf("expected 1 handler, got %d", len(c.handlers))
	}
	if c.index != -1 {
		t.Errorf("expected index to be -1, got %d", c.index)
	}
	if c.aborted {
		t.Error("expected aborted to be false")
	}

	// Set dynamic properties
	c.TraceID = "trace-456"
	c.RateLimitGroup = "rate-789"

	// 2. Release context
	ReleaseConsumeContext(c)

	// 3. Acquire another context and check if it has been reset
	// Note: sync.Pool might return a new instance or the recycled one,
	// but either way, the acquired instance must be perfectly clean.
	c2 := AcquireConsumeContext(ctx, task, msgID, queue, group, handlers)
	if c2.TraceID != "" {
		t.Errorf("expected TraceID to be empty on newly acquired context, got %s", c2.TraceID)
	}
	if c2.RateLimitGroup != "" {
		t.Errorf("expected RateLimitGroup to be empty on newly acquired context, got %s", c2.RateLimitGroup)
	}
	if c2.index != -1 {
		t.Errorf("expected index to be -1, got %d", c2.index)
	}
	if c2.aborted {
		t.Error("expected aborted to be false")
	}

	ReleaseConsumeContext(c2)
}

func TestConsumeContext_Reset(t *testing.T) {
	c := &ConsumeContext{
		Context:        context.Background(),
		Task: &taskmodel.Task{ID: "task-1"},
		MessageID:      "msg-1",
		Queue:          "queue-1",
		Group:          "group-1",
		TraceID:        "trace-1",
		RateLimitGroup: "rl-1",
		handlers: []CoreHandlerFunc{
			func(c *ConsumeContext) error { return nil },
		},
		aborted: true,
	}

	c.Reset()

	if c.Context != nil {
		t.Errorf("expected Context to be nil, got %v", c.Context)
	}
	if c.Task != nil {
		t.Errorf("expected taskmodel.Task to be nil, got %v", c.Task)
	}
	if c.MessageID != "" {
		t.Errorf("expected MessageID to be empty, got %s", c.MessageID)
	}
	if c.Queue != "" {
		t.Errorf("expected Queue to be empty, got %s", c.Queue)
	}
	if c.Group != "" {
		t.Errorf("expected Group to be empty, got %s", c.Group)
	}
	if c.TraceID != "" {
		t.Errorf("expected TraceID to be empty, got %s", c.TraceID)
	}
	if c.RateLimitGroup != "" {
		t.Errorf("expected RateLimitGroup to be empty, got %s", c.RateLimitGroup)
	}
	if c.handlers != nil {
		t.Errorf("expected handlers to be nil, got %v", c.handlers)
	}
	if c.aborted {
		t.Error("expected aborted to be false")
	}
}

func TestSemaphoreExecutionPool(t *testing.T) {
	ctx := context.Background()

	t.Run("basic acquire and release", func(t *testing.T) {
		pool := NewSemaphoreExecutionPool(3)
		if pool.Size() != 3 {
			t.Errorf("expected size 3, got %d", pool.Size())
		}
		if pool.InUse() != 0 {
			t.Errorf("expected in use 0, got %d", pool.InUse())
		}

		err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pool.InUse() != 1 {
			t.Errorf("expected in use 1, got %d", pool.InUse())
		}

		pool.Release()
		if pool.InUse() != 0 {
			t.Errorf("expected in use 0, got %d", pool.InUse())
		}
	})

	t.Run("concurrency limit and context cancellation", func(t *testing.T) {
		pool := NewSemaphoreExecutionPool(2)

		err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		err = pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if pool.InUse() != 2 {
			t.Errorf("expected in use 2, got %d", pool.InUse())
		}

		// Try acquiring when full, should timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()

		err = pool.Acquire(timeoutCtx)
		if err == nil {
			t.Error("expected timeout error, got nil")
		}

		// Release one, try acquiring again
		pool.Release()
		if pool.InUse() != 1 {
			t.Errorf("expected in use 1, got %d", pool.InUse())
		}

		err = pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

type mockExecutionPool struct {
	acquireCount int
	releaseCount int
}

func (m *mockExecutionPool) Acquire(ctx context.Context) error {
	m.acquireCount++
	return nil
}

func (m *mockExecutionPool) Release() {
	m.releaseCount++
}

func (m *mockExecutionPool) Size() int {
	return 10
}

func (m *mockExecutionPool) InUse() int {
	return 0
}

func TestBaseWorker_CustomExecutionPool(t *testing.T) {
	mockPool := &mockExecutionPool{}
	opts := defaultWorkerConfig(codec.JSONCodec{})

	err := WithExecutionPool(mockPool)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	worker := &baseWorker{}
	worker.initBase(nil, nil, &opts)

	if worker.execPool != mockPool {
		t.Error("expected custom execution pool to be injected")
	}

	_ = worker.execPool.Acquire(context.Background())
	worker.execPool.Release()

	if mockPool.acquireCount != 1 {
		t.Errorf("expected acquire count 1, got %d", mockPool.acquireCount)
	}
	if mockPool.releaseCount != 1 {
		t.Errorf("expected release count 1, got %d", mockPool.releaseCount)
	}
}
