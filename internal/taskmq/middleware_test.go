package taskmq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestMiddleware_Recovery(t *testing.T) {
	logger := zap.NewNop()
	
	t.Run("Recovery catches panic", func(t *testing.T) {
		handlers := []CoreHandlerFunc{
			RecoveryMiddleware(logger),
			func(c *ConsumeContext) error {
				panic("something went completely wrong")
			},
		}

		c := &ConsumeContext{
			Context:  context.Background(),
			Task:     &Task{ID: "t-1", Name: "panic-task"},
			handlers: handlers,
			index:    -1,
		}

		err := c.Next()
		if err == nil {
			t.Fatal("expected error to be caught and returned, got nil")
		}
		if err.Error() != "task panicked: something went completely wrong" {
			t.Errorf("unexpected error payload: %v", err)
		}
	})
}

func TestMiddleware_RateLimiter(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	limiter := NewGCRALimiter(rdb)
	logger := zap.NewNop()
	mb := &mockBroker{}

	t.Run("Rate limit not exceeded runs next", func(t *testing.T) {
		mb.deferCnt = 0
		handlers := []CoreHandlerFunc{
			RateLimitMiddleware(limiter, mb, func(queue string) (int64, time.Duration, string) {
				return 5, time.Minute, ""
			}, nil, JSONCodec{}, logger),
			func(c *ConsumeContext) error {
				return nil
			},
		}

		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &Task{ID: "t-1", Queue: "q-1"},
			MessageID: "1-0",
			Queue:     "q-1",
			handlers:  handlers,
			index:     -1,
		}

		err := c.Next()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mb.deferCnt != 0 {
			t.Errorf("expected no task defers, got %d", mb.deferCnt)
		}
	})

	t.Run("Rate limit exceeded defers task and aborts chain", func(t *testing.T) {
		mb.deferCnt = 0
		// Max limit of 1 per minute, trigger twice to force limit exceedance
		handlers := []CoreHandlerFunc{
			RateLimitMiddleware(limiter, mb, func(queue string) (int64, time.Duration, string) {
				return 1, time.Minute, ""
			}, nil, JSONCodec{}, logger),
			func(c *ConsumeContext) error {
				return nil
			},
		}

		runChain := func(taskID string, queue string) error {
			c := &ConsumeContext{
				Context:   context.Background(),
				Task:      &Task{ID: taskID, Queue: queue},
				MessageID: "2-0",
				Queue:     queue,
				handlers:  handlers,
				index:     -1,
			}
			return c.Next()
		}

		// First run: allowed on "q-2"
		if err := runChain("t-2", "q-2"); err != nil {
			t.Fatalf("unexpected first run failure: %v", err)
		}

		// Second run: should be limited on "q-2", deferred, and abort chain
		if err := runChain("t-3", "q-2"); err != nil {
			t.Fatalf("unexpected second run failure: %v", err)
		}

		mb.mu.Lock()
		defer mb.mu.Unlock()
		if mb.deferCnt != 1 {
			t.Errorf("expected task to be deferred exactly 1 time, got %d", mb.deferCnt)
		}
	})
}

func TestConsumeContext_Keys(t *testing.T) {
	// Test standard context propagation
	c := &ConsumeContext{
		Context: context.Background(),
	}
	type ctxKey string
	const key ctxKey = "custom-key"
	c.Context = context.WithValue(c.Context, key, "custom-value")

	val := c.Value(key)
	if val != "custom-value" {
		t.Errorf("expected context value to be retrieved, got %v", val)
	}

	// Test strongly-typed fields
	c.TraceID = "abcdef123456"
	c.RateLimitGroup = "group-1"

	if c.TraceID != "abcdef123456" {
		t.Errorf("expected TraceID to be 'abcdef123456', got %s", c.TraceID)
	}
	if c.RateLimitGroup != "group-1" {
		t.Errorf("expected RateLimitGroup to be 'group-1', got %s", c.RateLimitGroup)
	}
}

func TestMiddleware_RateLimiter_GroupKeyLayers(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	limiter := NewGCRALimiter(rdb)
	logger := zap.NewNop()
	mb := &mockBroker{}

	t.Run("Layer 1 - Task.GroupKey is used", func(t *testing.T) {
		var extractedGroup string
		handlers := []CoreHandlerFunc{
			RateLimitMiddleware(limiter, mb, func(queue string) (int64, time.Duration, string) {
				return 10, time.Minute, "field-dynamic"
			}, func(p []byte) string {
				return "layer-2"
			}, JSONCodec{}, logger),
			func(c *ConsumeContext) error {
				extractedGroup = c.RateLimitGroup
				return nil
			},
		}

		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &Task{ID: "t-1", Queue: "q-1", GroupKey: "layer-1", Payload: []byte(`{"field-dynamic":"layer-3"}`)},
			MessageID: "1-0",
			Queue:     "q-1",
			handlers:  handlers,
			index:     -1,
		}

		if err := c.Next(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if extractedGroup != "layer-1" {
			t.Errorf("expected extracted group to be 'layer-1', got %s", extractedGroup)
		}
	})

	t.Run("Layer 2 - extractor function is used when GroupKey is empty", func(t *testing.T) {
		var extractedGroup string
		handlers := []CoreHandlerFunc{
			RateLimitMiddleware(limiter, mb, func(queue string) (int64, time.Duration, string) {
				return 10, time.Minute, "field-dynamic"
			}, func(p []byte) string {
				return "layer-2"
			}, JSONCodec{}, logger),
			func(c *ConsumeContext) error {
				extractedGroup = c.RateLimitGroup
				return nil
			},
		}

		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &Task{ID: "t-1", Queue: "q-1", GroupKey: "", Payload: []byte(`{"field-dynamic":"layer-3"}`)},
			MessageID: "1-0",
			Queue:     "q-1",
			handlers:  handlers,
			index:     -1,
		}

		if err := c.Next(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if extractedGroup != "layer-2" {
			t.Errorf("expected extracted group to be 'layer-2', got %s", extractedGroup)
		}
	})

	t.Run("Layer 3 - JSON field extraction fallback", func(t *testing.T) {
		var extractedGroup string
		handlers := []CoreHandlerFunc{
			RateLimitMiddleware(limiter, mb, func(queue string) (int64, time.Duration, string) {
				return 10, time.Minute, "field-dynamic"
			}, nil, JSONCodec{}, logger),
			func(c *ConsumeContext) error {
				extractedGroup = c.RateLimitGroup
				return nil
			},
		}

		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &Task{ID: "t-1", Queue: "q-1", GroupKey: "", Payload: []byte(`{"field-dynamic":"layer-3"}`)},
			MessageID: "1-0",
			Queue:     "q-1",
			handlers:  handlers,
			index:     -1,
		}

		if err := c.Next(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if extractedGroup != "layer-3" {
			t.Errorf("expected extracted group to be 'layer-3', got %s", extractedGroup)
		}
	})
}
