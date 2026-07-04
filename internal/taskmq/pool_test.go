package taskmq

import (
	"context"
	"testing"
)

func TestAcquireReleaseConsumeContext(t *testing.T) {
	ctx := context.Background()
	task := &Task{
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
		t.Errorf("expected Task to be %v, got %v", task, c.Task)
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
		Task:           &Task{ID: "task-1"},
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
		t.Errorf("expected Task to be nil, got %v", c.Task)
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
