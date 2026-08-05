package worker

import (
	"context"

	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// ProgressFunc updates task progress (meta + events). Optional on ConsumeContext.
type ProgressFunc func(ctx context.Context, percent int, data string) error

// ConsumeContext encapsulates all states during a single task consumption lifecycle.
type ConsumeContext struct {
	context.Context

	Task      *taskmodel.Task
	MessageID string
	Queue     string
	Group     string

	// Strongly-typed fields for common middleware data flow
	TraceID        string
	RateLimitGroup string
	// Result is optional handler output; retained when completed_retention > 0.
	Result []byte

	progress ProgressFunc

	index    int
	handlers []CoreHandlerFunc
	aborted  bool
}

// SetResult stores handler output for optional completed retention.
func (c *ConsumeContext) SetResult(b []byte) {
	c.Result = b
	if c.Task != nil {
		c.Task.Result = b
	}
}

// UpdateProgress reports 0-100 progress (and optional data) for long-running handlers.
// No-op if progress backend is not wired.
func (c *ConsumeContext) UpdateProgress(percent int, data string) error {
	if c.progress == nil {
		return nil
	}
	return c.progress(c.Context, percent, data)
}

// Next triggers the next handler or middleware in the execution chain.
func (c *ConsumeContext) Next() error {
	if c.aborted {
		return nil
	}
	c.index++
	if c.index < len(c.handlers) {
		return c.handlers[c.index](c)
	}
	return nil
}

// Abort halts the execution chain.
func (c *ConsumeContext) Abort() {
	c.aborted = true
	c.index = len(c.handlers)
}

// IsAborted returns true if the execution chain was aborted.
func (c *ConsumeContext) IsAborted() bool {
	return c.aborted
}

// Reset clears all references and states to prevent memory leaks and dirty reuses in sync.Pool.
func (c *ConsumeContext) Reset() {
	c.Context = nil
	c.Task = nil
	c.MessageID = ""
	c.Queue = ""
	c.Group = ""
	c.TraceID = ""
	c.RateLimitGroup = ""
	c.Result = nil
	c.progress = nil
	c.handlers = nil
	c.aborted = false
}
