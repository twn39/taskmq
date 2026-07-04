package taskmq

import (
	"context"
)

// ConsumeContext encapsulates all states during a single task consumption lifecycle.
type ConsumeContext struct {
	context.Context

	Task      *Task
	MessageID string
	Queue     string
	Group     string

	// Strongly-typed fields for common middleware data flow
	TraceID        string
	RateLimitGroup string

	index    int
	handlers []CoreHandlerFunc
	aborted  bool
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
	c.handlers = nil
	c.aborted = false
}
