package taskmq

import (
	"context"
	"sync"
)

// ConsumeContext encapsulates all states during a single task consumption lifecycle.
type ConsumeContext struct {
	context.Context

	Task      *Task
	MessageID string
	Queue     string
	Group     string

	keysMu sync.RWMutex
	Keys   map[string]any

	index    int
	handlers []CoreHandlerFunc
}

// Set stores metadata within the context.
func (c *ConsumeContext) Set(key string, value any) {
	c.keysMu.Lock()
	defer c.keysMu.Unlock()
	if c.Keys == nil {
		c.Keys = make(map[string]any)
	}
	c.Keys[key] = value
}

// Get retrieves metadata from the context.
func (c *ConsumeContext) Get(key string) (any, bool) {
	c.keysMu.RLock()
	defer c.keysMu.RUnlock()
	if c.Keys == nil {
		return nil, false
	}
	val, ok := c.Keys[key]
	return val, ok
}

// Next triggers the next handler or middleware in the execution chain.
func (c *ConsumeContext) Next() error {
	c.index++
	if c.index < len(c.handlers) {
		return c.handlers[c.index](c)
	}
	return nil
}

// Abort halts the execution chain.
func (c *ConsumeContext) Abort() {
	c.index = len(c.handlers)
}
