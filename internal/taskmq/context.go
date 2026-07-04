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
	aborted  bool
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

var consumeContextPool = sync.Pool{
	New: func() any {
		return &ConsumeContext{
			index: -1,
		}
	},
}

// AcquireConsumeContext retrieves a clean ConsumeContext from the pool.
func AcquireConsumeContext(ctx context.Context, task *Task, msgID, queue, group string, handlers []CoreHandlerFunc) *ConsumeContext {
	c := consumeContextPool.Get().(*ConsumeContext)
	c.Context = ctx
	c.Task = task
	c.MessageID = msgID
	c.Queue = queue
	c.Group = group
	c.handlers = handlers
	c.index = -1
	c.aborted = false
	return c
}

// ReleaseConsumeContext returns a ConsumeContext back to the pool after clearing its state.
func ReleaseConsumeContext(c *ConsumeContext) {
	c.Context = nil
	c.Task = nil
	c.MessageID = ""
	c.Queue = ""
	c.Group = ""
	c.handlers = nil
	c.aborted = false
	c.keysMu.Lock()
	for k := range c.Keys {
		delete(c.Keys, k)
	}
	c.keysMu.Unlock()
	consumeContextPool.Put(c)
}
