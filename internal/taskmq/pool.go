package taskmq

import (
	"context"
	"sync"
)

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
	c.TraceID = ""
	c.RateLimitGroup = ""
	c.handlers = handlers
	c.index = -1
	c.aborted = false
	return c
}

// ReleaseConsumeContext returns a ConsumeContext back to the pool after clearing its state.
func ReleaseConsumeContext(c *ConsumeContext) {
	c.Reset()
	consumeContextPool.Put(c)
}
