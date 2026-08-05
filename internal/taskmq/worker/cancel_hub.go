package worker

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"go.uber.org/zap"
)

// Cancelations holds cancel functions for all active tasks.
type Cancelations struct {
	mu          sync.Mutex
	cancelFuncs map[string]context.CancelFunc
}

// NewCancelations creates an empty cancel registry.
func NewCancelations() *Cancelations {
	return &Cancelations{
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

// Add registers a cancel function for a running task.
func (c *Cancelations) Add(id string, fn context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFuncs[id] = fn
}

// Delete removes a cancel function after task completion.
func (c *Cancelations) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cancelFuncs, id)
}

// Cancel invokes the cancel function for a task if it is still running.
func (c *Cancelations) Cancel(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := c.cancelFuncs[id]; ok {
		fn()
	}
}

// CancelHub owns in-memory cancel registry and Redis cancel pub/sub.
type CancelHub struct {
	*Cancelations
	rdb    redis.UniversalClient
	logger *zap.Logger
}

// NewCancelHub creates a cancel hub with an empty registry.
func NewCancelHub(rdb redis.UniversalClient, logger *zap.Logger) *CancelHub {
	return &CancelHub{
		Cancelations: NewCancelations(),
		rdb:          rdb,
		logger:       logger,
	}
}

// StartSubscriber listens for cancel pub/sub messages and cancels matching tasks.
// It blocks until the Redis subscription is acknowledged (or ctx is done) so a
// subsequent Publish is not lost to a not-yet-subscribed race.
func (h *CancelHub) StartSubscriber(ctx context.Context, wg *sync.WaitGroup, queues []string) {
	if len(queues) == 0 {
		return
	}
	channels := make([]string, len(queues))
	for i, q := range queues {
		channels[i] = keys.KeysFor(q).CancelChannel()
	}
	pubsub := h.rdb.Subscribe(ctx, channels...)
	// Wait for subscribe confirmation before returning so Start can proceed safely.
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		if h.logger != nil && ctx.Err() == nil {
			h.logger.Error("Cancel subscriber: failed to subscribe", zap.Error(err))
		}
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer pubsub.Close()

		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					h.logger.Warn("Cancel subscriber: PubSub channel closed, refreshing subscription")
					_ = pubsub.Close()
					if ctx.Err() != nil {
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(1 * time.Second): // Backoff to prevent tight spin
					}
					pubsub = h.rdb.Subscribe(ctx, channels...)
					if _, err := pubsub.Receive(ctx); err != nil {
						if ctx.Err() != nil {
							return
						}
						h.logger.Error("Cancel subscriber: resubscribe failed", zap.Error(err))
						continue
					}
					ch = pubsub.Channel()
					continue
				}
				h.Cancel(msg.Payload)
			}
		}
	}()
}
