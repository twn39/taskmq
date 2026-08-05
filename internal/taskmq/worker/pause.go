package worker

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"go.uber.org/zap"
)

// PauseController tracks per-queue pause state and wakes waiters on resume.
type PauseController struct {
	rdb    redis.UniversalClient
	logger *zap.Logger

	mu           sync.RWMutex
	pausedQueues map[string]bool
	pauseChans   map[string]chan struct{}
}

// NewPauseController creates an empty pause controller.
func NewPauseController(rdb redis.UniversalClient, logger *zap.Logger) *PauseController {
	return &PauseController{
		rdb:          rdb,
		logger:       logger,
		pausedQueues: make(map[string]bool),
		pauseChans:   make(map[string]chan struct{}),
	}
}

// IsPaused reports whether the queue is currently paused in memory.
func (p *PauseController) IsPaused(queue string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pausedQueues[queue]
}

// SetPaused updates pause state and broadcasts resume when unpausing.
func (p *PauseController) SetPaused(queue string, paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldPaused := p.pausedQueues[queue]
	p.pausedQueues[queue] = paused

	// If transitioning from paused to active, close the channel to broadcast to all waiting workers
	if oldPaused && !paused {
		if ch, ok := p.pauseChans[queue]; ok {
			close(ch)
			delete(p.pauseChans, queue)
		}
	}
}

// WaitChan returns a channel that is closed when the queue is resumed.
// Callers should only wait on this channel while the queue is paused.
func (p *PauseController) WaitChan(queue string) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch, ok := p.pauseChans[queue]
	if !ok {
		ch = make(chan struct{})
		p.pauseChans[queue] = ch
	}
	return ch
}

// Reconcile reloads pause flags from Redis for the given queues.
func (p *PauseController) Reconcile(ctx context.Context, queues []string) {
	for _, q := range queues {
		pausedKey := keys.KeysFor(q).Paused()
		val, err := p.rdb.Exists(ctx, pausedKey).Result()
		if err == nil {
			p.SetPaused(q, val > 0)
		}
	}
}

// StartSubscriber listens for pause/resume control messages and periodically reconciles Redis state.
func (p *PauseController) StartSubscriber(ctx context.Context, wg *sync.WaitGroup, queues []string) {
	if len(queues) == 0 {
		return
	}

	p.Reconcile(ctx, queues)

	wg.Add(1)
	go func() {
		defer wg.Done()
		channels := make([]string, len(queues))
		for i, q := range queues {
			channels[i] = keys.KeysFor(q).Control()
		}
		pubsub := p.rdb.Subscribe(ctx, channels...)
		defer pubsub.Close()

		ch := pubsub.Channel()
		reconcileTicker := time.NewTicker(5 * time.Second)
		defer reconcileTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-reconcileTicker.C:
				p.Reconcile(ctx, queues)
			case msg, ok := <-ch:
				if !ok {
					p.logger.Warn("Control subscriber: PubSub channel closed, refreshing subscription")
					pubsub.Close()
					if ctx.Err() != nil {
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(1 * time.Second): // Backoff to prevent tight spin
					}
					pubsub = p.rdb.Subscribe(ctx, channels...)
					ch = pubsub.Channel()
					continue
				}

				var matchedQueue string
				for _, q := range queues {
					if msg.Channel == keys.KeysFor(q).Control() {
						matchedQueue = q
						break
					}
				}
				if matchedQueue == "" {
					continue
				}

				switch msg.Payload {
				case "pause":
					p.SetPaused(matchedQueue, true)
				case "resume":
					p.SetPaused(matchedQueue, false)
				}
			}
		}
	}()
}
