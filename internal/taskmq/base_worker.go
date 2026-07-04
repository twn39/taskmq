package taskmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type baseWorker struct {
	rdb               *redis.Client
	logger            *zap.Logger
	group             string
	consumer          string
	concurrency       int
	handlers          map[string]HandlerFunc
	codec             Codec
	syncExecution     bool
	execPoolSize      int
	sem               chan struct{}
	parentCtx         context.Context
	ctx               context.Context
	cancel            context.CancelFunc
	consumerCtx       context.Context
	consumerCancel    context.CancelFunc
	wg                sync.WaitGroup

	// Rate Limiting
	limiter           *GCRALimiter
	rateLimitMax      int64
	rateLimitDuration time.Duration
	rateLimitKeyField string
	getQueueRateLimit func(qName string) (int64, time.Duration, string)
	groupKeyExtractor func([]byte) string

	// Decoupled abstractions
	broker           TaskBroker
	retryPolicy      RetryPolicy
	deadLetterPolicy DeadLetterPolicy

	middlewareChain []CoreHandlerFunc
	cancelations    *Cancelations

	// Queue Pause/Resume Tracking
	mu           sync.RWMutex
	pausedQueues map[string]bool
	pauseChans   map[string]chan struct{}
}

func (b *baseWorker) initBase(rdb *redis.Client, logger *zap.Logger, opt *workerOptions) {
	b.rdb = rdb
	b.logger = logger
	b.handlers = make(map[string]HandlerFunc)
	b.parentCtx = opt.context
	b.limiter = NewGCRALimiter(rdb)
	b.cancelations = NewCancelations()

	b.pausedQueues = make(map[string]bool)
	b.pauseChans = make(map[string]chan struct{})

	b.group = opt.group
	b.consumer = opt.consumer
	b.concurrency = opt.concurrency
	b.execPoolSize = opt.concurrency
	b.codec = opt.codec
	b.syncExecution = opt.syncExecution
	if opt.executionPoolSize > 0 {
		b.execPoolSize = opt.executionPoolSize
	}

	b.rateLimitMax = opt.rateLimitMax
	b.rateLimitDuration = opt.rateLimitDuration
	b.rateLimitKeyField = opt.rateLimitKeyField
	b.groupKeyExtractor = opt.groupKeyExtractor

	b.broker = opt.broker
	b.retryPolicy = opt.retryPolicy
	b.deadLetterPolicy = opt.deadLetterPolicy

	b.sem = make(chan struct{}, b.execPoolSize)
}

func (b *baseWorker) buildMiddlewareChain() {
	b.middlewareChain = []CoreHandlerFunc{
		RetryAndDLQMiddleware(b.broker, b.retryPolicy, b.deadLetterPolicy, b.logger),
		UniqueLockWatchdogMiddleware(b.broker, b.logger),
		RateLimitMiddleware(b.limiter, b.broker, func(qName string) (int64, time.Duration, string) {
			if b.getQueueRateLimit != nil {
				return b.getQueueRateLimit(qName)
			}
			return 0, 0, ""
		}, b.groupKeyExtractor, b.logger),
		RecoveryMiddleware(b.logger),
		func(c *ConsumeContext) error {
			handler, exists := b.handlers[c.Task.Name]
			if !exists {
				return fmt.Errorf("no handler registered: %s", c.Task.Name)
			}
			if c.Task.TimeoutMs > 0 {
				timeoutCtx, cancel := context.WithTimeout(c.Context, time.Duration(c.Task.TimeoutMs)*time.Millisecond)
				defer cancel()
				oldCtx := c.Context
				c.Context = timeoutCtx
				defer func() { c.Context = oldCtx }()
			}
			return handler(c.Context, c.Task)
		},
	}
}

func (b *baseWorker) Register(taskName string, handler HandlerFunc) {
	b.handlers[taskName] = handler
}

func (b *baseWorker) Stop(ctxs ...context.Context) {
	b.logger.Info("Stopping background loops...")

	if b.consumerCancel != nil {
		b.consumerCancel()
	}
	if b.cancel != nil {
		b.cancel()
	}

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	var waitCtx context.Context
	if len(ctxs) > 0 {
		waitCtx = ctxs[0]
	} else {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}

	select {
	case <-done:
		b.logger.Info("Worker gracefully stopped.")
	case <-waitCtx.Done():
		b.logger.Warn("Worker shutdown timeout exceeded, forcing stop.")
	}
}

func (b *baseWorker) processMessage(ctx context.Context, streamKey string, msg redis.XMessage) {
	payload, ok := msg.Values["task"].([]byte)
	if !ok {
		if payloadStr, ok := msg.Values["task"].(string); ok {
			payload = unsafeStringToBytes(payloadStr)
		} else {
			// Fallback to legacy "payload" key
			payload, ok = msg.Values["payload"].([]byte)
			if !ok {
				if payloadStr, ok := msg.Values["payload"].(string); ok {
					payload = unsafeStringToBytes(payloadStr)
				} else {
					b.logger.Error("Message payload or task must be bytes or string")
					return
				}
			}
		}
	}

	var task Task
	err := b.codec.Unmarshal(payload, &task)
	if err != nil {
		b.logger.Error("Failed to deserialize task", zap.Error(err))
		return
	}

	_, exists := b.handlers[task.Name]
	if !exists {
		b.logger.Error("No handler registered", zap.String("task_name", task.Name))
		return
	}

	// Check if task is already cancelled before execution (pre-execution check for backlog tasks)
	cancelledKey := fmt.Sprintf("taskmq:{%s}:cancelled:%s", task.Queue, task.ID)
	isCancelled, err := b.rdb.Exists(ctx, cancelledKey).Result()
	if err == nil && isCancelled > 0 {
		b.logger.Warn("Task was cancelled before execution, discarding atomically", zap.String("task_id", task.ID))
		errComplete := b.broker.CompleteTask(ctx, &task, streamKey, msg.ID, b.group)
		if errComplete != nil {
			b.logger.Error("Failed to complete task in cancel check", zap.Error(errComplete))
		}
		return
	}

	// Create cancellable context and register it in b.cancelations map
	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	b.cancelations.Add(task.ID, cancel)
	defer b.cancelations.Delete(task.ID)

	c := AcquireConsumeContext(cancellableCtx, &task, msg.ID, task.Queue, b.group, b.middlewareChain)
	defer ReleaseConsumeContext(c)

	execErr := c.Next()

	// If no error occurred during processing chain and it completed fully (not aborted)
	if execErr == nil && !c.IsAborted() {
		err = b.broker.CompleteTask(ctx, &task, streamKey, msg.ID, b.group)
		if err != nil {
			b.logger.Error("Failed to complete task",
				zap.String("task_id", task.ID),
				zap.String("stream_id", msg.ID),
				zap.Error(err),
			)
			return
		}
	}
}

// Cancelations holds cancel functions for all active tasks.
type Cancelations struct {
	mu          sync.Mutex
	cancelFuncs map[string]context.CancelFunc
}

func NewCancelations() *Cancelations {
	return &Cancelations{
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

func (c *Cancelations) Add(id string, fn context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFuncs[id] = fn
}

func (c *Cancelations) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cancelFuncs, id)
}

func (c *Cancelations) Cancel(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := c.cancelFuncs[id]; ok {
		fn()
	}
}

func (b *baseWorker) startCancelSubscriber(ctx context.Context, wg *sync.WaitGroup, queues []string) {
	if len(queues) == 0 {
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		channels := make([]string, len(queues))
		for i, q := range queues {
			channels[i] = fmt.Sprintf("taskmq:{%s}:cancel", q)
		}
		pubsub := b.rdb.Subscribe(ctx, channels...)
		defer pubsub.Close()

		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				taskID := msg.Payload
				b.cancelations.Cancel(taskID)
			}
		}
	}()
}

func (b *baseWorker) isQueuePaused(queue string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.pausedQueues[queue]
}

func (b *baseWorker) setQueuePaused(queue string, paused bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	oldPaused := b.pausedQueues[queue]
	b.pausedQueues[queue] = paused

	// If transitioning from paused to active, close the channel to broadcast to all waiting workers
	if oldPaused && !paused {
		if ch, ok := b.pauseChans[queue]; ok {
			close(ch)
			delete(b.pauseChans, queue)
		}
	}
}

func (b *baseWorker) getOrInitPauseChan(queue string) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch, ok := b.pauseChans[queue]
	if !ok {
		ch = make(chan struct{})
		b.pauseChans[queue] = ch
	}
	return ch
}

func (b *baseWorker) reconcilePausedStates(ctx context.Context, queues []string) {
	for _, q := range queues {
		pausedKey := PausedKey(q)
		val, err := b.rdb.Exists(ctx, pausedKey).Result()
		if err == nil {
			isPaused := val > 0
			b.setQueuePaused(q, isPaused)
		}
	}
}

func (b *baseWorker) startControlSubscriber(ctx context.Context, wg *sync.WaitGroup, queues []string) {
	if len(queues) == 0 {
		return
	}

	// Initialize state
	b.reconcilePausedStates(ctx, queues)

	wg.Add(1)
	go func() {
		defer wg.Done()
		channels := make([]string, len(queues))
		for i, q := range queues {
			channels[i] = ControlChannel(q)
		}
		pubsub := b.rdb.Subscribe(ctx, channels...)
		defer pubsub.Close()

		ch := pubsub.Channel()
		reconcileTicker := time.NewTicker(5 * time.Second)
		defer reconcileTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-reconcileTicker.C:
				b.reconcilePausedStates(ctx, queues)
			case msg, ok := <-ch:
				if !ok {
					return
				}

				// Find which queue channel this message belongs to
				var matchedQueue string
				for _, q := range queues {
					if msg.Channel == ControlChannel(q) {
						matchedQueue = q
						break
					}
				}

				if matchedQueue == "" {
					continue
				}

				switch msg.Payload {
				case "pause":
					b.setQueuePaused(matchedQueue, true)
				case "resume":
					b.setQueuePaused(matchedQueue, false)
				}
			}
		}
	}()
}
