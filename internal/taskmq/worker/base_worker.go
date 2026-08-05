package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/events"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	"github.com/twn39/taskmq/internal/taskmq/ratelimit"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

// baseWorker is the shared runtime kernel for pool and priority workers.
// Pause, cancel, and message processing are delegated to focused collaborators.
type baseWorker struct {
	rdb            redis.UniversalClient
	logger         *zap.Logger
	group          string
	consumer       string
	concurrency    int
	handlers       map[string]HandlerFunc
	codec          codec.Codec
	syncExecution  bool
	execPoolSize   int
	execPool       ExecutionPool
	parentCtx      context.Context
	ctx            context.Context
	cancel         context.CancelFunc
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	wg             sync.WaitGroup

	// Rate Limiting
	limiter           *ratelimit.GCRALimiter
	rateLimitMax      int64
	rateLimitDuration time.Duration
	rateLimitKeyField string
	getQueueRateLimit func(qName string) (int64, time.Duration, string)
	groupKeyExtractor func([]byte) string

	// Decoupled abstractions
	broker           broker.TaskBroker
	retryPolicy      policy.RetryPolicy
	deadLetterPolicy policy.DeadLetterPolicy

	middlewareChain []CoreHandlerFunc

	// Focused collaborators (extracted from former God Object surface)
	pause     *PauseController
	cancelHub *CancelHub
	processor *MessageProcessor

	// Graceful shutdown timeout configuration
	shutdownTimeout time.Duration
}

func (b *baseWorker) initBase(rdb redis.UniversalClient, logger *zap.Logger, opt *WorkerConfig) {
	b.rdb = rdb
	b.logger = logger
	b.handlers = make(map[string]HandlerFunc)
	b.parentCtx = opt.context
	b.limiter = ratelimit.NewGCRALimiter(rdb)

	b.pause = NewPauseController(rdb, logger)
	b.cancelHub = NewCancelHub(rdb, logger)

	b.group = opt.group
	b.consumer = opt.consumer
	b.concurrency = opt.concurrency
	b.execPoolSize = opt.concurrency
	b.codec = opt.codec
	b.syncExecution = opt.syncExecution
	if opt.executionPoolSize > 0 {
		b.execPoolSize = opt.executionPoolSize
	}

	b.groupKeyExtractor = opt.groupKeyExtractor
	b.shutdownTimeout = opt.shutdownTimeout

	b.broker = opt.policies.broker
	b.retryPolicy = opt.policies.retryPolicy
	b.deadLetterPolicy = opt.policies.deadLetterPolicy

	if opt.executionPool != nil {
		b.execPool = opt.executionPool
	} else {
		b.execPool = NewSemaphoreExecutionPool(b.execPoolSize)
	}
}

type semaphoreExecutionPool struct {
	sem  chan struct{}
	size int
}

// NewSemaphoreExecutionPool creates a default ExecutionPool based on a buffered channel.
func NewSemaphoreExecutionPool(size int) ExecutionPool {
	if size <= 0 {
		size = 1
	}
	return &semaphoreExecutionPool{
		sem:  make(chan struct{}, size),
		size: size,
	}
}

func (p *semaphoreExecutionPool) Acquire(ctx context.Context) error {
	select {
	case p.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *semaphoreExecutionPool) Release() {
	select {
	case <-p.sem:
	default:
	}
}

func (p *semaphoreExecutionPool) Size() int {
	return p.size
}

func (p *semaphoreExecutionPool) InUse() int {
	return len(p.sem)
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
		}, b.groupKeyExtractor, b.codec, b.logger),
		RecoveryMiddleware(b.logger),
		func(c *ConsumeContext) error {
			handler, exists := b.handlers[c.Task.Name]
			if !exists {
				return fmt.Errorf("%w: %s", taskmodel.ErrNoHandler, c.Task.Name)
			}
			if eff := c.Task.EffectiveTimeout(time.Now()); eff > 0 {
				timeoutCtx, cancel := context.WithTimeout(c.Context, eff)
				defer cancel()
				oldCtx := c.Context
				c.Context = timeoutCtx
				defer func() { c.Context = oldCtx }()
			}
			return handler(c.Context, c.Task)
		},
	}

	// Wire message processor to share handler map + middleware chain by reference.
	b.processor = &MessageProcessor{
		rdb:              b.rdb,
		logger:           b.logger,
		group:            b.group,
		codec:            b.codec,
		broker:           b.broker,
		deadLetterPolicy: b.deadLetterPolicy,
		cancel:           b.cancelHub.Cancelations,
		handlers:         b.handlers,
		middlewareChain:  b.middlewareChain,
		meta:             meta.NewStore(b.rdb),
		events:           events.NewPublisher(b.rdb, 0),
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
	var cancel context.CancelFunc
	if len(ctxs) > 0 {
		waitCtx, cancel = context.WithTimeout(ctxs[0], b.shutdownTimeout)
	} else {
		waitCtx, cancel = context.WithTimeout(context.Background(), b.shutdownTimeout)
	}
	defer cancel()

	select {
	case <-done:
		b.logger.Info("Worker gracefully stopped.")
	case <-waitCtx.Done():
		b.logger.Warn("Worker shutdown timeout exceeded, forcing stop.")
	}
}

func (b *baseWorker) processMessage(ctx context.Context, streamKey string, msg redis.XMessage) {
	if b.processor == nil {
		// Defensive: ensure processor exists even if buildMiddlewareChain was skipped.
		b.buildMiddlewareChain()
	}
	b.processor.Process(ctx, streamKey, msg)
}

// Compatibility wrappers so pool/priority loops keep a stable call style.

func (b *baseWorker) isQueuePaused(queue string) bool {
	return b.pause.IsPaused(queue)
}

func (b *baseWorker) getOrInitPauseChan(queue string) <-chan struct{} {
	return b.pause.WaitChan(queue)
}

func (b *baseWorker) startCancelSubscriber(ctx context.Context, wg *sync.WaitGroup, queues []string) {
	b.cancelHub.StartSubscriber(ctx, wg, queues)
}

func (b *baseWorker) startControlSubscriber(ctx context.Context, wg *sync.WaitGroup, queues []string) {
	b.pause.StartSubscriber(ctx, wg, queues)
}
