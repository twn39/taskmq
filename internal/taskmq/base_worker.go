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
}

func (b *baseWorker) initBase(rdb *redis.Client, logger *zap.Logger, opt *workerOptions) {
	b.rdb = rdb
	b.logger = logger
	b.handlers = make(map[string]HandlerFunc)
	b.parentCtx = opt.context
	b.limiter = NewGCRALimiter(rdb)

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

	c := AcquireConsumeContext(ctx, &task, msg.ID, task.Queue, b.group, b.middlewareChain)
	defer ReleaseConsumeContext(c)

	execErr := c.Next()

	// If no error occurred during processing chain and it completed fully (not aborted)
	if execErr == nil && !c.IsAborted() {
		err = b.rdb.XAck(ctx, streamKey, b.group, msg.ID).Err()
		if err != nil {
			b.logger.Error("Failed to ACK task stream message",
				zap.String("task_id", task.ID),
				zap.String("stream_id", msg.ID),
				zap.Error(err),
			)
			return
		}

		if task.UniqueKey != "" {
			_ = b.broker.ReleaseUniqueLock(ctx, &task)
		}
	}
}
