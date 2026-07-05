package taskmq

import (
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type WorkerPoolOptions struct {
	BaseWorkerOptions
	rateLimitMax      int64
	rateLimitDuration time.Duration
	rateLimitKeyField string
}

type WorkerPoolOption interface {
	ApplyWorkerPool(*WorkerPoolOptions) error
}

type poolOption func(*WorkerPoolOptions) error

func (o poolOption) ApplyWorkerPool(opts *WorkerPoolOptions) error {
	return o(opts)
}

func defaultWorkerPoolOptions(codec Codec) WorkerPoolOptions {
	return WorkerPoolOptions{
		BaseWorkerOptions: defaultBaseWorkerOptions(codec),
	}
}

func buildDefaultPoolComponents(rdb *redis.Client, logger *zap.Logger, queue string, opts *WorkerPoolOptions) {
	if opts.cron.manager == nil && opts.cron.factory != nil {
		opts.cron.manager = opts.cron.factory(rdb, logger, queue, opts.codec, opts.cron.healingInterval, opts.cron.lockTTL, opts.cron.scanBatchSize, opts.cron.scanMaxCount)
	}
	if opts.scheduler.scheduler == nil && opts.scheduler.factory != nil {
		opts.scheduler.scheduler = opts.scheduler.factory(rdb, logger, queue, opts.cron.manager, opts.codec, opts.scheduler.pollInterval)
	}
	if opts.janitor.janitor == nil && opts.janitor.factory != nil {
		opts.janitor.janitor = opts.janitor.factory(rdb, logger, queue, opts.group, opts.consumer, opts.concurrency, opts.janitor.interval, opts.janitor.minIdleTime)
	}
	buildSharedPoolDefaults(rdb, opts)
}

func WithGroup(group string) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if group == "" {
			return errors.New("group name cannot be empty")
		}
		o.group = group
		return nil
	}
}

func WithConsumer(consumer string) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if consumer == "" {
			return errors.New("consumer name cannot be empty")
		}
		o.consumer = consumer
		return nil
	}
}

func WithConcurrency(concurrency int) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if concurrency <= 0 {
			return fmt.Errorf("concurrency must be greater than 0: got %d", concurrency)
		}
		o.concurrency = concurrency
		return nil
	}
}

func WithSyncExecution(syncExecution bool) sharedOption {
	return func(o *BaseWorkerOptions) error {
		o.syncExecution = syncExecution
		return nil
	}
}

func WithExecutionPoolSize(size int) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if size <= 0 {
			return fmt.Errorf("execution pool size must be greater than 0: got %d", size)
		}
		o.executionPoolSize = size
		return nil
	}
}
