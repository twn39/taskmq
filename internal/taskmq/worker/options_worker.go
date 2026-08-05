package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	"go.uber.org/zap"
)

// WorkerPoolOptions is WorkerConfig plus single-queue rate-limit settings.
type WorkerPoolOptions struct {
	WorkerConfig
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

func defaultWorkerPoolOptions(codec codec.Codec) WorkerPoolOptions {
	return WorkerPoolOptions{
		WorkerConfig: defaultWorkerConfig(codec),
	}
}

func buildDefaultPoolComponents(rdb redis.UniversalClient, logger *zap.Logger, queue string, opts *WorkerPoolOptions) {
	if opts.cron.manager == nil && opts.cron.factory != nil {
		opts.cron.manager = opts.cron.factory(rdb, logger, queue, opts.codec, opts.cron.healingInterval, opts.cron.lockTTL, opts.cron.scanBatchSize, opts.cron.scanMaxCount, opts.lifecycle)
	}
	if opts.scheduler.scheduler == nil && opts.scheduler.factory != nil {
		opts.scheduler.scheduler = opts.scheduler.factory(rdb, logger, queue, opts.cron.manager, opts.codec, opts.scheduler.pollInterval, streamHardLimitFrom(opts.lifecycle), streamMaxLenFrom(opts.lifecycle))
	}
	if opts.janitor.janitor == nil {
		if opts.janitor.factory != nil {
			opts.janitor.janitor = opts.janitor.factory(rdb, logger, queue, opts.group, opts.consumer, opts.concurrency, opts.janitor.interval, opts.janitor.minIdleTime)
		} else {
			opts.janitor.janitor = runner.NewPELRecoveryJanitorWithCodec(
				rdb, logger, queue, opts.group, opts.consumer, opts.concurrency,
				opts.janitor.interval, opts.janitor.minIdleTime, opts.codec, 0,
			)
		}
	}
	if opts.retentionJanitor == nil && opts.lifecycle != nil {
		opts.retentionJanitor = runner.NewRetentionJanitor(rdb, logger, queue, opts.group, opts.codec, opts.lifecycle)
	}
	ensurePolicyDefaults(rdb, &opts.WorkerConfig)
}

// applyWorkerPoolOptions fills a WorkerPoolOptions from mixed option list.
func applyWorkerPoolOptions(codec codec.Codec, opts []WorkerPoolOption) (WorkerPoolOptions, error) {
	cfg := defaultWorkerPoolOptions(codec)
	for _, o := range opts {
		if o == nil {
			continue
		}
		if err := o.ApplyWorkerPool(&cfg); err != nil {
			return WorkerPoolOptions{}, err
		}
	}
	return cfg, nil
}

func WithGroup(group string) SharedOption {
	return func(o *WorkerConfig) error {
		if group == "" {
			return errors.New("group name cannot be empty")
		}
		o.group = group
		return nil
	}
}

func WithConsumer(consumer string) SharedOption {
	return func(o *WorkerConfig) error {
		if consumer == "" {
			return errors.New("consumer name cannot be empty")
		}
		o.consumer = consumer
		return nil
	}
}

func WithConcurrency(concurrency int) SharedOption {
	return func(o *WorkerConfig) error {
		if concurrency <= 0 {
			return fmt.Errorf("concurrency must be greater than 0: got %d", concurrency)
		}
		o.concurrency = concurrency
		return nil
	}
}

func WithSyncExecution(syncExecution bool) SharedOption {
	return func(o *WorkerConfig) error {
		o.syncExecution = syncExecution
		return nil
	}
}

func WithExecutionPoolSize(size int) SharedOption {
	return func(o *WorkerConfig) error {
		if size <= 0 {
			return fmt.Errorf("execution pool size must be greater than 0: got %d", size)
		}
		o.executionPoolSize = size
		return nil
	}
}
