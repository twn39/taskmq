package taskmq

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type PolicyOptions struct {
	broker           TaskBroker
	retryPolicy      RetryPolicy
	deadLetterPolicy DeadLetterPolicy
}

type BaseWorkerOptions struct {
	group             string
	consumer          string
	concurrency       int
	codec             Codec
	syncExecution     bool
	executionPoolSize int
	context           context.Context
	groupKeyExtractor func([]byte) string

	cron      CronOptions
	scheduler SchedulerOptions
	janitor   JanitorOptions
	policies  PolicyOptions
}

type sharedOption func(*BaseWorkerOptions) error

func (o sharedOption) ApplyWorkerPool(opts *WorkerPoolOptions) error {
	return o(&opts.BaseWorkerOptions)
}

func (o sharedOption) ApplyPriorityWorker(opts *PriorityWorkerOptions) error {
	return o(&opts.BaseWorkerOptions)
}

func defaultBaseWorkerOptions(codec Codec) BaseWorkerOptions {
	return BaseWorkerOptions{
		concurrency: 5,
		group:       "taskmq-group",
		consumer:    "taskmq-consumer-1",
		codec:       codec,
		context:     context.Background(),
		cron: CronOptions{
			healingInterval: 1 * time.Minute,
			lockTTL:         50 * time.Second,
			scanBatchSize:   100,
			scanMaxCount:    1000,
			factory:         DefaultCronManagerFactory,
		},
		scheduler: SchedulerOptions{
			pollInterval: 500 * time.Millisecond,
			factory:      DefaultSchedulerFactory,
		},
		janitor: JanitorOptions{
			interval:    3 * time.Second,
			minIdleTime: 5 * time.Second,
			factory:     DefaultJanitorFactory,
		},
	}
}

func buildSharedPoolDefaults(rdb *redis.Client, opts *WorkerPoolOptions) {
	if opts.policies.broker == nil {
		opts.policies.broker = NewRedisBroker(rdb, opts.codec)
	}
	if opts.policies.retryPolicy == nil {
		opts.policies.retryPolicy = NewExponentialBackoff(100*time.Millisecond, 1*time.Hour, true)
	}
	if opts.policies.deadLetterPolicy == nil {
		opts.policies.deadLetterPolicy = NewStandardDeadLetterPolicy("", nil)
	}
}

func buildSharedPriorityDefaults(rdb *redis.Client, opts *PriorityWorkerOptions) {
	if opts.policies.broker == nil {
		opts.policies.broker = NewRedisBroker(rdb, opts.codec)
	}
	if opts.policies.retryPolicy == nil {
		opts.policies.retryPolicy = NewExponentialBackoff(100*time.Millisecond, 1*time.Hour, true)
	}
	if opts.policies.deadLetterPolicy == nil {
		opts.policies.deadLetterPolicy = NewStandardDeadLetterPolicy("", nil)
	}
}

func WithContext(ctx context.Context) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if ctx == nil {
			return errors.New("context cannot be nil")
		}
		o.context = ctx
		return nil
	}
}

func WithCodec(codec Codec) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if codec == nil {
			return errors.New("codec cannot be nil")
		}
		o.codec = codec
		return nil
	}
}

func WithGroupKeyExtractor(extractor func([]byte) string) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if extractor == nil {
			return errors.New("group key extractor cannot be nil")
		}
		o.groupKeyExtractor = extractor
		return nil
	}
}

func WithBroker(b TaskBroker) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if b == nil {
			return errors.New("broker cannot be nil")
		}
		o.policies.broker = b
		return nil
	}
}

func WithRetryPolicy(p RetryPolicy) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if p == nil {
			return errors.New("retry policy cannot be nil")
		}
		o.policies.retryPolicy = p
		return nil
	}
}

func WithDeadLetterPolicy(p DeadLetterPolicy) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if p == nil {
			return errors.New("dead letter policy cannot be nil")
		}
		o.policies.deadLetterPolicy = p
		return nil
	}
}
