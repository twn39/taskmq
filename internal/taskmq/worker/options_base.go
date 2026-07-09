package worker

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	"github.com/twn39/taskmq/internal/taskmq/runner"
)

type PolicyOptions struct {
	broker           broker.TaskBroker
	retryPolicy      policy.RetryPolicy
	deadLetterPolicy policy.DeadLetterPolicy
}

// WorkerConfig is the unified configuration shared by pool and priority workers.
// Functional options (SharedOption) mutate this single structure; pool/priority
// wrappers only add mode-specific fields (rate limit, priority queues).
type WorkerConfig struct {
	group             string
	consumer          string
	concurrency       int
	codec             codec.Codec
	syncExecution     bool
	executionPoolSize int
	executionPool     ExecutionPool
	context           context.Context
	groupKeyExtractor func([]byte) string
	shutdownTimeout   time.Duration

	cron      CronOptions
	scheduler SchedulerOptions
	janitor   JanitorOptions
	policies  PolicyOptions

	// lifecycle bounds Redis memory (admission, DLQ, SafeTrim).
	lifecycle        *lifecycle.Lifecycle
	retentionJanitor runner.Runner // optional override; built from lifecycle when nil
}

// SharedOption applies base worker configuration and is valid for both pool and priority workers.
type SharedOption func(*WorkerConfig) error

func (o SharedOption) ApplyWorkerPool(opts *WorkerPoolOptions) error {
	return o(&opts.WorkerConfig)
}

func (o SharedOption) ApplyPriorityWorker(opts *PriorityWorkerOptions) error {
	return o(&opts.WorkerConfig)
}

func defaultWorkerConfig(codec codec.Codec) WorkerConfig {
	return WorkerConfig{
		concurrency:     5,
		group:           "taskmq-group",
		consumer:        "taskmq-consumer-1",
		codec:           codec,
		context:         context.Background(),
		shutdownTimeout: 1 * time.Second,
		cron: CronOptions{
			healingInterval: 1 * time.Minute,
			lockTTL:         50 * time.Second,
			scanBatchSize:   100,
			scanMaxCount:    1000,
			factory:         runner.DefaultCronManagerFactory,
		},
		scheduler: SchedulerOptions{
			pollInterval: 500 * time.Millisecond,
			factory:      runner.DefaultSchedulerFactory,
		},
		janitor: JanitorOptions{
			interval:    3 * time.Second,
			minIdleTime: 5 * time.Second,
			factory:     runner.DefaultJanitorFactory,
		},
	}
}

// ensurePolicyDefaults fills broker / retry / DLQ policies when unset.
// Used by both pool and priority workers (single mapping path).
func ensurePolicyDefaults(rdb *redis.Client, cfg *WorkerConfig) {
	if cfg.policies.broker == nil {
		cfg.policies.broker = broker.NewRedisBroker(rdb, cfg.codec, cfg.lifecycle)
	}
	if cfg.policies.retryPolicy == nil {
		cfg.policies.retryPolicy = policy.NewExponentialBackoff(100*time.Millisecond, 1*time.Hour, true)
	}
	if cfg.policies.deadLetterPolicy == nil {
		cfg.policies.deadLetterPolicy = policy.NewStandardDeadLetterPolicy("", nil)
	}
}

// Apply applies shared options onto this config (single entry for SharedOption lists).
func (c *WorkerConfig) Apply(opts ...SharedOption) error {
	for _, o := range opts {
		if o == nil {
			continue
		}
		if err := o(c); err != nil {
			return err
		}
	}
	return nil
}

// BaseWorkerOptions is a compatibility alias for WorkerConfig.
type BaseWorkerOptions = WorkerConfig


// WithLifecycle attaches memory / admission lifecycle policy to workers and brokers.
func WithLifecycle(lc *lifecycle.Lifecycle) SharedOption {
	return func(o *WorkerConfig) error {
		if lc == nil {
			return errors.New("lifecycle cannot be nil")
		}
		o.lifecycle = lc
		return nil
	}
}

// WithRetentionJanitor overrides the retention janitor runner.
func WithRetentionJanitor(r runner.Runner) SharedOption {
	return func(o *WorkerConfig) error {
		if r == nil {
			return errors.New("retention janitor cannot be nil")
		}
		o.retentionJanitor = r
		return nil
	}
}

func streamHardLimitFrom(lc *lifecycle.Lifecycle) int64 {
	if lc == nil {
		return 0
	}
	return lc.Config().EnqueueHardLimit
}

func streamMaxLenFrom(lc *lifecycle.Lifecycle) int64 {
	if lc == nil {
		return 0
	}
	return lc.Config().StreamMaxLen
}

func WithContext(ctx context.Context) SharedOption {
	return func(o *WorkerConfig) error {
		if ctx == nil {
			return errors.New("context cannot be nil")
		}
		o.context = ctx
		return nil
	}
}

func WithCodec(codec codec.Codec) SharedOption {
	return func(o *WorkerConfig) error {
		if codec == nil {
			return errors.New("codec cannot be nil")
		}
		o.codec = codec
		return nil
	}
}

func WithGroupKeyExtractor(extractor func([]byte) string) SharedOption {
	return func(o *WorkerConfig) error {
		if extractor == nil {
			return errors.New("group key extractor cannot be nil")
		}
		o.groupKeyExtractor = extractor
		return nil
	}
}

func WithBroker(b broker.TaskBroker) SharedOption {
	return func(o *WorkerConfig) error {
		if b == nil {
			return errors.New("broker cannot be nil")
		}
		o.policies.broker = b
		return nil
	}
}

func WithRetryPolicy(p policy.RetryPolicy) SharedOption {
	return func(o *WorkerConfig) error {
		if p == nil {
			return errors.New("retry policy cannot be nil")
		}
		o.policies.retryPolicy = p
		return nil
	}
}

func WithDeadLetterPolicy(p policy.DeadLetterPolicy) SharedOption {
	return func(o *WorkerConfig) error {
		if p == nil {
			return errors.New("dead letter policy cannot be nil")
		}
		o.policies.deadLetterPolicy = p
		return nil
	}
}

func WithExecutionPool(pool ExecutionPool) SharedOption {
	return func(o *WorkerConfig) error {
		if pool == nil {
			return errors.New("execution pool cannot be nil")
		}
		o.executionPool = pool
		return nil
	}
}

func WithShutdownTimeout(d time.Duration) SharedOption {
	return func(o *WorkerConfig) error {
		if d <= 0 {
			return errors.New("shutdown timeout must be greater than 0")
		}
		o.shutdownTimeout = d
		return nil
	}
}
