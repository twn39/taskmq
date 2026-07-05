package taskmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type CronManagerFactory func(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, healingInterval time.Duration, healingLockTTL time.Duration, scanBatchSize int, scanMaxCount int) CronManager
type SchedulerFactory func(rdb *redis.Client, logger *zap.Logger, queue string, cronManager CronManager, codec Codec, pollInterval time.Duration) Runner
type JanitorFactory func(rdb *redis.Client, logger *zap.Logger, queue string, group string, consumer string, concurrency int, checkInterval time.Duration, minIdleTime time.Duration) PELRecoveryJanitor

type CronOptions struct {
	healingInterval      time.Duration
	lockTTL              time.Duration
	scanBatchSize        int
	scanMaxCount         int
	manager              CronManager
	factory              CronManagerFactory
}

type SchedulerOptions struct {
	pollInterval         time.Duration
	scheduler            Runner
	factory              SchedulerFactory
}

type JanitorOptions struct {
	interval             time.Duration
	minIdleTime          time.Duration
	janitor              Runner
	factory              JanitorFactory
}

type PolicyOptions struct {
	broker               TaskBroker
	retryPolicy          RetryPolicy
	deadLetterPolicy     DeadLetterPolicy
}

type BaseWorkerOptions struct {
	group                string
	consumer             string
	concurrency          int
	codec                Codec
	syncExecution        bool
	executionPoolSize    int
	context              context.Context
	groupKeyExtractor    func([]byte) string

	cron                 CronOptions
	scheduler            SchedulerOptions
	janitor              JanitorOptions
	policies             PolicyOptions
}

type WorkerPoolOptions struct {
	BaseWorkerOptions
	rateLimitMax      int64
	rateLimitDuration time.Duration
	rateLimitKeyField string
}

type PriorityWorkerOptions struct {
	BaseWorkerOptions
	priorityQueues   []QueuePriority
	priorityStrategy string
}

// WorkerPoolOption defines functional options for WorkerPool
type WorkerPoolOption interface {
	ApplyWorkerPool(*WorkerPoolOptions) error
}

// PriorityWorkerOption defines functional options for PriorityWorker
type PriorityWorkerOption interface {
	ApplyPriorityWorker(*PriorityWorkerOptions) error
}

type poolOption func(*WorkerPoolOptions) error

func (o poolOption) ApplyWorkerPool(opts *WorkerPoolOptions) error {
	return o(opts)
}

type priorityOption func(*PriorityWorkerOptions) error

func (o priorityOption) ApplyPriorityWorker(opts *PriorityWorkerOptions) error {
	return o(opts)
}
type sharedOption func(*BaseWorkerOptions) error

func (o sharedOption) ApplyWorkerPool(opts *WorkerPoolOptions) error {
	return o(&opts.BaseWorkerOptions)
}

func (o sharedOption) ApplyPriorityWorker(opts *PriorityWorkerOptions) error {
	return o(&opts.BaseWorkerOptions)
}

func DefaultCronManagerFactory(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, healingInterval time.Duration, healingLockTTL time.Duration, scanBatchSize int, scanMaxCount int) CronManager {
	return newCronManager(rdb, logger, queue, codec, healingInterval, healingLockTTL, scanBatchSize, scanMaxCount)
}

func DefaultSchedulerFactory(rdb *redis.Client, logger *zap.Logger, queue string, cronManager CronManager, codec Codec, pollInterval time.Duration) Runner {
	return newDelayedScheduler(rdb, logger, queue, cronManager, codec, pollInterval)
}

func DefaultJanitorFactory(rdb *redis.Client, logger *zap.Logger, queue string, group string, consumer string, concurrency int, checkInterval time.Duration, minIdleTime time.Duration) PELRecoveryJanitor {
	return newPELRecoveryJanitor(rdb, logger, queue, group, consumer, concurrency, checkInterval, minIdleTime, nil)
}

func defaultBaseWorkerOptions(codec Codec) BaseWorkerOptions {
	return BaseWorkerOptions{
		concurrency:              5,
		group:                    "taskmq-group",
		consumer:                 "taskmq-consumer-1",
		codec:                    codec,
		context:                  context.Background(),
		cron: CronOptions{
			healingInterval:      1 * time.Minute,
			lockTTL:              50 * time.Second,
			scanBatchSize:        100,
			scanMaxCount:         1000,
			factory:              DefaultCronManagerFactory,
		},
		scheduler: SchedulerOptions{
			pollInterval:         500 * time.Millisecond,
			factory:              DefaultSchedulerFactory,
		},
		janitor: JanitorOptions{
			interval:             3 * time.Second,
			minIdleTime:          5 * time.Second,
			factory:              DefaultJanitorFactory,
		},
	}
}

func defaultWorkerPoolOptions(codec Codec) WorkerPoolOptions {
	return WorkerPoolOptions{
		BaseWorkerOptions: defaultBaseWorkerOptions(codec),
	}
}

func defaultPriorityWorkerOptions(codec Codec) PriorityWorkerOptions {
	return PriorityWorkerOptions{
		BaseWorkerOptions: defaultBaseWorkerOptions(codec),
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

// Option functions

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

func WithCodec(codec Codec) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if codec == nil {
			return errors.New("codec cannot be nil")
		}
		o.codec = codec
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

func WithCronHealingInterval(interval time.Duration) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if interval <= 0 {
			return fmt.Errorf("cron healing interval must be positive: got %v", interval)
		}
		o.cron.healingInterval = interval
		return nil
	}
}

func WithCronHealingLockTTL(ttl time.Duration) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if ttl <= 0 {
			return fmt.Errorf("cron healing lock TTL must be positive: got %v", ttl)
		}
		o.cron.lockTTL = ttl
		return nil
	}
}

func WithCronHealingScanBatchSize(size int) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if size <= 0 {
			return fmt.Errorf("cron healing scan batch size must be positive: got %d", size)
		}
		o.cron.scanBatchSize = size
		return nil
	}
}

func WithCronHealingScanMaxCount(count int) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if count <= 0 {
			return fmt.Errorf("cron healing scan max count must be positive: got %d", count)
		}
		o.cron.scanMaxCount = count
		return nil
	}
}

func WithSchedulerPollInterval(interval time.Duration) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if interval <= 0 {
			return fmt.Errorf("scheduler poll interval must be positive: got %v", interval)
		}
		o.scheduler.pollInterval = interval
		return nil
	}
}

func WithJanitorInterval(interval time.Duration) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if interval <= 0 {
			return fmt.Errorf("janitor interval must be positive: got %v", interval)
		}
		o.janitor.interval = interval
		return nil
	}
}

func WithJanitorMinIdleTime(idleTime time.Duration) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if idleTime <= 0 {
			return fmt.Errorf("janitor min idle time must be positive: got %v", idleTime)
		}
		o.janitor.minIdleTime = idleTime
		return nil
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

func WithPriorityQueues(queues []QueuePriority) priorityOption {
	return priorityOption(func(o *PriorityWorkerOptions) error {
		if len(queues) == 0 {
			return errors.New("priority queues cannot be empty")
		}
		o.priorityQueues = queues
		return nil
	})
}

func WithPriorityStrategy(strategy string) priorityOption {
	return priorityOption(func(o *PriorityWorkerOptions) error {
		if strategy != "strict" && strategy != "weighted" && strategy != "" {
			return fmt.Errorf("invalid priority strategy: %s (must be 'strict' or 'weighted')", strategy)
		}
		o.priorityStrategy = strategy
		return nil
	})
}

func WithRateLimit(max int64, duration time.Duration) poolOption {
	return poolOption(func(o *WorkerPoolOptions) error {
		if max <= 0 || duration <= 0 {
			return fmt.Errorf("invalid rate limit parameters: max=%d, duration=%v", max, duration)
		}
		o.rateLimitMax = max
		o.rateLimitDuration = duration
		return nil
	})
}

func WithRateLimitKeyField(field string) poolOption {
	return poolOption(func(o *WorkerPoolOptions) error {
		o.rateLimitKeyField = field
		return nil
	})
}

func WithCronManager(m CronManager) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if m == nil {
			return errors.New("cron manager cannot be nil")
		}
		o.cron.manager = m
		return nil
	}
}

func WithScheduler(s Runner) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if s == nil {
			return errors.New("scheduler cannot be nil")
		}
		o.scheduler.scheduler = s
		return nil
	}
}

func WithJanitor(j Runner) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if j == nil {
			return errors.New("janitor cannot be nil")
		}
		o.janitor.janitor = j
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

func WithCronManagerFactory(f CronManagerFactory) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if f == nil {
			return errors.New("cron manager factory cannot be nil")
		}
		o.cron.factory = f
		return nil
	}
}

func WithSchedulerFactory(f SchedulerFactory) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if f == nil {
			return errors.New("scheduler factory cannot be nil")
		}
		o.scheduler.factory = f
		return nil
	}
}

func WithJanitorFactory(f JanitorFactory) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if f == nil {
			return errors.New("janitor factory cannot be nil")
		}
		o.janitor.factory = f
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
