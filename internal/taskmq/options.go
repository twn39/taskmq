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

type BaseWorkerOptions struct {
	group                    string
	consumer                 string
	concurrency              int
	codec                    Codec
	syncExecution            bool
	executionPoolSize        int
	cronHealingInterval      time.Duration
	cronHealingLockTTL       time.Duration
	cronHealingScanBatchSize int
	cronHealingScanMaxCount  int

	cronManager      CronManager
	scheduler        Runner
	janitor          Runner
	broker           TaskBroker
	retryPolicy      RetryPolicy
	deadLetterPolicy DeadLetterPolicy

	schedulerPollInterval time.Duration
	janitorInterval       time.Duration
	janitorMinIdleTime    time.Duration

	cronManagerFactory CronManagerFactory
	schedulerFactory   SchedulerFactory
	janitorFactory     JanitorFactory

	context context.Context

	groupKeyExtractor func([]byte) string
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

type sharedOption struct {
	poolFunc     func(*WorkerPoolOptions) error
	priorityFunc func(*PriorityWorkerOptions) error
}

func (o sharedOption) ApplyWorkerPool(opts *WorkerPoolOptions) error {
	return o.poolFunc(opts)
}

func (o sharedOption) ApplyPriorityWorker(opts *PriorityWorkerOptions) error {
	return o.priorityFunc(opts)
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
		cronHealingInterval:      1 * time.Minute,
		cronHealingLockTTL:       50 * time.Second,
		cronHealingScanBatchSize: 100,
		cronHealingScanMaxCount:  1000,
		schedulerPollInterval:    500 * time.Millisecond,
		janitorInterval:          3 * time.Second,
		janitorMinIdleTime:       5 * time.Second,
		cronManagerFactory:       DefaultCronManagerFactory,
		schedulerFactory:         DefaultSchedulerFactory,
		janitorFactory:           DefaultJanitorFactory,
		context:                  context.Background(),
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
	if opts.broker == nil {
		opts.broker = NewRedisBroker(rdb, opts.codec)
	}
	if opts.retryPolicy == nil {
		opts.retryPolicy = NewExponentialBackoff(100*time.Millisecond, 1*time.Hour, true)
	}
	if opts.deadLetterPolicy == nil {
		opts.deadLetterPolicy = NewStandardDeadLetterPolicy("", nil)
	}
}

func buildDefaultPoolComponents(rdb *redis.Client, logger *zap.Logger, queue string, opts *WorkerPoolOptions) {
	if opts.cronManager == nil && opts.cronManagerFactory != nil {
		opts.cronManager = opts.cronManagerFactory(rdb, logger, queue, opts.codec, opts.cronHealingInterval, opts.cronHealingLockTTL, opts.cronHealingScanBatchSize, opts.cronHealingScanMaxCount)
	}
	if opts.scheduler == nil && opts.schedulerFactory != nil {
		opts.scheduler = opts.schedulerFactory(rdb, logger, queue, opts.cronManager, opts.codec, opts.schedulerPollInterval)
	}
	if opts.janitor == nil && opts.janitorFactory != nil {
		opts.janitor = opts.janitorFactory(rdb, logger, queue, opts.group, opts.consumer, opts.concurrency, opts.janitorInterval, opts.janitorMinIdleTime)
	}
	buildSharedPoolDefaults(rdb, opts)
}

func buildSharedPriorityDefaults(rdb *redis.Client, opts *PriorityWorkerOptions) {
	if opts.broker == nil {
		opts.broker = NewRedisBroker(rdb, opts.codec)
	}
	if opts.retryPolicy == nil {
		opts.retryPolicy = NewExponentialBackoff(100*time.Millisecond, 1*time.Hour, true)
	}
	if opts.deadLetterPolicy == nil {
		opts.deadLetterPolicy = NewStandardDeadLetterPolicy("", nil)
	}
}

// Option functions


func WithGroup(group string) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if group == "" {
				return errors.New("group name cannot be empty")
			}
			o.group = group
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if group == "" {
				return errors.New("group name cannot be empty")
			}
			o.group = group
			return nil
		},
	}
}

func WithConsumer(consumer string) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if consumer == "" {
				return errors.New("consumer name cannot be empty")
			}
			o.consumer = consumer
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if consumer == "" {
				return errors.New("consumer name cannot be empty")
			}
			o.consumer = consumer
			return nil
		},
	}
}

func WithConcurrency(concurrency int) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if concurrency <= 0 {
				return fmt.Errorf("concurrency must be greater than 0: got %d", concurrency)
			}
			o.concurrency = concurrency
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if concurrency <= 0 {
				return fmt.Errorf("concurrency must be greater than 0: got %d", concurrency)
			}
			o.concurrency = concurrency
			return nil
		},
	}
}

func WithCodec(codec Codec) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if codec == nil {
				return errors.New("codec cannot be nil")
			}
			o.codec = codec
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if codec == nil {
				return errors.New("codec cannot be nil")
			}
			o.codec = codec
			return nil
		},
	}
}

func WithSyncExecution(syncExecution bool) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			o.syncExecution = syncExecution
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			o.syncExecution = syncExecution
			return nil
		},
	}
}

func WithExecutionPoolSize(size int) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if size <= 0 {
				return fmt.Errorf("execution pool size must be greater than 0: got %d", size)
			}
			o.executionPoolSize = size
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if size <= 0 {
				return fmt.Errorf("execution pool size must be greater than 0: got %d", size)
			}
			o.executionPoolSize = size
			return nil
		},
	}
}

func WithCronHealingInterval(interval time.Duration) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if interval <= 0 {
				return fmt.Errorf("cron healing interval must be positive: got %v", interval)
			}
			o.cronHealingInterval = interval
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if interval <= 0 {
				return fmt.Errorf("cron healing interval must be positive: got %v", interval)
			}
			o.cronHealingInterval = interval
			return nil
		},
	}
}

func WithCronHealingLockTTL(ttl time.Duration) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if ttl <= 0 {
				return fmt.Errorf("cron healing lock TTL must be positive: got %v", ttl)
			}
			o.cronHealingLockTTL = ttl
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if ttl <= 0 {
				return fmt.Errorf("cron healing lock TTL must be positive: got %v", ttl)
			}
			o.cronHealingLockTTL = ttl
			return nil
		},
	}
}

func WithCronHealingScanBatchSize(size int) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if size <= 0 {
				return fmt.Errorf("cron healing scan batch size must be positive: got %d", size)
			}
			o.cronHealingScanBatchSize = size
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if size <= 0 {
				return fmt.Errorf("cron healing scan batch size must be positive: got %d", size)
			}
			o.cronHealingScanBatchSize = size
			return nil
		},
	}
}

func WithCronHealingScanMaxCount(count int) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if count <= 0 {
				return fmt.Errorf("cron healing scan max count must be positive: got %d", count)
			}
			o.cronHealingScanMaxCount = count
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if count <= 0 {
				return fmt.Errorf("cron healing scan max count must be positive: got %d", count)
			}
			o.cronHealingScanMaxCount = count
			return nil
		},
	}
}

func WithSchedulerPollInterval(interval time.Duration) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if interval <= 0 {
				return fmt.Errorf("scheduler poll interval must be positive: got %v", interval)
			}
			o.schedulerPollInterval = interval
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if interval <= 0 {
				return fmt.Errorf("scheduler poll interval must be positive: got %v", interval)
			}
			o.schedulerPollInterval = interval
			return nil
		},
	}
}

func WithJanitorInterval(interval time.Duration) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if interval <= 0 {
				return fmt.Errorf("janitor interval must be positive: got %v", interval)
			}
			o.janitorInterval = interval
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if interval <= 0 {
				return fmt.Errorf("janitor interval must be positive: got %v", interval)
			}
			o.janitorInterval = interval
			return nil
		},
	}
}

func WithJanitorMinIdleTime(idleTime time.Duration) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if idleTime <= 0 {
				return fmt.Errorf("janitor min idle time must be positive: got %v", idleTime)
			}
			o.janitorMinIdleTime = idleTime
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if idleTime <= 0 {
				return fmt.Errorf("janitor min idle time must be positive: got %v", idleTime)
			}
			o.janitorMinIdleTime = idleTime
			return nil
		},
	}
}

func WithContext(ctx context.Context) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if ctx == nil {
				return errors.New("context cannot be nil")
			}
			o.context = ctx
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if ctx == nil {
				return errors.New("context cannot be nil")
			}
			o.context = ctx
			return nil
		},
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
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if m == nil {
				return errors.New("cron manager cannot be nil")
			}
			o.cronManager = m
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if m == nil {
				return errors.New("cron manager cannot be nil")
			}
			o.cronManager = m
			return nil
		},
	}
}

func WithScheduler(s Runner) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if s == nil {
				return errors.New("scheduler cannot be nil")
			}
			o.scheduler = s
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if s == nil {
				return errors.New("scheduler cannot be nil")
			}
			o.scheduler = s
			return nil
		},
	}
}

func WithJanitor(j Runner) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if j == nil {
				return errors.New("janitor cannot be nil")
			}
			o.janitor = j
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if j == nil {
				return errors.New("janitor cannot be nil")
			}
			o.janitor = j
			return nil
		},
	}
}

func WithBroker(b TaskBroker) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if b == nil {
				return errors.New("broker cannot be nil")
			}
			o.broker = b
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if b == nil {
				return errors.New("broker cannot be nil")
			}
			o.broker = b
			return nil
		},
	}
}

func WithRetryPolicy(p RetryPolicy) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if p == nil {
				return errors.New("retry policy cannot be nil")
			}
			o.retryPolicy = p
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if p == nil {
				return errors.New("retry policy cannot be nil")
			}
			o.retryPolicy = p
			return nil
		},
	}
}

func WithDeadLetterPolicy(p DeadLetterPolicy) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if p == nil {
				return errors.New("dead letter policy cannot be nil")
			}
			o.deadLetterPolicy = p
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if p == nil {
				return errors.New("dead letter policy cannot be nil")
			}
			o.deadLetterPolicy = p
			return nil
		},
	}
}

func WithCronManagerFactory(f CronManagerFactory) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if f == nil {
				return errors.New("cron manager factory cannot be nil")
			}
			o.cronManagerFactory = f
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if f == nil {
				return errors.New("cron manager factory cannot be nil")
			}
			o.cronManagerFactory = f
			return nil
		},
	}
}

func WithSchedulerFactory(f SchedulerFactory) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if f == nil {
				return errors.New("scheduler factory cannot be nil")
			}
			o.schedulerFactory = f
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if f == nil {
				return errors.New("scheduler factory cannot be nil")
			}
			o.schedulerFactory = f
			return nil
		},
	}
}

func WithJanitorFactory(f JanitorFactory) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if f == nil {
				return errors.New("janitor factory cannot be nil")
			}
			o.janitorFactory = f
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if f == nil {
				return errors.New("janitor factory cannot be nil")
			}
			o.janitorFactory = f
			return nil
		},
	}
}

func WithGroupKeyExtractor(extractor func([]byte) string) sharedOption {
	return sharedOption{
		poolFunc: func(o *WorkerPoolOptions) error {
			if extractor == nil {
				return errors.New("group key extractor cannot be nil")
			}
			o.groupKeyExtractor = extractor
			return nil
		},
		priorityFunc: func(o *PriorityWorkerOptions) error {
			if extractor == nil {
				return errors.New("group key extractor cannot be nil")
			}
			o.groupKeyExtractor = extractor
			return nil
		},
	}
}
