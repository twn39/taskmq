package taskmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// workerOptions is the internal configuration struct for WorkerPool.
type workerOptions struct {
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

	context context.Context

	priorityQueues   []QueuePriority
	priorityStrategy string

	rateLimitMax      int64
	rateLimitDuration time.Duration
	rateLimitKeyField string
}

// WorkerOption defines the functional option signature.
type WorkerOption func(*workerOptions) error

func defaultWorkerOptions(codec Codec) workerOptions {
	return workerOptions{
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
		context:                  context.Background(),
	}
}

func buildDefaultComponents(rdb *redis.Client, logger *zap.Logger, queue string, opts *workerOptions) {
	if opts.cronManager == nil {
		opts.cronManager = newCronManager(rdb, logger, queue, opts.codec, opts.cronHealingInterval, opts.cronHealingLockTTL, opts.cronHealingScanBatchSize, opts.cronHealingScanMaxCount)
	}
	if opts.scheduler == nil {
		opts.scheduler = newDelayedScheduler(rdb, logger, queue, opts.cronManager, opts.codec, opts.schedulerPollInterval)
	}
	if opts.janitor == nil {
		opts.janitor = newPELRecoveryJanitor(rdb, logger, queue, opts.group, opts.consumer, opts.concurrency, opts.janitorInterval, opts.janitorMinIdleTime, nil)
	}
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

func WithGroup(group string) WorkerOption {
	return func(o *workerOptions) error {
		if group == "" {
			return errors.New("group name cannot be empty")
		}
		o.group = group
		return nil
	}
}

func WithConsumer(consumer string) WorkerOption {
	return func(o *workerOptions) error {
		if consumer == "" {
			return errors.New("consumer name cannot be empty")
		}
		o.consumer = consumer
		return nil
	}
}

func WithConcurrency(concurrency int) WorkerOption {
	return func(o *workerOptions) error {
		if concurrency <= 0 {
			return fmt.Errorf("concurrency must be greater than 0: got %d", concurrency)
		}
		o.concurrency = concurrency
		return nil
	}
}

func WithCodec(codec Codec) WorkerOption {
	return func(o *workerOptions) error {
		if codec == nil {
			return errors.New("codec cannot be nil")
		}
		o.codec = codec
		return nil
	}
}

func WithSyncExecution(syncExecution bool) WorkerOption {
	return func(o *workerOptions) error {
		o.syncExecution = syncExecution
		return nil
	}
}

func WithExecutionPoolSize(size int) WorkerOption {
	return func(o *workerOptions) error {
		if size <= 0 {
			return fmt.Errorf("execution pool size must be greater than 0: got %d", size)
		}
		o.executionPoolSize = size
		return nil
	}
}

func WithCronHealingInterval(interval time.Duration) WorkerOption {
	return func(o *workerOptions) error {
		if interval <= 0 {
			return fmt.Errorf("cron healing interval must be positive: got %v", interval)
		}
		o.cronHealingInterval = interval
		return nil
	}
}

func WithCronHealingLockTTL(ttl time.Duration) WorkerOption {
	return func(o *workerOptions) error {
		if ttl <= 0 {
			return fmt.Errorf("cron healing lock TTL must be positive: got %v", ttl)
		}
		o.cronHealingLockTTL = ttl
		return nil
	}
}

func WithCronHealingScanBatchSize(size int) WorkerOption {
	return func(o *workerOptions) error {
		if size <= 0 {
			return fmt.Errorf("cron healing scan batch size must be positive: got %d", size)
		}
		o.cronHealingScanBatchSize = size
		return nil
	}
}

func WithCronHealingScanMaxCount(count int) WorkerOption {
	return func(o *workerOptions) error {
		if count <= 0 {
			return fmt.Errorf("cron healing scan max count must be positive: got %d", count)
		}
		o.cronHealingScanMaxCount = count
		return nil
	}
}

func WithSchedulerPollInterval(interval time.Duration) WorkerOption {
	return func(o *workerOptions) error {
		if interval <= 0 {
			return fmt.Errorf("scheduler poll interval must be positive: got %v", interval)
		}
		o.schedulerPollInterval = interval
		return nil
	}
}

func WithJanitorInterval(interval time.Duration) WorkerOption {
	return func(o *workerOptions) error {
		if interval <= 0 {
			return fmt.Errorf("janitor interval must be positive: got %v", interval)
		}
		o.janitorInterval = interval
		return nil
	}
}

func WithJanitorMinIdleTime(idleTime time.Duration) WorkerOption {
	return func(o *workerOptions) error {
		if idleTime <= 0 {
			return fmt.Errorf("janitor min idle time must be positive: got %v", idleTime)
		}
		o.janitorMinIdleTime = idleTime
		return nil
	}
}

func WithContext(ctx context.Context) WorkerOption {
	return func(o *workerOptions) error {
		if ctx == nil {
			return errors.New("context cannot be nil")
		}
		o.context = ctx
		return nil
	}
}

func WithPriorityQueues(queues []QueuePriority) WorkerOption {
	return func(o *workerOptions) error {
		if len(queues) == 0 {
			return errors.New("priority queues cannot be empty")
		}
		o.priorityQueues = queues
		return nil
	}
}

func WithPriorityStrategy(strategy string) WorkerOption {
	return func(o *workerOptions) error {
		if strategy != "strict" && strategy != "weighted" && strategy != "" {
			return fmt.Errorf("invalid priority strategy: %s (must be 'strict' or 'weighted')", strategy)
		}
		o.priorityStrategy = strategy
		return nil
	}
}

func WithRateLimit(max int64, duration time.Duration) WorkerOption {
	return func(o *workerOptions) error {
		if max <= 0 || duration <= 0 {
			return fmt.Errorf("invalid rate limit parameters: max=%d, duration=%v", max, duration)
		}
		o.rateLimitMax = max
		o.rateLimitDuration = duration
		return nil
	}
}

func WithRateLimitKeyField(field string) WorkerOption {
	return func(o *workerOptions) error {
		o.rateLimitKeyField = field
		return nil
	}
}

func WithCronManager(m CronManager) WorkerOption {
	return func(o *workerOptions) error {
		if m == nil {
			return errors.New("cron manager cannot be nil")
		}
		o.cronManager = m
		return nil
	}
}

func WithScheduler(s Runner) WorkerOption {
	return func(o *workerOptions) error {
		if s == nil {
			return errors.New("scheduler cannot be nil")
		}
		o.scheduler = s
		return nil
	}
}

func WithJanitor(j Runner) WorkerOption {
	return func(o *workerOptions) error {
		if j == nil {
			return errors.New("janitor cannot be nil")
		}
		o.janitor = j
		return nil
	}
}

func WithBroker(b TaskBroker) WorkerOption {
	return func(o *workerOptions) error {
		if b == nil {
			return errors.New("broker cannot be nil")
		}
		o.broker = b
		return nil
	}
}

func WithRetryPolicy(p RetryPolicy) WorkerOption {
	return func(o *workerOptions) error {
		if p == nil {
			return errors.New("retry policy cannot be nil")
		}
		o.retryPolicy = p
		return nil
	}
}

func WithDeadLetterPolicy(p DeadLetterPolicy) WorkerOption {
	return func(o *workerOptions) error {
		if p == nil {
			return errors.New("dead letter policy cannot be nil")
		}
		o.deadLetterPolicy = p
		return nil
	}
}

// --- Legacy Compatibility Layer ---

// WorkerOptions is retained only for testing and backward compatibility.
type WorkerOptions struct {
	Group                    string
	Consumer                 string
	Concurrency              int
	Codec                    Codec
	SyncExecution            bool
	ExecutionPoolSize        int
	CronHealingInterval      time.Duration
	CronHealingLockTTL       time.Duration
	CronHealingScanBatchSize int
	CronHealingScanMaxCount  int
	CronManager              CronManager
	Scheduler                Runner
	Janitor                  Runner
	Broker                   TaskBroker
	RetryPolicy              RetryPolicy
	DeadLetterPolicy         DeadLetterPolicy
	SchedulerPollInterval    time.Duration
	JanitorInterval          time.Duration
	JanitorMinIdleTime       time.Duration
	Context                  context.Context
	PriorityQueues           []QueuePriority
	PriorityStrategy         string
	RateLimitMax             int64
	RateLimitDuration        time.Duration
	RateLimitKeyField        string
}

func NewDefaultWorkerOptions(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, base WorkerOptions) []WorkerOption {
	var opts []WorkerOption
	if base.Group != "" {
		opts = append(opts, WithGroup(base.Group))
	}
	if base.Consumer != "" {
		opts = append(opts, WithConsumer(base.Consumer))
	}
	if base.Concurrency > 0 {
		opts = append(opts, WithConcurrency(base.Concurrency))
	}
	if codec != nil {
		opts = append(opts, WithCodec(codec))
	}
	if base.SyncExecution {
		opts = append(opts, WithSyncExecution(base.SyncExecution))
	}
	if base.ExecutionPoolSize > 0 {
		opts = append(opts, WithExecutionPoolSize(base.ExecutionPoolSize))
	}
	if base.CronHealingInterval > 0 {
		opts = append(opts, WithCronHealingInterval(base.CronHealingInterval))
	}
	if base.CronHealingLockTTL > 0 {
		opts = append(opts, WithCronHealingLockTTL(base.CronHealingLockTTL))
	}
	if base.CronHealingScanBatchSize > 0 {
		opts = append(opts, WithCronHealingScanBatchSize(base.CronHealingScanBatchSize))
	}
	if base.CronHealingScanMaxCount > 0 {
		opts = append(opts, WithCronHealingScanMaxCount(base.CronHealingScanMaxCount))
	}
	if base.CronManager != nil {
		opts = append(opts, WithCronManager(base.CronManager))
	}
	if base.Scheduler != nil {
		opts = append(opts, WithScheduler(base.Scheduler))
	}
	if base.Janitor != nil {
		opts = append(opts, WithJanitor(base.Janitor))
	}
	if base.Broker != nil {
		opts = append(opts, WithBroker(base.Broker))
	}
	if base.RetryPolicy != nil {
		opts = append(opts, WithRetryPolicy(base.RetryPolicy))
	}
	if base.DeadLetterPolicy != nil {
		opts = append(opts, WithDeadLetterPolicy(base.DeadLetterPolicy))
	}
	if base.SchedulerPollInterval > 0 {
		opts = append(opts, WithSchedulerPollInterval(base.SchedulerPollInterval))
	}
	if base.JanitorInterval > 0 {
		opts = append(opts, WithJanitorInterval(base.JanitorInterval))
	}
	if base.JanitorMinIdleTime > 0 {
		opts = append(opts, WithJanitorMinIdleTime(base.JanitorMinIdleTime))
	}
	if base.Context != nil {
		opts = append(opts, WithContext(base.Context))
	}
	if len(base.PriorityQueues) > 0 {
		opts = append(opts, WithPriorityQueues(base.PriorityQueues))
	}
	if base.PriorityStrategy != "" {
		opts = append(opts, WithPriorityStrategy(base.PriorityStrategy))
	}
	if base.RateLimitMax > 0 && base.RateLimitDuration > 0 {
		opts = append(opts, WithRateLimit(base.RateLimitMax, base.RateLimitDuration))
	}
	if base.RateLimitKeyField != "" {
		opts = append(opts, WithRateLimitKeyField(base.RateLimitKeyField))
	}
	return opts
}

// ApplyDefaults retains compatibility for old tests invoking opts.ApplyDefaults() directly.
func (o *WorkerOptions) ApplyDefaults(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec) {
	// NOP or simple logic since NewWorkerPool/NewPriorityWorker handles it dynamically.
}
