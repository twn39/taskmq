package worker

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"go.uber.org/zap"
)

// BuildWorkerTopology constructs a Worker from config using default lifecycle settings.
func BuildWorkerTopology(rdb *redis.Client, logger *zap.Logger, cfg *config.Config, c codec.Codec, rootCtx context.Context) (Worker, error) {
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	return BuildWorkerTopologyWithLifecycle(rdb, logger, cfg, c, rootCtx, lc)
}

// BuildWorkerTopologyWithLifecycle reuses a shared Lifecycle instance
// (so client and workers share metrics / policy).
func BuildWorkerTopologyWithLifecycle(rdb *redis.Client, logger *zap.Logger, cfg *config.Config, c codec.Codec, rootCtx context.Context, lc *lifecycle.Lifecycle) (Worker, error) {
	workers := make(map[string]Worker)

	queues := cfg.TaskMQ.Queues
	if len(queues) == 0 {
		queues = []config.QueueConfig{
			{
				Name:        "default",
				Concurrency: 5,
			},
		}
	}

	priorityQueuesEnabled := cfg.TaskMQ.PriorityQueuesEnabled
	priorityStrategy := cfg.TaskMQ.PriorityStrategy

	var priorityQueues []QueuePriority
	var priorityConcurrency int
	var normalQueues []config.QueueConfig

	for _, qCfg := range queues {
		if priorityQueuesEnabled && qCfg.Priority > 0 {
			priorityQueues = append(priorityQueues, QueuePriority{
				Name:              qCfg.Name,
				Weight:            qCfg.Priority,
				RateLimitMax:      qCfg.RateLimitMax,
				RateLimitDuration: qCfg.RateLimitDuration,
				RateLimitKeyField: qCfg.RateLimitKeyField,
			})
			priorityConcurrency += qCfg.Concurrency
		} else {
			normalQueues = append(normalQueues, qCfg)
		}
	}

	// 1. Instantiate the prioritized worker pool if enabled
	if len(priorityQueues) > 0 {
		if priorityConcurrency <= 0 {
			priorityConcurrency = 5
		}

		commonOpts := buildCommonOptionsWithLifecycle(cfg, c, rootCtx, lc)
		opts := toPriorityOptions(commonOpts)
		opts = append(opts, WithGroup("taskmq-priority-group"))
		opts = append(opts, WithConsumer("taskmq-priority-consumer-1"))
		opts = append(opts, WithConcurrency(priorityConcurrency))
		opts = append(opts, WithPriorityQueues(priorityQueues))
		opts = append(opts, WithPriorityStrategy(priorityStrategy))

		pw := NewPriorityWorker(rdb, logger, opts...)

		// Register the pool under all priority queue names
		for _, pq := range priorityQueues {
			workers[pq.Name] = pw
		}
	}

	// 2. Instantiate normal queues independently
	for _, qCfg := range normalQueues {
		concurrency := 5
		if qCfg.Concurrency > 0 {
			concurrency = qCfg.Concurrency
		}
		group := "taskmq-group-" + qCfg.Name
		if qCfg.Group != "" {
			group = qCfg.Group
		}
		consumer := "taskmq-consumer-" + qCfg.Name + "-1"
		if qCfg.Consumer != "" {
			consumer = qCfg.Consumer
		}

		commonOpts := buildCommonOptionsWithLifecycle(cfg, c, rootCtx, lc)
		opts := toPoolOptions(commonOpts)
		opts = append(opts, WithGroup(group))
		opts = append(opts, WithConsumer(consumer))
		opts = append(opts, WithConcurrency(concurrency))
		if qCfg.RateLimitMax > 0 && qCfg.RateLimitDuration > 0 {
			opts = append(opts, WithRateLimit(qCfg.RateLimitMax, qCfg.RateLimitDuration))
		}
		if qCfg.RateLimitKeyField != "" {
			opts = append(opts, WithRateLimitKeyField(qCfg.RateLimitKeyField))
		}

		pool := NewWorkerPool(rdb, logger, qCfg.Name, opts...)
		workers[qCfg.Name] = pool
	}

	return NewMultiQueueWorker(workers), nil
}

func buildCommonOptionsWithLifecycle(cfg *config.Config, c codec.Codec, rootCtx context.Context, lc *lifecycle.Lifecycle) []SharedOption {
	var opts []SharedOption
	if cfg.TaskMQ.CronHealingInterval > 0 {
		opts = append(opts, WithCronHealingInterval(cfg.TaskMQ.CronHealingInterval))
	}
	if cfg.TaskMQ.CronHealingLockTTL > 0 {
		opts = append(opts, WithCronHealingLockTTL(cfg.TaskMQ.CronHealingLockTTL))
	}
	if cfg.TaskMQ.CronHealingScanBatchSize > 0 {
		opts = append(opts, WithCronHealingScanBatchSize(cfg.TaskMQ.CronHealingScanBatchSize))
	}
	if cfg.TaskMQ.CronHealingScanMaxCount > 0 {
		opts = append(opts, WithCronHealingScanMaxCount(cfg.TaskMQ.CronHealingScanMaxCount))
	}
	if cfg.TaskMQ.SchedulerPollInterval > 0 {
		opts = append(opts, WithSchedulerPollInterval(cfg.TaskMQ.SchedulerPollInterval))
	}
	if cfg.TaskMQ.JanitorInterval > 0 {
		opts = append(opts, WithJanitorInterval(cfg.TaskMQ.JanitorInterval))
	}
	if cfg.TaskMQ.JanitorMinIdleTime > 0 {
		opts = append(opts, WithJanitorMinIdleTime(cfg.TaskMQ.JanitorMinIdleTime))
	}
	opts = append(opts, WithCodec(c))
	if lc != nil {
		opts = append(opts, WithLifecycle(lc))
	}
	if rootCtx != nil {
		opts = append(opts, WithContext(rootCtx))
	}
	return opts
}

func toPoolOptions(shared []SharedOption) []WorkerPoolOption {
	res := make([]WorkerPoolOption, len(shared))
	for i, o := range shared {
		res[i] = o
	}
	return res
}

func toPriorityOptions(shared []SharedOption) []PriorityWorkerOption {
	res := make([]PriorityWorkerOption, len(shared))
	for i, o := range shared {
		res[i] = o
	}
	return res
}
