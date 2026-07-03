package taskmq

import (
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// NewDefaultWorkerOptions returns a WorkerOptions pre-populated with default sub-components, honoring any custom overrides.
func NewDefaultWorkerOptions(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, base WorkerOptions) WorkerOptions {
	cronHealingInterval := 1 * time.Minute
	if base.CronHealingInterval > 0 {
		cronHealingInterval = base.CronHealingInterval
	}
	cronHealingLockTTL := 50 * time.Second
	if base.CronHealingLockTTL > 0 {
		cronHealingLockTTL = base.CronHealingLockTTL
	}
	cronHealingScanBatchSize := 100
	if base.CronHealingScanBatchSize > 0 {
		cronHealingScanBatchSize = base.CronHealingScanBatchSize
	}
	cronHealingScanMaxCount := 1000
	if base.CronHealingScanMaxCount > 0 {
		cronHealingScanMaxCount = base.CronHealingScanMaxCount
	}

	schedulerPollInterval := 500 * time.Millisecond
	if base.SchedulerPollInterval > 0 {
		schedulerPollInterval = base.SchedulerPollInterval
	}

	janitorInterval := 3 * time.Second
	if base.JanitorInterval > 0 {
		janitorInterval = base.JanitorInterval
	}
	janitorMinIdleTime := 5 * time.Second
	if base.JanitorMinIdleTime > 0 {
		janitorMinIdleTime = base.JanitorMinIdleTime
	}

	concurrency := 5
	if base.Concurrency > 0 {
		concurrency = base.Concurrency
	}
	group := "taskmq-group"
	if base.Group != "" {
		group = base.Group
	}
	consumer := "taskmq-consumer-1"
	if base.Consumer != "" {
		consumer = base.Consumer
	}

	if base.CronManager == nil {
		base.CronManager = newCronManager(rdb, logger, queue, codec, cronHealingInterval, cronHealingLockTTL, cronHealingScanBatchSize, cronHealingScanMaxCount)
	}
	if base.Scheduler == nil {
		base.Scheduler = newDelayedScheduler(rdb, logger, queue, base.CronManager, codec, schedulerPollInterval)
	}
	if base.Janitor == nil {
		base.Janitor = newPELRecoveryJanitor(rdb, logger, queue, group, consumer, concurrency, janitorInterval, janitorMinIdleTime, nil)
	}

	if base.Group == "" {
		base.Group = group
	}
	if base.Consumer == "" {
		base.Consumer = consumer
	}
	if base.Concurrency == 0 {
		base.Concurrency = concurrency
	}
	if base.Codec == nil {
		base.Codec = codec
	}

	return base
}
