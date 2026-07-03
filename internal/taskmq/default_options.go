package taskmq

import (
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// NewDefaultWorkerOptions returns a WorkerOptions pre-populated with default sub-components, honoring any custom overrides.
func NewDefaultWorkerOptions(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, base WorkerOptions) WorkerOptions {
	base.ApplyDefaults(rdb, logger, queue, codec)
	return base
}

// ApplyDefaults populates all zero-values in WorkerOptions with sensible defaults.
func (o *WorkerOptions) ApplyDefaults(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec) {
	if o.CronHealingInterval <= 0 {
		o.CronHealingInterval = 1 * time.Minute
	}
	if o.CronHealingLockTTL <= 0 {
		o.CronHealingLockTTL = 50 * time.Second
	}
	if o.CronHealingScanBatchSize <= 0 {
		o.CronHealingScanBatchSize = 100
	}
	if o.CronHealingScanMaxCount <= 0 {
		o.CronHealingScanMaxCount = 1000
	}
	if o.SchedulerPollInterval <= 0 {
		o.SchedulerPollInterval = 500 * time.Millisecond
	}
	if o.JanitorInterval <= 0 {
		o.JanitorInterval = 3 * time.Second
	}
	if o.JanitorMinIdleTime <= 0 {
		o.JanitorMinIdleTime = 5 * time.Second
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 5
	}
	if o.Group == "" {
		o.Group = "taskmq-group"
	}
	if o.Consumer == "" {
		o.Consumer = "taskmq-consumer-1"
	}
	if o.Codec == nil {
		o.Codec = codec
	}
	if o.CronManager == nil {
		o.CronManager = newCronManager(rdb, logger, queue, o.Codec, o.CronHealingInterval, o.CronHealingLockTTL, o.CronHealingScanBatchSize, o.CronHealingScanMaxCount)
	}
	if o.Scheduler == nil {
		o.Scheduler = newDelayedScheduler(rdb, logger, queue, o.CronManager, o.Codec, o.SchedulerPollInterval)
	}
	if o.Janitor == nil {
		o.Janitor = newPELRecoveryJanitor(rdb, logger, queue, o.Group, o.Consumer, o.Concurrency, o.JanitorInterval, o.JanitorMinIdleTime, nil)
	}
}
