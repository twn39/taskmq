package taskmq

import (
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type CronManagerFactory func(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, healingInterval time.Duration, healingLockTTL time.Duration, scanBatchSize int, scanMaxCount int) CronManager

type CronOptions struct {
	healingInterval      time.Duration
	lockTTL              time.Duration
	scanBatchSize        int
	scanMaxCount         int
	manager              CronManager
	factory              CronManagerFactory
}

func DefaultCronManagerFactory(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, healingInterval time.Duration, healingLockTTL time.Duration, scanBatchSize int, scanMaxCount int) CronManager {
	return newCronManager(rdb, logger, queue, codec, healingInterval, healingLockTTL, scanBatchSize, scanMaxCount)
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

func WithCronManagerFactory(f CronManagerFactory) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if f == nil {
			return errors.New("cron manager factory cannot be nil")
		}
		o.cron.factory = f
		return nil
	}
}
