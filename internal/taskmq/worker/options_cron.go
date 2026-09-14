package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	"go.uber.org/zap"
)

// CronManagerFactory builds a cron manager. lc may be nil.
type CronManagerFactory func(rdb redis.UniversalClient, logger *zap.Logger, queue string, codec codec.Codec, healingInterval time.Duration, healingLockTTL time.Duration, scanBatchSize int, scanMaxCount int, lc *lifecycle.Lifecycle) runner.CronManager

type CronOptions struct {
	disabled        bool
	healingInterval time.Duration
	lockTTL         time.Duration
	scanBatchSize   int
	scanMaxCount    int
	manager         runner.CronManager
	factory         CronManagerFactory
}

// WithDisableCron disables automatic initialization and running of the cron manager.
func WithDisableCron() SharedOption {
	return func(o *WorkerConfig) error {
		o.cron.disabled = true
		return nil
	}
}

func WithCronManager(m runner.CronManager) SharedOption {
	return func(o *WorkerConfig) error {
		if m == nil {
			return errors.New("cron manager cannot be nil")
		}
		o.cron.manager = m
		return nil
	}
}

func WithCronHealingInterval(interval time.Duration) SharedOption {
	return func(o *WorkerConfig) error {
		if interval <= 0 {
			return fmt.Errorf("cron healing interval must be positive: got %v", interval)
		}
		o.cron.healingInterval = interval
		return nil
	}
}

func WithCronHealingLockTTL(ttl time.Duration) SharedOption {
	return func(o *WorkerConfig) error {
		if ttl <= 0 {
			return fmt.Errorf("cron healing lock TTL must be positive: got %v", ttl)
		}
		o.cron.lockTTL = ttl
		return nil
	}
}

func WithCronHealingScanBatchSize(size int) SharedOption {
	return func(o *WorkerConfig) error {
		if size <= 0 {
			return fmt.Errorf("cron healing scan batch size must be positive: got %d", size)
		}
		o.cron.scanBatchSize = size
		return nil
	}
}

func WithCronHealingScanMaxCount(count int) SharedOption {
	return func(o *WorkerConfig) error {
		if count <= 0 {
			return fmt.Errorf("cron healing scan max count must be positive: got %d", count)
		}
		o.cron.scanMaxCount = count
		return nil
	}
}

func WithCronManagerFactory(f CronManagerFactory) SharedOption {
	return func(o *WorkerConfig) error {
		if f == nil {
			return errors.New("cron manager factory cannot be nil")
		}
		o.cron.factory = f
		return nil
	}
}
