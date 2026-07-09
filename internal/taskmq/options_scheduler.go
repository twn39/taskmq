package taskmq

import (
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// SchedulerFactory builds a delayed scheduler. streamHardLimit is EnqueueHardLimit (0 = unlimited).
type SchedulerFactory func(rdb *redis.Client, logger *zap.Logger, queue string, cronManager CronManager, codec Codec, pollInterval time.Duration, streamHardLimit int64) Runner

type SchedulerOptions struct {
	pollInterval time.Duration
	scheduler    Runner
	factory      SchedulerFactory
}

func DefaultSchedulerFactory(rdb *redis.Client, logger *zap.Logger, queue string, cronManager CronManager, codec Codec, pollInterval time.Duration, streamHardLimit int64) Runner {
	return newDelayedScheduler(rdb, logger, queue, cronManager, codec, pollInterval, streamHardLimit)
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

func WithSchedulerPollInterval(interval time.Duration) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if interval <= 0 {
			return fmt.Errorf("scheduler poll interval must be positive: got %v", interval)
		}
		o.scheduler.pollInterval = interval
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
