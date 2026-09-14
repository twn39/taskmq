package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	"go.uber.org/zap"
)

// SchedulerFactory builds a delayed scheduler.
// streamHardLimit is EnqueueHardLimit; streamMaxLen is StreamMaxLen (both 0 = unlimited/off).
type SchedulerFactory func(rdb redis.UniversalClient, logger *zap.Logger, queue string, cronManager runner.CronManager, codec codec.Codec, pollInterval time.Duration, streamHardLimit, streamMaxLen int64) runner.Runner

type SchedulerOptions struct {
	disabled     bool
	pollInterval time.Duration
	scheduler    runner.Runner
	factory      SchedulerFactory
}

// WithDisableScheduler disables automatic initialization and running of the delayed scheduler.
func WithDisableScheduler() SharedOption {
	return func(o *WorkerConfig) error {
		o.scheduler.disabled = true
		return nil
	}
}

func WithScheduler(s runner.Runner) SharedOption {
	return func(o *WorkerConfig) error {
		if s == nil {
			return errors.New("scheduler cannot be nil")
		}
		o.scheduler.scheduler = s
		return nil
	}
}

func WithSchedulerPollInterval(interval time.Duration) SharedOption {
	return func(o *WorkerConfig) error {
		if interval <= 0 {
			return fmt.Errorf("scheduler poll interval must be positive: got %v", interval)
		}
		o.scheduler.pollInterval = interval
		return nil
	}
}

func WithSchedulerFactory(f SchedulerFactory) SharedOption {
	return func(o *WorkerConfig) error {
		if f == nil {
			return errors.New("scheduler factory cannot be nil")
		}
		o.scheduler.factory = f
		return nil
	}
}
