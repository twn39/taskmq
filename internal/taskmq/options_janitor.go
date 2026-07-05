package taskmq

import (
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type JanitorFactory func(rdb *redis.Client, logger *zap.Logger, queue string, group string, consumer string, concurrency int, checkInterval time.Duration, minIdleTime time.Duration) PELRecoveryJanitor

type JanitorOptions struct {
	interval             time.Duration
	minIdleTime          time.Duration
	janitor              Runner
	factory              JanitorFactory
}

func DefaultJanitorFactory(rdb *redis.Client, logger *zap.Logger, queue string, group string, consumer string, concurrency int, checkInterval time.Duration, minIdleTime time.Duration) PELRecoveryJanitor {
	return newPELRecoveryJanitor(rdb, logger, queue, group, consumer, concurrency, checkInterval, minIdleTime, nil)
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

func WithJanitorFactory(f JanitorFactory) sharedOption {
	return func(o *BaseWorkerOptions) error {
		if f == nil {
			return errors.New("janitor factory cannot be nil")
		}
		o.janitor.factory = f
		return nil
	}
}
