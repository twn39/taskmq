package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	"go.uber.org/zap"
)

type JanitorFactory func(rdb redis.UniversalClient, logger *zap.Logger, queue string, group string, consumer string, concurrency int, checkInterval time.Duration, minIdleTime time.Duration) runner.PELRecoveryJanitor

type JanitorOptions struct {
	interval    time.Duration
	minIdleTime time.Duration
	janitor     runner.Runner
	factory     JanitorFactory
}

func WithJanitor(j runner.Runner) SharedOption {
	return func(o *WorkerConfig) error {
		if j == nil {
			return errors.New("janitor cannot be nil")
		}
		o.janitor.janitor = j
		return nil
	}
}

func WithJanitorInterval(interval time.Duration) SharedOption {
	return func(o *WorkerConfig) error {
		if interval <= 0 {
			return fmt.Errorf("janitor interval must be positive: got %v", interval)
		}
		o.janitor.interval = interval
		return nil
	}
}

func WithJanitorMinIdleTime(idleTime time.Duration) SharedOption {
	return func(o *WorkerConfig) error {
		if idleTime <= 0 {
			return fmt.Errorf("janitor min idle time must be positive: got %v", idleTime)
		}
		o.janitor.minIdleTime = idleTime
		return nil
	}
}

func WithJanitorFactory(f JanitorFactory) SharedOption {
	return func(o *WorkerConfig) error {
		if f == nil {
			return errors.New("janitor factory cannot be nil")
		}
		o.janitor.factory = f
		return nil
	}
}
