package taskmq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Runner defines a component that can run in a background goroutine until context cancellation
type Runner interface {
	Run(ctx context.Context) error
}

// CronManager defines the interface for managing cron jobs and running self-healing checks
type CronManager interface {
	Runner
	Reschedule(ctx context.Context, task *Task) error
}

// PELRecoveryJanitor defines the interface for the PEL recovery loop
type PELRecoveryJanitor interface {
	Runner
	RegisterProcessor(fn func(ctx context.Context, msg redis.XMessage))
}

