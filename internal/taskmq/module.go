package taskmq

import (
	"context"

	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Module is the Fx module for TaskMQ dependencies
var Module = fx.Module("taskmq",
	fx.Provide(
		NewClient,
		// Provide WorkerPool with a default queue named "default"
		func(rdb *redis.Client, logger *zap.Logger) *WorkerPool {
			return NewWorkerPool(rdb, logger, "default")
		},
	),
	fx.Invoke(RegisterWorkerPoolLifecycle),
)

// RegisterWorkerPoolLifecycle registers worker pool startup and shutdown inside Fx container lifecycle hooks.
func RegisterWorkerPoolLifecycle(lc fx.Lifecycle, worker *WorkerPool) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return worker.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			worker.Stop()
			return nil
		},
	})
}
