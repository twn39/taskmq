package taskmq

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
	"github.com/twn39/taskmq/internal/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// WorkerParams defines the injected parameters for Worker, supporting optional RootCtx
type WorkerParams struct {
	fx.In
	Rdb         *redis.Client
	Logger      *zap.Logger
	Cron        CronManager
	Scheduler   Runner
	Janitor     PELRecoveryJanitor
	RootCtx     context.Context `optional:"true"`
}

// Module is the Fx module for TaskMQ dependencies
var Module = fx.Module("taskmq",
	fx.Provide(
		// Provide Client using injected config
		func(rdb *redis.Client, cfg *config.Config) Client {
			return NewClient(rdb, WithDefaultUniqueTTL(cfg.TaskMQ.DefaultUniqueTTL))
		},
		// Provide CronManager
		func(rdb *redis.Client, logger *zap.Logger, cfg *config.Config) CronManager {
			healingInterval := 1 * time.Minute
			if cfg.TaskMQ.CronHealingInterval > 0 {
				healingInterval = cfg.TaskMQ.CronHealingInterval
			}
			healingLockTTL := 50 * time.Second
			if cfg.TaskMQ.CronHealingLockTTL > 0 {
				healingLockTTL = cfg.TaskMQ.CronHealingLockTTL
			}
			scanBatchSize := 100
			if cfg.TaskMQ.CronHealingScanBatchSize > 0 {
				scanBatchSize = cfg.TaskMQ.CronHealingScanBatchSize
			}
			scanMaxCount := 1000
			if cfg.TaskMQ.CronHealingScanMaxCount > 0 {
				scanMaxCount = cfg.TaskMQ.CronHealingScanMaxCount
			}
			return newCronManager(rdb, logger, "default", JSONCodec{}, healingInterval, healingLockTTL, scanBatchSize, scanMaxCount)
		},
		// Provide DelayedScheduler as a Runner
		func(rdb *redis.Client, logger *zap.Logger, cron CronManager, cfg *config.Config) Runner {
			pollInterval := 500 * time.Millisecond
			if cfg.TaskMQ.SchedulerPollInterval > 0 {
				pollInterval = cfg.TaskMQ.SchedulerPollInterval
			}
			return newDelayedScheduler(rdb, logger, "default", cron, JSONCodec{}, pollInterval)
		},
		// Provide PELRecoveryJanitor
		func(rdb *redis.Client, logger *zap.Logger, cfg *config.Config) PELRecoveryJanitor {
			janitorInterval := 3 * time.Second
			if cfg.TaskMQ.JanitorInterval > 0 {
				janitorInterval = cfg.TaskMQ.JanitorInterval
			}
			janitorMinIdleTime := 5 * time.Second
			if cfg.TaskMQ.JanitorMinIdleTime > 0 {
				janitorMinIdleTime = cfg.TaskMQ.JanitorMinIdleTime
			}
			return newPELRecoveryJanitor(rdb, logger, "default", "taskmq-group", "taskmq-consumer-1", 5, janitorInterval, janitorMinIdleTime, nil)
		},
		// Provide Worker using injected dependencies
		func(p WorkerParams) Worker {
			opts := WorkerOptions{
				CronManager: p.Cron,
				Scheduler:   p.Scheduler,
				Janitor:     p.Janitor,
			}
			if p.RootCtx != nil {
				opts.Context = p.RootCtx
			}
			return NewWorkerPool(p.Rdb, p.Logger, "default", opts)
		},
		// Provide gRPC Server constructor
		NewGRPCServer,
	),
	fx.Invoke(
		RegisterWorkerPoolLifecycle,
		RegisterGRPCServerLifecycle,
	),
)

// RegisterWorkerPoolLifecycle registers worker pool startup and shutdown inside Fx container lifecycle hooks.
func RegisterWorkerPoolLifecycle(lc fx.Lifecycle, worker Worker) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return worker.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			worker.Stop(ctx)
			return nil
		},
	})
}

// RegisterGRPCServerLifecycle registers gRPC server startup and graceful shutdown hooks.
func RegisterGRPCServerLifecycle(lc fx.Lifecycle, grpcSrv *GRPCServer, cfg *config.Config, logger *zap.Logger) {
	s := grpc.NewServer()
	taskmqv1.RegisterTaskMQServiceServer(s, grpcSrv)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting gRPC server", zap.String("port", cfg.Server.GRPCPort))
			lis, err := net.Listen("tcp", cfg.Server.GRPCPort)
			if err != nil {
				return fmt.Errorf("failed to listen on gRPC port: %w", err)
			}

			go func() {
				if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
					logger.Error("gRPC server failed to serve", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping gRPC server gracefully")
			s.GracefulStop()
			return nil
		},
	})
}
