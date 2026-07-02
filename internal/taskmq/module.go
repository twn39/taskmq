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

// Module is the Fx module for TaskMQ dependencies
var Module = fx.Module("taskmq",
	fx.Provide(
		NewClient,
		// Provide CronManager
		func(rdb *redis.Client, logger *zap.Logger) CronManager {
			return newCronManager(rdb, logger, "default", JSONCodec{}, 1*time.Minute, 50*time.Second)
		},
		// Provide DelayedScheduler as a Runner
		func(rdb *redis.Client, logger *zap.Logger, cron CronManager) Runner {
			return newDelayedScheduler(rdb, logger, "default", cron, JSONCodec{}, 500*time.Millisecond)
		},
		// Provide PELRecoveryJanitor
		func(rdb *redis.Client, logger *zap.Logger) PELRecoveryJanitor {
			return newPELRecoveryJanitor(rdb, logger, "default", "taskmq-group", "taskmq-consumer-1", 5, 3*time.Second, 5*time.Second, nil)
		},
		// Provide Worker using injected dependencies
		func(rdb *redis.Client, logger *zap.Logger, cron CronManager, scheduler Runner, janitor PELRecoveryJanitor) Worker {
			return NewWorkerPool(rdb, logger, "default", WorkerOptions{
				CronManager: cron,
				Scheduler:   scheduler,
				Janitor:     janitor,
			})
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
			worker.Stop()
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
