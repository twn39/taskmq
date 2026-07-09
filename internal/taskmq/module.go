package taskmq

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/grpcserver"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ProvideWorkersParams defines the injected parameters for ProvideWorkers.
type ProvideWorkersParams struct {
	fx.In
	Rdb     *redis.Client
	Logger  *zap.Logger
	Codec   codec.Codec
	Cfg     *config.Config
	RootCtx context.Context `optional:"true"`
	// Lifecycle is optional: when omitted, ProvideWorkers builds one from config.
	Lifecycle *lifecycle.Lifecycle `optional:"true"`
}

// Module is the Fx module for TaskMQ dependencies.
var Module = fx.Module("taskmq",
	fx.Provide(
		// Provide Codec based on config
		func(cfg *config.Config) codec.Codec {
			if strings.ToLower(cfg.TaskMQ.Codec) == "binary" {
				return codec.BinaryCodec{}
			}
			return codec.JSONCodec{}
		},
		// Provide shared Lifecycle from config
		func(cfg *config.Config) *lifecycle.Lifecycle {
			return lifecycle.NewLifecycle(LifecycleFromConfig(cfg))
		},
		// Provide Client facade + ISP projections (zero-cost adapters).
		func(rdb *redis.Client, c codec.Codec, cfg *config.Config, lc *lifecycle.Lifecycle) client.Client {
			return client.NewClient(rdb,
				client.WithDefaultUniqueTTL(cfg.TaskMQ.DefaultUniqueTTL),
				client.WithClientCodec(c),
				client.WithClientLifecycle(lc),
			)
		},
		// Provide Workers dynamically based on configuration
		ProvideWorkers,
		// Provide gRPC Server constructor
		grpcserver.NewGRPCServer,
		// Client satisfies grpcserver.API (enqueue + cron + dlq).
		func(c client.Client) grpcserver.API { return c },
	),
	// Project Client onto EnqueueClient / AdminClient / CronClient / …
	client.ProvideISP,
	fx.Invoke(
		RegisterWorkerPoolLifecycle,
		RegisterGRPCServerLifecycle,
	),
)

// ProvideWorkers constructs and provides a Worker for all configured queues.
func ProvideWorkers(p ProvideWorkersParams) (worker.Worker, error) {
	lc := p.Lifecycle
	if lc == nil {
		lc = lifecycle.NewLifecycle(LifecycleFromConfig(p.Cfg))
	}
	return worker.BuildWorkerTopologyWithLifecycle(p.Rdb, p.Logger, p.Cfg, p.Codec, p.RootCtx, lc)
}

// BuildWorkerTopology constructs and returns a Worker based on the configuration.
func BuildWorkerTopology(rdb *redis.Client, logger *zap.Logger, cfg *config.Config, c codec.Codec, rootCtx context.Context) (worker.Worker, error) {
	lc := lifecycle.NewLifecycle(LifecycleFromConfig(cfg))
	return worker.BuildWorkerTopologyWithLifecycle(rdb, logger, cfg, c, rootCtx, lc)
}

// BuildWorkerTopologyWithLifecycle reuses a shared Lifecycle instance.
func BuildWorkerTopologyWithLifecycle(rdb *redis.Client, logger *zap.Logger, cfg *config.Config, c codec.Codec, rootCtx context.Context, lc *lifecycle.Lifecycle) (worker.Worker, error) {
	return worker.BuildWorkerTopologyWithLifecycle(rdb, logger, cfg, c, rootCtx, lc)
}

// RegisterWorkerPoolLifecycle registers worker pool startup and shutdown inside Fx container lifecycle hooks.
func RegisterWorkerPoolLifecycle(lc fx.Lifecycle, w worker.Worker) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return w.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			w.Stop(ctx)
			return nil
		},
	})
}

// RegisterGRPCServerLifecycle registers gRPC server startup and graceful shutdown hooks.
func RegisterGRPCServerLifecycle(lc fx.Lifecycle, grpcSrv *grpcserver.GRPCServer, cfg *config.Config, logger *zap.Logger) {
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

// LifecycleFromConfig maps config into lifecycle settings.
func LifecycleFromConfig(cfg *config.Config) lifecycle.LifecycleConfig {
	return lifecycle.FromConfig(cfg)
}
