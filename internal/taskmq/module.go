package taskmq

import (
	"context"
	"errors"
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
	"github.com/twn39/taskmq/internal/taskmq/runner"
	"github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ProvideWorkersParams defines the injected parameters for ProvideWorkers.
type ProvideWorkersParams struct {
	fx.In
	Rdb     redis.UniversalClient
	Logger  *zap.Logger
	Codec   codec.Codec
	Cfg     *config.Config
	RootCtx context.Context `optional:"true"`
	// Lifecycle is required and must be the same instance injected into client.Client
	// so admission limits and process-local metrics stay consistent.
	Lifecycle *lifecycle.Lifecycle
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
		func(rdb redis.UniversalClient, c codec.Codec, cfg *config.Config, lc *lifecycle.Lifecycle) client.Client {
			return client.NewClient(rdb,
				client.WithDefaultUniqueTTL(cfg.TaskMQ.DefaultUniqueTTL),
				client.WithClientCodec(c),
				client.WithClientLifecycle(lc),
				client.WithEventsMaxLen(cfg.TaskMQ.EventsMaxLen),
			)
		},
		// Provide Workers dynamically based on configuration
		ProvideWorkers,
		// Provide DaemonSet for background maintenance daemons
		ProvideDaemonSet,
		// Provide gRPC Server constructor
		grpcserver.NewGRPCServer,
		// Client satisfies grpcserver.API (enqueue + cron + dlq).
		func(c client.Client) grpcserver.API { return c },
	),
	// Project Client onto EnqueueClient / AdminClient / CronClient / …
	client.ProvideISP,
	fx.Invoke(
		RegisterWorkerPoolLifecycle,
		RegisterDaemonSetLifecycle,
		RegisterGRPCServerLifecycle,
	),
)

// ProvideDaemonSetParams defines injected parameters for ProvideDaemonSet.
type ProvideDaemonSetParams struct {
	fx.In
	Rdb       redis.UniversalClient
	Logger    *zap.Logger
	Codec     codec.Codec
	Cfg       *config.Config
	Lifecycle *lifecycle.Lifecycle
}

// ProvideDaemonSet constructs a DaemonSet when role is "all" or "daemon".
// When role is "worker", returns an empty DaemonSet.
func ProvideDaemonSet(p ProvideDaemonSetParams) (runner.DaemonSet, error) {
	if p.Lifecycle == nil {
		return nil, fmt.Errorf("taskmq: shared Lifecycle is required; use taskmq.Module or inject *lifecycle.Lifecycle")
	}
	role := strings.ToLower(strings.TrimSpace(p.Cfg.TaskMQ.Role))
	if role == "worker" {
		return runner.NewDaemonSet(p.Logger), nil
	}
	if role == "daemon" {
		return worker.BuildDaemonTopologyWithLifecycle(p.Rdb, p.Logger, p.Cfg, p.Codec, p.Lifecycle)
	}
	// Default "all" mode: workers host in-process daemons for full compatibility.
	return runner.NewDaemonSet(p.Logger), nil
}

// ProvideWorkers constructs and provides a Worker for all configured queues.
// The injected Lifecycle must be shared with client.Client (enforced by Module wiring).
func ProvideWorkers(p ProvideWorkersParams) (worker.Worker, error) {
	if p.Lifecycle == nil {
		return nil, fmt.Errorf("taskmq: shared Lifecycle is required; use taskmq.Module or inject *lifecycle.Lifecycle")
	}
	return worker.BuildWorkerTopologyWithLifecycle(p.Rdb, p.Logger, p.Cfg, p.Codec, p.RootCtx, p.Lifecycle)
}

// BuildWorkerTopology constructs workers with a Lifecycle derived from cfg.
// The Lifecycle instance is not shared with any Client created separately.
// Prefer BuildWorkerTopologyWithLifecycle when co-locating with a client.
func BuildWorkerTopology(rdb redis.UniversalClient, logger *zap.Logger, cfg *config.Config, c codec.Codec, rootCtx context.Context) (worker.Worker, error) {
	lc := lifecycle.NewLifecycle(LifecycleFromConfig(cfg))
	return worker.BuildWorkerTopologyWithLifecycle(rdb, logger, cfg, c, rootCtx, lc)
}

// BuildWorkerTopologyWithLifecycle reuses a shared Lifecycle instance
// (same pointer as client.WithClientLifecycle in production).
func BuildWorkerTopologyWithLifecycle(rdb redis.UniversalClient, logger *zap.Logger, cfg *config.Config, c codec.Codec, rootCtx context.Context, lc *lifecycle.Lifecycle) (worker.Worker, error) {
	if lc == nil {
		return nil, fmt.Errorf("taskmq: Lifecycle must not be nil; use lifecycle.NewLifecycle(...)")
	}
	return worker.BuildWorkerTopologyWithLifecycle(rdb, logger, cfg, c, rootCtx, lc)
}

// WorkerPoolLifecycleParams defines injected parameters for RegisterWorkerPoolLifecycle.
type WorkerPoolLifecycleParams struct {
	fx.In
	Lc  fx.Lifecycle
	W   worker.Worker
	Cfg *config.Config `optional:"true"`
}

// RegisterWorkerPoolLifecycle registers worker pool startup and shutdown inside Fx container lifecycle hooks.
func RegisterWorkerPoolLifecycle(p WorkerPoolLifecycleParams) {
	if p.Cfg != nil && strings.ToLower(strings.TrimSpace(p.Cfg.TaskMQ.Role)) == "daemon" {
		// Pure daemon role does not start stream consumer workers.
		return
	}
	p.Lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return p.W.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			p.W.Stop(ctx)
			return nil
		},
	})
}

// DaemonSetLifecycleParams defines injected parameters for RegisterDaemonSetLifecycle.
type DaemonSetLifecycleParams struct {
	fx.In
	Lc     fx.Lifecycle
	Ds     runner.DaemonSet
	Cfg    *config.Config `optional:"true"`
	Logger *zap.Logger    `optional:"true"`
}

// RegisterDaemonSetLifecycle registers the background daemonset in Fx lifecycle.
func RegisterDaemonSetLifecycle(p DaemonSetLifecycleParams) {
	if p.Cfg == nil || strings.ToLower(strings.TrimSpace(p.Cfg.TaskMQ.Role)) != "daemon" {
		return
	}
	logger := p.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	var cancel context.CancelFunc
	p.Lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			runCtx, c := context.WithCancel(context.Background())
			cancel = c
			logger.Info("Starting TaskMQ DaemonSet runners", zap.Int("runners_count", len(p.Ds.Runners())))
			go func() {
				if err := p.Ds.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("TaskMQ DaemonSet stopped with error", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping TaskMQ DaemonSet runners")
			if cancel != nil {
				cancel()
			}
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
