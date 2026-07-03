package taskmq

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
	"github.com/twn39/taskmq/internal/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ProvideWorkersParams defines the injected parameters for ProvideWorkers
type ProvideWorkersParams struct {
	fx.In
	Rdb     *redis.Client
	Logger  *zap.Logger
	Codec   Codec
	Cfg     *config.Config
	RootCtx context.Context `optional:"true"`
}

// Module is the Fx module for TaskMQ dependencies
var Module = fx.Module("taskmq",
	fx.Provide(
		// Provide Codec based on config
		func(cfg *config.Config) Codec {
			if strings.ToLower(cfg.TaskMQ.Codec) == "binary" {
				return BinaryCodec{}
			}
			return JSONCodec{}
		},
		// Provide Client using injected config and codec
		func(rdb *redis.Client, codec Codec, cfg *config.Config) Client {
			return NewClient(rdb, WithDefaultUniqueTTL(cfg.TaskMQ.DefaultUniqueTTL), WithClientCodec(codec))
		},
		// Provide Workers dynamically based on configuration
		ProvideWorkers,
		// Provide gRPC Server constructor
		NewGRPCServer,
	),
	fx.Invoke(
		RegisterWorkerPoolLifecycle,
		RegisterGRPCServerLifecycle,
	),
)

// ProvideWorkers constructs and provides a Worker (implemented by multiWorker) for all configured queues.
func ProvideWorkers(p ProvideWorkersParams) (Worker, error) {
	workers := make(map[string]Worker)

	queues := p.Cfg.TaskMQ.Queues
	if len(queues) == 0 {
		queues = []config.QueueConfig{
			{
				Name:        "default",
				Concurrency: 5,
			},
		}
	}

	for _, qCfg := range queues {
		concurrency := 5
		if qCfg.Concurrency > 0 {
			concurrency = qCfg.Concurrency
		}
		group := "taskmq-group-" + qCfg.Name
		if qCfg.Group != "" {
			group = qCfg.Group
		}
		consumer := "taskmq-consumer-" + qCfg.Name + "-1"
		if qCfg.Consumer != "" {
			consumer = qCfg.Consumer
		}

		baseOpts := WorkerOptions{
			Group:                    group,
			Consumer:                 consumer,
			Concurrency:              concurrency,
			CronHealingInterval:      p.Cfg.TaskMQ.CronHealingInterval,
			CronHealingLockTTL:       p.Cfg.TaskMQ.CronHealingLockTTL,
			CronHealingScanBatchSize: p.Cfg.TaskMQ.CronHealingScanBatchSize,
			CronHealingScanMaxCount:  p.Cfg.TaskMQ.CronHealingScanMaxCount,
			SchedulerPollInterval:    p.Cfg.TaskMQ.SchedulerPollInterval,
			JanitorInterval:          p.Cfg.TaskMQ.JanitorInterval,
			JanitorMinIdleTime:       p.Cfg.TaskMQ.JanitorMinIdleTime,
		}
		if p.RootCtx != nil {
			baseOpts.Context = p.RootCtx
		}

		opts := NewDefaultWorkerOptions(p.Rdb, p.Logger, qCfg.Name, p.Codec, baseOpts)
		pool := NewWorkerPool(p.Rdb, p.Logger, qCfg.Name, opts)
		workers[qCfg.Name] = pool
	}

	return NewMultiQueueWorker(workers), nil
}

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
