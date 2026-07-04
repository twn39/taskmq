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

// ProvideWorkers constructs and provides a Worker (implemented by multiWorker or priorityWorker) for all configured queues.
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

	priorityQueuesEnabled := p.Cfg.TaskMQ.PriorityQueuesEnabled
	priorityStrategy := p.Cfg.TaskMQ.PriorityStrategy

	var priorityQueues []QueuePriority
	var priorityConcurrency int
	var normalQueues []config.QueueConfig

	for _, qCfg := range queues {
		if priorityQueuesEnabled && qCfg.Priority > 0 {
			priorityQueues = append(priorityQueues, QueuePriority{
				Name:              qCfg.Name,
				Weight:            qCfg.Priority,
				RateLimitMax:      qCfg.RateLimitMax,
				RateLimitDuration: qCfg.RateLimitDuration,
				RateLimitKeyField: qCfg.RateLimitKeyField,
			})
			priorityConcurrency += qCfg.Concurrency
		} else {
			normalQueues = append(normalQueues, qCfg)
		}
	}

	// 1. Instantiate the prioritized worker pool if enabled
	if len(priorityQueues) > 0 {
		if priorityConcurrency <= 0 {
			priorityConcurrency = 5
		}

		commonOpts := buildCommonOptions(p.Cfg, p.Codec, p.RootCtx)
		opts := toPriorityOptions(commonOpts)
		opts = append(opts, WithGroup("taskmq-priority-group"))
		opts = append(opts, WithConsumer("taskmq-priority-consumer-1"))
		opts = append(opts, WithConcurrency(priorityConcurrency))
		opts = append(opts, WithPriorityQueues(priorityQueues))
		opts = append(opts, WithPriorityStrategy(priorityStrategy))

		pw := NewPriorityWorker(p.Rdb, p.Logger, opts...)

		// Register the pool under all priority queue names
		for _, pq := range priorityQueues {
			workers[pq.Name] = pw
		}
	}

	// 2. Instantiate normal queues independently
	for _, qCfg := range normalQueues {
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

		commonOpts := buildCommonOptions(p.Cfg, p.Codec, p.RootCtx)
		opts := toPoolOptions(commonOpts)
		opts = append(opts, WithGroup(group))
		opts = append(opts, WithConsumer(consumer))
		opts = append(opts, WithConcurrency(concurrency))
		if qCfg.RateLimitMax > 0 && qCfg.RateLimitDuration > 0 {
			opts = append(opts, WithRateLimit(qCfg.RateLimitMax, qCfg.RateLimitDuration))
		}
		if qCfg.RateLimitKeyField != "" {
			opts = append(opts, WithRateLimitKeyField(qCfg.RateLimitKeyField))
		}

		pool := NewWorkerPool(p.Rdb, p.Logger, qCfg.Name, opts...)
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

func buildCommonOptions(cfg *config.Config, codec Codec, rootCtx context.Context) []sharedOption {
	var opts []sharedOption
	if cfg.TaskMQ.CronHealingInterval > 0 {
		opts = append(opts, WithCronHealingInterval(cfg.TaskMQ.CronHealingInterval))
	}
	if cfg.TaskMQ.CronHealingLockTTL > 0 {
		opts = append(opts, WithCronHealingLockTTL(cfg.TaskMQ.CronHealingLockTTL))
	}
	if cfg.TaskMQ.CronHealingScanBatchSize > 0 {
		opts = append(opts, WithCronHealingScanBatchSize(cfg.TaskMQ.CronHealingScanBatchSize))
	}
	if cfg.TaskMQ.CronHealingScanMaxCount > 0 {
		opts = append(opts, WithCronHealingScanMaxCount(cfg.TaskMQ.CronHealingScanMaxCount))
	}
	if cfg.TaskMQ.SchedulerPollInterval > 0 {
		opts = append(opts, WithSchedulerPollInterval(cfg.TaskMQ.SchedulerPollInterval))
	}
	if cfg.TaskMQ.JanitorInterval > 0 {
		opts = append(opts, WithJanitorInterval(cfg.TaskMQ.JanitorInterval))
	}
	if cfg.TaskMQ.JanitorMinIdleTime > 0 {
		opts = append(opts, WithJanitorMinIdleTime(cfg.TaskMQ.JanitorMinIdleTime))
	}
	opts = append(opts, WithCodec(codec))
	if rootCtx != nil {
		opts = append(opts, WithContext(rootCtx))
	}
	return opts
}

func toPoolOptions(shared []sharedOption) []WorkerPoolOption {
	res := make([]WorkerPoolOption, len(shared))
	for i, o := range shared {
		res[i] = o
	}
	return res
}

func toPriorityOptions(shared []sharedOption) []PriorityWorkerOption {
	res := make([]PriorityWorkerOption, len(shared))
	for i, o := range shared {
		res[i] = o
	}
	return res
}
