package redis

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// NewRedisClient creates and verifies a Redis client and manages its lifecycle via Fx.
func NewRedisClient(lc fx.Lifecycle, cfg *config.Config, logger *zap.Logger) (*redis.Client, error) {
	poolSize := cfg.Redis.PoolSize
	if poolSize <= 0 {
		// Calculate pool size: 10 * CPU cores, with a minimum floor of 50 to prevent
		// connection pool exhaustion in single/dual core container environments.
		numCPU := runtime.GOMAXPROCS(0)
		poolSize = 10 * numCPU
		if poolSize < 50 {
			poolSize = 50
		}
	}

	logger.Info("Connecting to Redis",
		zap.String("addr", cfg.Redis.Addr),
		zap.Int("pool_size", poolSize),
	)

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: poolSize,
	})

	// Verify connection on startup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logger.Info("Closing Redis connection")
			return rdb.Close()
		},
	})

	return rdb, nil
}
