package redis

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// NewRedisClient creates a Redis client (standalone, cluster, or sentinel) and
// manages its lifecycle via Fx. The returned type is redis.UniversalClient so
// callers work with any topology without forking APIs.
//
// Mode selection (config.redis.mode):
//   - standalone (default): single Addr (or first of Addrs)
//   - cluster: Addrs seed nodes; DB is ignored by Redis Cluster
//   - sentinel: MasterName + Addrs (sentinel endpoints)
//
// TaskMQ key layout is Cluster-safe (per-queue hash tags; see keys package).
func NewRedisClient(lc fx.Lifecycle, cfg *config.Config, logger *zap.Logger) (redis.UniversalClient, error) {
	poolSize := cfg.Redis.PoolSize
	if poolSize <= 0 {
		// 10 * CPU cores, floor 50 for small containers.
		numCPU := runtime.GOMAXPROCS(0)
		poolSize = 10 * numCPU
		if poolSize < 50 {
			poolSize = 50
		}
	}

	opts, mode, err := buildUniversalOptions(cfg.Redis, poolSize)
	if err != nil {
		return nil, err
	}

	logger.Info("Connecting to Redis",
		zap.String("mode", mode),
		zap.Strings("addrs", opts.Addrs),
		zap.String("master_name", opts.MasterName),
		zap.Int("pool_size", poolSize),
		zap.Int("db", opts.DB),
	)

	rdb := redis.NewUniversalClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("failed to connect to Redis (%s): %w", mode, err)
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logger.Info("Closing Redis connection", zap.String("mode", mode))
			return rdb.Close()
		},
	})

	return rdb, nil
}

// buildUniversalOptions maps config → go-redis UniversalOptions.
// Exported logic is unit-tested via NewUniversalOptionsFromConfig.
func buildUniversalOptions(rc config.RedisConfig, poolSize int) (*redis.UniversalOptions, string, error) {
	mode := strings.ToLower(strings.TrimSpace(rc.Mode))
	if mode == "" {
		mode = "standalone"
	}

	addrs := make([]string, 0, len(rc.Addrs)+1)
	for _, a := range rc.Addrs {
		a = strings.TrimSpace(a)
		if a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 && strings.TrimSpace(rc.Addr) != "" {
		addrs = []string{strings.TrimSpace(rc.Addr)}
	}

	switch mode {
	case "standalone", "single":
		if len(addrs) == 0 {
			return nil, mode, fmt.Errorf("redis: standalone mode requires redis.addr or redis.addrs")
		}
		// Single-node Client: use first address only.
		return &redis.UniversalOptions{
			Addrs:    []string{addrs[0]},
			Password: rc.Password,
			DB:       rc.DB,
			PoolSize: poolSize,
		}, "standalone", nil

	case "cluster":
		if len(addrs) == 0 {
			return nil, mode, fmt.Errorf("redis: cluster mode requires redis.addrs (seed nodes)")
		}
		return &redis.UniversalOptions{
			Addrs:    addrs,
			Password: rc.Password,
			// DB is not used by Cluster; leave 0.
			PoolSize: poolSize,
		}, "cluster", nil

	case "sentinel", "failover":
		if strings.TrimSpace(rc.MasterName) == "" {
			return nil, mode, fmt.Errorf("redis: sentinel mode requires redis.master_name")
		}
		if len(addrs) == 0 {
			return nil, mode, fmt.Errorf("redis: sentinel mode requires redis.addrs (sentinel hosts)")
		}
		return &redis.UniversalOptions{
			Addrs:      addrs,
			MasterName: rc.MasterName,
			Password:   rc.Password,
			DB:         rc.DB,
			PoolSize:   poolSize,
		}, "sentinel", nil

	default:
		return nil, mode, fmt.Errorf("redis: unknown mode %q (want standalone|cluster|sentinel)", mode)
	}
}
