package integration

import (
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
)

// NewTestConfig provides a configuration for testing
func NewTestConfig() *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			Port:     ":8081",
			GRPCPort: ":50085",
		},
		Logger: config.LoggerConfig{
			Level: "error", // Quiet logs during test
		},
		Redis: config.RedisConfig{
			Addr: "localhost:6379",
		},
	}
}

// ProvideSharedLifecycle builds a Lifecycle from config for Fx graphs that use
// taskmq.ProvideWorkers (Lifecycle is required and should be shared with Client).
func ProvideSharedLifecycle(cfg *config.Config) *lifecycle.Lifecycle {
	return lifecycle.NewLifecycle(taskmq.LifecycleFromConfig(cfg))
}

// ProvideClientWithLifecycle wires a client that shares the same Lifecycle as workers.
func ProvideClientWithLifecycle(rdb *redis.Client, lc *lifecycle.Lifecycle) mqclient.Client {
	return mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
}
