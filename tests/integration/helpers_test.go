package integration

import "github.com/twn39/taskmq/internal/config"

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
