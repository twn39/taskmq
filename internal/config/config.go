package config

import (
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all the global configuration for the application
type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Logger LoggerConfig `mapstructure:"logger"`
	Redis  RedisConfig  `mapstructure:"redis"`
	TaskMQ TaskMQConfig `mapstructure:"taskmq"`
}

type ServerConfig struct {
	Port     string `mapstructure:"port"`
	GRPCPort string `mapstructure:"grpc_port"`
}

type LoggerConfig struct {
	Level string `mapstructure:"level"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`
}

type QueueConfig struct {
	Name        string `mapstructure:"name"`
	Concurrency int    `mapstructure:"concurrency"`
	Group       string `mapstructure:"group"`
	Consumer    string `mapstructure:"consumer"`
}

type TaskMQConfig struct {
	Codec                    string        `mapstructure:"codec"`
	DefaultUniqueTTL         time.Duration `mapstructure:"default_unique_ttl"`
	CronHealingInterval      time.Duration `mapstructure:"cron_healing_interval"`
	CronHealingLockTTL       time.Duration `mapstructure:"cron_healing_lock_ttl"`
	CronHealingScanBatchSize int           `mapstructure:"cron_healing_scan_batch_size"`
	CronHealingScanMaxCount  int           `mapstructure:"cron_healing_scan_max_count"`
	SchedulerPollInterval    time.Duration `mapstructure:"scheduler_poll_interval"`
	JanitorInterval          time.Duration `mapstructure:"janitor_interval"`
	JanitorMinIdleTime       time.Duration `mapstructure:"janitor_min_idle_time"`
	Queues                   []QueueConfig `mapstructure:"queues"`
}

// NewConfig loads the configuration from environment variables and/or config files
func NewConfig() (*Config, error) {
	v := viper.New()

	// Set default values
	v.SetDefault("server.port", ":8080")
	v.SetDefault("server.grpc_port", ":50051")
	v.SetDefault("logger.level", "info")
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 0)

	v.SetDefault("taskmq.codec", "binary")
	v.SetDefault("taskmq.default_unique_ttl", 1*time.Hour)
	v.SetDefault("taskmq.cron_healing_interval", 1*time.Minute)
	v.SetDefault("taskmq.cron_healing_lock_ttl", 50*time.Second)
	v.SetDefault("taskmq.cron_healing_scan_batch_size", 100)
	v.SetDefault("taskmq.cron_healing_scan_max_count", 1000)
	v.SetDefault("taskmq.scheduler_poll_interval", 500*time.Millisecond)
	v.SetDefault("taskmq.janitor_interval", 3*time.Second)
	v.SetDefault("taskmq.janitor_min_idle_time", 5*time.Second)
	v.SetDefault("taskmq.queues", []map[string]interface{}{
		{
			"name":        "default",
			"concurrency": 5,
		},
	})

	// Enable environment variable support
	// This makes env vars like TASKMQ_SERVER_PORT map to server.port
	v.SetEnvPrefix("TASKMQ")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Determine config file name based on APP_ENV
	// Default: config.yaml
	// If APP_ENV=prod, looks for config.prod.yaml
	env := os.Getenv("APP_ENV")
	configName := "config"
	if env != "" {
		configName = "config." + env
	}

	// Optionally look for a config file
	v.AddConfigPath(".")
	v.SetConfigName(configName)
	v.SetConfigType("yaml")

	// Attempt to read the config file, ignore error if not found
	_ = v.ReadInConfig()

	// Unmarshal into the Config struct
	var c Config
	if err := v.Unmarshal(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
