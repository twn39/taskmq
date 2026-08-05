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

// RedisConfig configures the Redis connection topology.
//
// Mode:
//   - standalone (default): use Addr (or first Addrs entry)
//   - cluster: use Addrs as cluster seed nodes (DB ignored)
//   - sentinel: use MasterName + Addrs (sentinel endpoints)
type RedisConfig struct {
	// Mode is standalone | cluster | sentinel. Empty defaults to standalone.
	Mode string `mapstructure:"mode"`
	// Addr is the standalone host:port (also used when Addrs is empty).
	Addr string `mapstructure:"addr"`
	// Addrs is used for cluster seeds or sentinel hosts; optional for standalone.
	Addrs []string `mapstructure:"addrs"`
	// MasterName is required for sentinel/failover mode.
	MasterName string `mapstructure:"master_name"`
	Password   string `mapstructure:"password"`
	// DB is logical database for standalone/sentinel (ignored by cluster).
	DB       int `mapstructure:"db"`
	PoolSize int `mapstructure:"pool_size"`
}

type QueueConfig struct {
	Name              string        `mapstructure:"name"`
	Concurrency       int           `mapstructure:"concurrency"`
	Group             string        `mapstructure:"group"`
	Consumer          string        `mapstructure:"consumer"`
	Priority          int           `mapstructure:"priority"`
	RateLimitMax      int64         `mapstructure:"rate_limit_max"`
	RateLimitDuration time.Duration `mapstructure:"rate_limit_duration"`
	RateLimitKeyField string        `mapstructure:"rate_limit_key_field"`
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
	// ShutdownTimeout is how long workers wait for in-flight tasks on Stop.
	// Default 30s (aligned with default task timeout). Must be > 0.
	ShutdownTimeout       time.Duration   `mapstructure:"shutdown_timeout"`
	PriorityQueuesEnabled bool            `mapstructure:"priority_queues_enabled"`
	PriorityStrategy      string          `mapstructure:"priority_strategy"`
	Queues                []QueueConfig   `mapstructure:"queues"`
	Lifecycle             LifecycleConfig `mapstructure:"lifecycle"`
	// CompletedRetention is how long completed task meta/results are kept.
	// Zero (default) means success deletes stream entry and does not retain meta.
	CompletedRetention time.Duration `mapstructure:"completed_retention"`
	// CompletedMaxCount caps the completed ZSET per queue (0 = unlimited while retention applies).
	CompletedMaxCount int64 `mapstructure:"completed_max_count"`
	// EventsMaxLen is the approximate MAXLEN for the per-queue lifecycle events stream.
	// Zero (default) uses events.DefaultMaxLen (10_000).
	EventsMaxLen int64 `mapstructure:"events_max_len"`
}

// LifecycleConfig mirrors taskmq.LifecycleConfig for YAML/env loading.
// Zero limits mean disabled (except dlq_max_count defaulted to 1000).
type LifecycleConfig struct {
	StreamMaxLen     int64         `mapstructure:"stream_maxlen"`
	EnqueueSoftLimit int64         `mapstructure:"enqueue_soft_limit"`
	EnqueueHardLimit int64         `mapstructure:"enqueue_hard_limit"`
	DelayedMaxCount  int64         `mapstructure:"delayed_max_count"`
	DelayedMaxDelay  time.Duration `mapstructure:"delayed_max_delay"`
	DelayedOverflow  string        `mapstructure:"delayed_overflow"`
	// DLQMaxCount: nil = default 1000; ptr(0) = unlimited; >0 = cap.
	DLQMaxCount           *int64        `mapstructure:"dlq_max_count"`
	DLQMaxAge             time.Duration `mapstructure:"dlq_max_age"`
	CancelledTTL          time.Duration `mapstructure:"cancelled_ttl"`
	MaxPayloadBytes       int           `mapstructure:"max_payload_bytes"`
	SafeTrimEnabled       *bool         `mapstructure:"safe_trim_enabled"`
	SafeTrimInterval      time.Duration `mapstructure:"safe_trim_interval"`
	SafeTrimBatchLimit    int64         `mapstructure:"safe_trim_batch_limit"`
	IdleConsumerTimeout   time.Duration `mapstructure:"idle_consumer_timeout"`
	PurgeCancelledDelayed *bool         `mapstructure:"purge_cancelled_delayed"`
}

// NewConfig loads the configuration from environment variables and/or config files
func NewConfig() (*Config, error) {
	v := viper.New()

	// Set default values
	v.SetDefault("server.port", ":8080")
	v.SetDefault("server.grpc_port", ":50051")
	v.SetDefault("logger.level", "info")
	v.SetDefault("redis.mode", "standalone")
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 0)
	v.SetDefault("redis.master_name", "")
	v.SetDefault("taskmq.completed_retention", 0)
	v.SetDefault("taskmq.completed_max_count", int64(0))
	v.SetDefault("taskmq.events_max_len", int64(0)) // 0 → events.DefaultMaxLen

	v.SetDefault("taskmq.codec", "binary")
	v.SetDefault("taskmq.default_unique_ttl", 1*time.Hour)
	v.SetDefault("taskmq.cron_healing_interval", 1*time.Minute)
	v.SetDefault("taskmq.cron_healing_lock_ttl", 50*time.Second)
	v.SetDefault("taskmq.cron_healing_scan_batch_size", 100)
	v.SetDefault("taskmq.cron_healing_scan_max_count", 1000)
	v.SetDefault("taskmq.scheduler_poll_interval", 500*time.Millisecond)
	v.SetDefault("taskmq.janitor_interval", 3*time.Second)
	v.SetDefault("taskmq.janitor_min_idle_time", 5*time.Second)
	v.SetDefault("taskmq.shutdown_timeout", 30*time.Second)
	v.SetDefault("taskmq.priority_queues_enabled", false)
	v.SetDefault("taskmq.priority_strategy", "weighted")
	v.SetDefault("taskmq.queues", []map[string]interface{}{
		{
			"name":        "default",
			"concurrency": 5,
		},
	})
	// Lifecycle defaults: compatible with historical behavior (DLQ 1000, SafeTrim on).
	// Note: dlq_max_count default applied in LifecycleFromConfig when nil (not via viper pointer).
	v.SetDefault("taskmq.lifecycle.cancelled_ttl", 24*time.Hour)
	v.SetDefault("taskmq.lifecycle.delayed_overflow", "reject")
	v.SetDefault("taskmq.lifecycle.safe_trim_enabled", true)
	v.SetDefault("taskmq.lifecycle.safe_trim_interval", 30*time.Second)
	v.SetDefault("taskmq.lifecycle.safe_trim_batch_limit", int64(1000))
	v.SetDefault("taskmq.lifecycle.purge_cancelled_delayed", true)

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
