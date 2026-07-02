package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all the global configuration for the application
type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Logger LoggerConfig `mapstructure:"logger"`
	Redis  RedisConfig  `mapstructure:"redis"`
}

type ServerConfig struct {
	Port string `mapstructure:"port"`
}

type LoggerConfig struct {
	Level string `mapstructure:"level"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// NewConfig loads the configuration from environment variables and/or config files
func NewConfig() (*Config, error) {
	v := viper.New()

	// Set default values
	v.SetDefault("server.port", ":8080")
	v.SetDefault("logger.level", "info")
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

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
