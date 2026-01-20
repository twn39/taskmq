package config

import (
	"strings"

	"github.com/spf13/viper"
)

// Config holds all the global configuration for the application
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Logger   LoggerConfig   `mapstructure:"logger"`
}

type ServerConfig struct {
	Port string `mapstructure:"port"`
}

type DatabaseConfig struct {
	DSN string `mapstructure:"dsn"`
}

type LoggerConfig struct {
	Level string `mapstructure:"level"`
}

// NewConfig loads the configuration from environment variables and/or config files
func NewConfig() (*Config, error) {
	v := viper.New()

	// Set default values
	v.SetDefault("server.port", ":8080")
	v.SetDefault("database.dsn", "gocms.db")
	v.SetDefault("logger.level", "info")

	// Enable environment variable support
	// This makes env vars like GOCMS_SERVER_PORT map to server.port
	v.SetEnvPrefix("GOCMS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Optionally look for a config file
	v.AddConfigPath(".")
	v.SetConfigName("config")
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
