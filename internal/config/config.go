package config

import (
	"os"
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
	Port         string `mapstructure:"port"`
	TemplateGlob string `mapstructure:"template_glob"`
	ManifestPath string `mapstructure:"manifest_path"`
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
	v.SetDefault("server.template_glob", "views/*.html")
	v.SetDefault("server.manifest_path", "static/.vite/manifest.json")
	v.SetDefault("database.dsn", "gocms.db")
	v.SetDefault("logger.level", "info")

	// Enable environment variable support
	// This makes env vars like GOCMS_SERVER_PORT map to server.port
	v.SetEnvPrefix("GOCMS")
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
