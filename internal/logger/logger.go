package logger

import (
	"github.com/twn39/gocms/internal/config"
	"go.uber.org/zap"
)

// NewLogger creates a new zap logger
func NewLogger(cfg *config.Config) (*zap.Logger, error) {
	config := zap.NewDevelopmentConfig()

	level, err := zap.ParseAtomicLevel(cfg.Logger.Level)
	if err == nil {
		config.Level = level
	}

	return config.Build()
}
