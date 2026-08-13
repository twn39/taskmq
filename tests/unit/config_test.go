package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/twn39/taskmq/internal/config"
)

func TestConfig_Defaults(t *testing.T) {
	cfg, err := config.NewConfig()
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	// Verify server defaults
	assert.Equal(t, ":8080", cfg.Server.Port)

	// Verify redis defaults
	assert.Equal(t, "localhost:6379", cfg.Redis.Addr)
	assert.Equal(t, 0, cfg.Redis.DB)

	// Verify logger default
	assert.Equal(t, "info", cfg.Logger.Level)
}
