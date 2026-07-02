package integration

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/handler"
	"github.com/twn39/taskmq/internal/logger"
	"github.com/twn39/taskmq/internal/server"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

// NewTestConfig provides a configuration for testing
func NewTestConfig() *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			Port:     ":8081",
			GRPCPort: ":50051",
		},
		Logger: config.LoggerConfig{
			Level: "error", // Quiet logs during test
		},
		Redis: config.RedisConfig{
			Addr: "localhost:6379",
		},
	}
}

func TestUserEndpoints(t *testing.T) {
	var e *echo.Echo

	// Create the app using fxtest to manage lifecycle and dependencies
	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			handler.NewUserHandler,
			server.NewServer,
		),
		fx.Populate(&e),
	)

	app.RequireStart()
	defer app.RequireStop()

	t.Run("CreateUser and GetUsers", func(t *testing.T) {
		// 1. Create a User
		userJSON := `{"name":"Test User","email":"test@example.com"}`
		req := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(userJSON))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec := httptest.NewRecorder()

		// Send request to the Echo instance directly
		e.ServeHTTP(rec, req)

		// Assertions
		assert.Equal(t, http.StatusCreated, rec.Code)
		assert.Contains(t, rec.Body.String(), "test@example.com")

		// 2. Get Users
		req = httptest.NewRequest(http.MethodGet, "/users", nil)
		rec = httptest.NewRecorder()

		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "Test User")
		assert.Contains(t, rec.Body.String(), "test@example.com")
	})

	t.Run("GetHello", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "TaskMQ")
	})
}
