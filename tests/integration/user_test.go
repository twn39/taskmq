package integration

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/gocms/internal/config"
	"github.com/twn39/gocms/internal/handler"
	"github.com/twn39/gocms/internal/logger"
	"github.com/twn39/gocms/internal/server"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// NewTestDatabase creates a new in-memory GORM database connection for testing
func NewTestDatabase() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return db, nil
}

// NewTestConfig provides a configuration for testing
func NewTestConfig() *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			Port: ":8081", // Use different port for testing if we were binding, though hardcoded in server currently? No I made it use config.
		},
		Database: config.DatabaseConfig{
			DSN: ":memory:",
		},
		Logger: config.LoggerConfig{
			Level: "error", // Quiet logs during test
		},
	}
}

func TestUserEndpoints(t *testing.T) {
	var e *echo.Echo
	var db *gorm.DB

	// Create the app using fxtest to manage lifecycle and dependencies
	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			// Override the real database with our test database
			NewTestDatabase,
			handler.NewUserHandler,
			server.NewServer,
		),
		// Use fx.Decorate or fx.Replace to swap implementations if needed.
		// Since NewTestDatabase returns (*gorm.DB, error), it matches the signature of database.NewDatabase.
		// However, to be safe and explicit, let's just Provide it and NOT provide the original.
		// NOTE: In the main.go we provided database.NewDatabase. Here we provide NewTestDatabase instead.

		fx.Populate(&e, &db),
	)

	// Since NewTestDatabase signature matches, we just need to make sure we are not importing the original module that provides the DB
	// or we can use fx.Replace if we were using a Module bundle.
	// In main.go we listed providers manually. Here we can just list our test providers.

	// Start the app (this runs OnStart hooks)
	// Note: server.NewServer starts the http server in a goroutine on :8080.
	// For testing, we might not want to bind to a real port, but our NewServer hardcodes it.
	// However, we can still use httptest with e.ServeHTTP without using the real network call if we want,
	// BUT e.Start is running.
	// To avoid port conflict or needing to wait, we can assume it starts fine or ignore the network listener for these tests
	// by directly invoking handler methods or using `e.ServeHTTP`.
	// Since we used `go func() { e.Start(...) }`, it shouldn't block.

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
		assert.Contains(t, rec.Body.String(), "Hello from GoCMS!")
	})
}

// NOTE: Because server.NewServer hardcodes the port connection, running this test
// might conflict if something is already on 8080.
// A better approach in `server.go` would be to accept a Config struct or allow disabling the listener.
// For now, this assumes port 8080 is free.
