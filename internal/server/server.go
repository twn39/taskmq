package server

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/twn39/gocms/internal/config"
	"github.com/twn39/gocms/internal/handler"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// NewServer defines the Echo server
func NewServer(lc fx.Lifecycle, logger *zap.Logger, userHandler *handler.UserHandler, cfg *config.Config) *echo.Echo {
	e := echo.New()

	// Middleware
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogURI:     true,
		LogStatus:  true,
		LogMethod:  true,
		LogLatency: true,
		LogValuesFunc: func(c *echo.Context, v middleware.RequestLoggerValues) error {
			logger.Info("request",
				zap.String("URI", v.URI),
				zap.Int("status", v.Status),
				zap.String("method", v.Method),
				zap.Duration("latency", v.Latency),
			)
			return nil
		},
	}))
	e.Use(middleware.Recover())

	// Routes
	e.GET("/", userHandler.GetHello)
	e.POST("/users", userHandler.CreateUser)
	e.GET("/users", userHandler.GetUsers)

	// Lifecycle hooks
	serverCtx, cancelServer := context.WithCancel(context.Background())

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting HTTP server", zap.String("port", cfg.Server.Port))
			sc := echo.StartConfig{
				Address: cfg.Server.Port,
			}
			// Run server in a goroutine so it doesn't block
			go func() {
				if err := sc.Start(serverCtx, e); err != nil && err != http.ErrServerClosed {
					logger.Fatal("Shutting down the server", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping HTTP server")
			cancelServer()
			return nil
		},
	})

	return e
}
