package server

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/twn39/gocms/internal/config"
	"github.com/twn39/gocms/internal/handler"
	"github.com/twn39/gocms/internal/templates"
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
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
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

	// Static files
	e.Static("/static", "static")

	// Templates
	renderer, err := templates.NewTemplateRenderer(cfg.Server.TemplateGlob, cfg.Server.ManifestPath)
	if err != nil {
		logger.Fatal("Failed to parse templates", zap.Error(err))
	}
	e.Renderer = renderer

	// Routes
	e.GET("/", userHandler.GetHello)
	e.POST("/users", userHandler.CreateUser)
	e.GET("/users", userHandler.GetUsers)

	// Lifecycle hooks
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting HTTP server", zap.String("port", cfg.Server.Port))
			// Run server in a goroutine so it doesn't block
			go func() {
				if err := e.Start(cfg.Server.Port); err != nil && err != http.ErrServerClosed {
					logger.Fatal("Shutting down the server", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping HTTP server")
			return e.Shutdown(ctx)
		},
	})

	return e
}
