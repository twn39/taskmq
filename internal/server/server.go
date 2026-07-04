package server

import (
	"context"
	"embed"
	"html/template"
	"io"
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/handler"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

//go:embed templates/*
var templateFS embed.FS

type Template struct {
	templates *template.Template
}

func (t *Template) Render(c *echo.Context, w io.Writer, name string, data any) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

// NewServer defines the Echo server
func NewServer(lc fx.Lifecycle, logger *zap.Logger, adminHandler *handler.AdminHandler, cfg *config.Config) *echo.Echo {
	e := echo.New()

	// Register templates
	t := &Template{
		templates: template.Must(template.ParseFS(templateFS, "templates/*.html")),
	}
	e.Renderer = t

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

	// Admin Dashboard Routes
	e.GET("/admin", adminHandler.GetDashboard)
	e.GET("/api/stats", adminHandler.GetStats)
	e.POST("/api/queues/:queue/pause", adminHandler.PauseQueue)
	e.POST("/api/queues/:queue/resume", adminHandler.ResumeQueue)
	e.GET("/api/queues/:queue/dlq", adminHandler.ListDLQ)
	e.POST("/api/queues/:queue/dlq/:id/retry", adminHandler.RetryDLQ)
	e.DELETE("/api/queues/:queue/dlq/:id", adminHandler.DeleteDLQ)
	e.POST("/api/queues/:queue/enqueue", adminHandler.EnqueueTest)
	e.GET("/api/queues/:queue/scheduled", adminHandler.ListScheduled)
	e.POST("/api/queues/:queue/scheduled/:id/run", adminHandler.RunScheduled)
	e.DELETE("/api/queues/:queue/scheduled/:id", adminHandler.DeleteScheduled)

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
