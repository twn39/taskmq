package main

import (
	"github.com/labstack/echo/v5"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/handler"
	"github.com/twn39/taskmq/internal/logger"
	"github.com/twn39/taskmq/internal/server"
	"go.uber.org/fx"
)

func main() {
	fx.New(
		// Provide all the constructors
		fx.Provide(
			config.NewConfig,
			logger.NewLogger,
			handler.NewUserHandler,
			server.NewServer,
		),
		// Invoke the server to start it
		fx.Invoke(func(*echo.Echo) {}),
	).Run()
}
