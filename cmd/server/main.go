package main

import (
	"github.com/labstack/echo/v4"
	"github.com/twn39/gocms/internal/config"
	"github.com/twn39/gocms/internal/database"
	"github.com/twn39/gocms/internal/handler"
	"github.com/twn39/gocms/internal/logger"
	"github.com/twn39/gocms/internal/server"
	"go.uber.org/fx"
)

func main() {
	fx.New(
		// Provide all the constructors
		fx.Provide(
			config.NewConfig,
			logger.NewLogger,
			database.NewDatabase,
			handler.NewUserHandler,
			server.NewServer,
		),
		// Invoke the server to start it
		fx.Invoke(func(*echo.Echo) {}),
	).Run()
}
