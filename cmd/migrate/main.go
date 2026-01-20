package main

import (
	"flag"
	"log"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/twn39/gocms/internal/config"
)

func main() {
	// Parse command line flags
	var direction string
	flag.StringVar(&direction, "direction", "up", "migration direction (up/down)")
	flag.Parse()

	// Load config to get DSN
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Setup migration
	// Note: sqlite3 driver expects dsn in specific format usually, but here we construct the URL for migrate
	// migrate URL format for sqlite3: sqlite3://path/to/database?query
	dsn := "sqlite3://" + cfg.Database.DSN

	m, err := migrate.New(
		"file://migrations",
		dsn,
	)
	if err != nil {
		log.Fatalf("Failed to create migrate instance: %v", err)
	}

	// Run migration
	if direction == "down" {
		if err := m.Down(); err != nil && err != migrate.ErrNoChange {
			log.Fatalf("Migration down failed: %v", err)
		}
		log.Println("Migration down applied successfully")
	} else {
		if err := m.Up(); err != nil && err != migrate.ErrNoChange {
			log.Fatalf("Migration up failed: %v", err)
		}
		log.Println("Migration up applied successfully")
	}
}
