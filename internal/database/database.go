package database

import (
	"github.com/twn39/gocms/internal/config"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// NewDatabase creates a new GORM database connection
func NewDatabase(cfg *config.Config) (*gorm.DB, error) {
	// Using in-memory database for simplicity, or "test.db" file
	db, err := gorm.Open(sqlite.Open(cfg.Database.DSN), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return db, nil
}
