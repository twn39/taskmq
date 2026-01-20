package handler

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// User represents a simple user model
type User struct {
	gorm.Model
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserHandler handles user related HTTP requests
type UserHandler struct {
	db     *gorm.DB
	logger *zap.Logger
}

// NewUserHandler creates a new UserHandler
func NewUserHandler(db *gorm.DB, logger *zap.Logger) *UserHandler {
	// Auto migrate the User model for simplicity
	err := db.AutoMigrate(&User{})
	if err != nil {
		logger.Error("Failed to auto migrate User model", zap.Error(err))
	}

	return &UserHandler{
		db:     db, // Start with a default connection
		logger: logger,
	}
}

// GetHello returns a simple hello message
func (h *UserHandler) GetHello(c echo.Context) error {
	h.logger.Info("Hello endpoint called")
	return c.JSON(http.StatusOK, map[string]string{
		"message": "Hello from GoCMS!",
	})
}

// CreateUser creates a new user
func (h *UserHandler) CreateUser(c echo.Context) error {
	var user User
	if err := c.Bind(&user); err != nil {
		h.logger.Error("Failed to bind user", zap.Error(err))
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	if result := h.db.Create(&user); result.Error != nil {
		h.logger.Error("Failed to create user", zap.Error(result.Error))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to create user"})
	}

	h.logger.Info("User created", zap.String("email", user.Email))
	return c.JSON(http.StatusCreated, user)
}

// GetUsers returns all users
func (h *UserHandler) GetUsers(c echo.Context) error {
	var users []User
	if result := h.db.Find(&users); result.Error != nil {
		h.logger.Error("Failed to fetch users", zap.Error(result.Error))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to fetch users"})
	}
	return c.JSON(http.StatusOK, users)
}
