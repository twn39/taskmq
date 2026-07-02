package handler

import (
	"net/http"
	"sync"

	"github.com/labstack/echo/v5"
	"go.uber.org/zap"
)

// User represents a simple user model
type User struct {
	ID    uint   `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserHandler handles user related HTTP requests
type UserHandler struct {
	logger *zap.Logger
	mu     sync.RWMutex
	users  []User
	nextID uint
}

// NewUserHandler creates a new UserHandler
func NewUserHandler(logger *zap.Logger) *UserHandler {
	return &UserHandler{
		logger: logger,
		nextID: 1,
	}
}

// GetHello returns a simple hello message
func (h *UserHandler) GetHello(c *echo.Context) error {
	h.logger.Info("Hello endpoint called")
	return c.JSON(http.StatusOK, map[string]interface{}{
		"Title":   "GoCMS",
		"Message": "Welcome to the GoCMS API!",
	})
}

// CreateUser creates a new user
func (h *UserHandler) CreateUser(c *echo.Context) error {
	var user User
	if err := c.Bind(&user); err != nil {
		h.logger.Error("Failed to bind user", zap.Error(err))
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	h.mu.Lock()
	user.ID = h.nextID
	h.nextID++
	h.users = append(h.users, user)
	h.mu.Unlock()

	h.logger.Info("User created", zap.String("email", user.Email))
	return c.JSON(http.StatusCreated, user)
}

// GetUsers returns all users
func (h *UserHandler) GetUsers(c *echo.Context) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Return empty slice instead of nil for clean JSON output
	if h.users == nil {
		return c.JSON(http.StatusOK, []User{})
	}
	return c.JSON(http.StatusOK, h.users)
}
