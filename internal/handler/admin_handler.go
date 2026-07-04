package handler

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/zap"
)

type AdminHandler struct {
	rdb    *redis.Client
	client taskmq.Client
	logger *zap.Logger
}

func NewAdminHandler(rdb *redis.Client, client taskmq.Client, logger *zap.Logger) *AdminHandler {
	return &AdminHandler{
		rdb:    rdb,
		client: client,
		logger: logger,
	}
}

func (h *AdminHandler) GetDashboard(c *echo.Context) error {
	return c.Render(http.StatusOK, "index.html", map[string]interface{}{
		"Title": "TaskMQ Dashboard",
	})
}

type QueueStat struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Active     int64  `json:"active"`
	Scheduled  int64  `json:"scheduled"`
	DeadLetter int64  `json:"dead_letter"`
}

func (h *AdminHandler) GetStats(c *echo.Context) error {
	ctx := c.Request().Context()

	keys, err := h.rdb.Keys(ctx, "taskmq:{*}:queue").Result()
	if err != nil {
		h.logger.Error("Failed to scan Redis for queues", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to scan queues"})
	}

	queuesMap := make(map[string]bool)
	for _, key := range keys {
		start := len("taskmq:{")
		end := strings.Index(key, "}:queue")
		if start < len(key) && end > start {
			queueName := key[start:end]
			queuesMap[queueName] = true
		}
	}

	// Always ensure "default" is in the list, even if it has no active tasks currently
	queuesMap["default"] = true

	stats := []QueueStat{}
	for q := range queuesMap {
		pausedKey := taskmq.PausedKey(q)
		streamKey := taskmq.StreamKey(q)
		delayedKey := taskmq.DelayedKey(q)
		dlqKey := taskmq.DLQKey(q)

		isPaused, err := h.rdb.Exists(ctx, pausedKey).Result()
		status := "Active"
		if err == nil && isPaused > 0 {
			status = "Paused"
		}

		activeCount, _ := h.rdb.XLen(ctx, streamKey).Result()
		scheduledCount, _ := h.rdb.ZCard(ctx, delayedKey).Result()
		dlqCount, _ := h.rdb.ZCard(ctx, dlqKey).Result()

		stats = append(stats, QueueStat{
			Name:       q,
			Status:     status,
			Active:     activeCount,
			Scheduled:  scheduledCount,
			DeadLetter: dlqCount,
		})
	}

	return c.JSON(http.StatusOK, stats)
}

func (h *AdminHandler) PauseQueue(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	if queue == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue parameter"})
	}

	if err := h.client.Pause(ctx, queue); err != nil {
		h.logger.Error("Failed to pause queue", zap.String("queue", queue), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Queue '%s' paused successfully", queue)})
}

func (h *AdminHandler) ResumeQueue(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	if queue == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue parameter"})
	}

	if err := h.client.Resume(ctx, queue); err != nil {
		h.logger.Error("Failed to resume queue", zap.String("queue", queue), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Queue '%s' resumed successfully", queue)})
}

func (h *AdminHandler) ListDLQ(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	if queue == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue parameter"})
	}

	limitStr := c.QueryParam("limit")
	limit := 50
	if limitStr != "" {
		if lim, err := strconv.Atoi(limitStr); err == nil && lim > 0 {
			limit = lim
		}
	}

	tasks, err := h.client.ListDeadLetters(ctx, queue, limit)
	if err != nil {
		h.logger.Error("Failed to list dead letters", zap.String("queue", queue), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if tasks == nil {
		return c.JSON(http.StatusOK, []interface{}{})
	}
	return c.JSON(http.StatusOK, tasks)
}

func (h *AdminHandler) RetryDLQ(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	id := c.Param("id")
	if queue == "" || id == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue or id parameter"})
	}

	if err := h.client.RetryDeadLetter(ctx, queue, id); err != nil {
		h.logger.Error("Failed to retry dead letter", zap.String("queue", queue), zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Task '%s' successfully re-enqueued for retry", id)})
}

func (h *AdminHandler) DeleteDLQ(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	id := c.Param("id")
	if queue == "" || id == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue or id parameter"})
	}

	if err := h.client.DeleteDeadLetter(ctx, queue, id); err != nil {
		h.logger.Error("Failed to delete dead letter", zap.String("queue", queue), zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Task '%s' successfully deleted from DLQ", id)})
}

type EnqueueTestRequest struct {
	Queue    string `json:"queue"`
	Name     string `json:"name"`
	Payload  string `json:"payload"`
	DelaySec int    `json:"delay_sec"`
}

func (h *AdminHandler) EnqueueTest(c *echo.Context) error {
	ctx := c.Request().Context()
	var req EnqueueTestRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
	}

	if req.Queue == "" {
		req.Queue = "default"
	}
	if req.Name == "" {
		req.Name = "task:test"
	}

	task := taskmq.NewTask(req.Name, []byte(req.Payload), taskmq.TaskOptions{
		Queue: req.Queue,
	})

	var err error
	if req.DelaySec > 0 {
		err = h.client.EnqueueIn(ctx, task, time.Duration(req.DelaySec)*time.Second)
	} else {
		err = h.client.Enqueue(ctx, task)
	}

	if err != nil {
		h.logger.Error("Failed to enqueue test task", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message": fmt.Sprintf("Successfully enqueued task '%s' to queue '%s'", req.Name, req.Queue),
		"task_id": task.ID,
	})
}

func (h *AdminHandler) ListScheduled(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	if queue == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue parameter"})
	}

	limitStr := c.QueryParam("limit")
	limit := 50
	if limitStr != "" {
		if lim, err := strconv.Atoi(limitStr); err == nil && lim > 0 {
			limit = lim
		}
	}

	tasks, err := h.client.ListScheduledTasks(ctx, queue, limit)
	if err != nil {
		h.logger.Error("Failed to list scheduled tasks", zap.String("queue", queue), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if tasks == nil {
		return c.JSON(http.StatusOK, []interface{}{})
	}
	return c.JSON(http.StatusOK, tasks)
}

func (h *AdminHandler) RunScheduled(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	id := c.Param("id")
	if queue == "" || id == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue or id parameter"})
	}

	if err := h.client.RunScheduledTask(ctx, queue, id); err != nil {
		h.logger.Error("Failed to run scheduled task", zap.String("queue", queue), zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Task '%s' successfully promoted to run immediately", id)})
}

func (h *AdminHandler) DeleteScheduled(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	id := c.Param("id")
	if queue == "" || id == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue or id parameter"})
	}

	if err := h.client.DeleteScheduledTask(ctx, queue, id); err != nil {
		h.logger.Error("Failed to delete scheduled task", zap.String("queue", queue), zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Task '%s' successfully deleted from scheduled tasks", id)})
}
