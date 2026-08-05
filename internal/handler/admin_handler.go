package handler

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/redis/go-redis/v9"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	mqkeys "github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

// AdminHandler serves HTTP admin APIs using ISP-narrow client ports.
type AdminHandler struct {
	rdb       *redis.Client
	admin     mqclient.AdminClient
	cron      mqclient.CronClient
	enq       mqclient.EnqueueClient
	lifecycle *lifecycle.Lifecycle
	logger    *zap.Logger
}

// NewAdminHandler wires admin, cron, enqueue ports and optional shared lifecycle metrics.
func NewAdminHandler(rdb *redis.Client, admin mqclient.AdminClient, cron mqclient.CronClient, enq mqclient.EnqueueClient, lc *lifecycle.Lifecycle, logger *zap.Logger) *AdminHandler {
	return &AdminHandler{
		rdb:       rdb,
		admin:     admin,
		cron:      cron,
		enq:       enq,
		lifecycle: lc,
		logger:    logger,
	}
}

// GetLifecycleMetrics returns process-local lifecycle counters.
// Query format=prometheus for Prometheus exposition text (no client dependency).
func (h *AdminHandler) GetLifecycleMetrics(c *echo.Context) error {
	var snap map[string]int64
	if h.lifecycle != nil {
		snap = h.lifecycle.Metrics().Snapshot()
	} else {
		snap = map[string]int64{}
	}
	if c.QueryParam("format") == "prometheus" {
		return c.String(http.StatusOK, lifecycle.PrometheusText(snap))
	}
	return c.JSON(http.StatusOK, snap)
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

	redisKeys, err := h.rdb.Keys(ctx, mqkeys.StreamScanPattern()).Result()
	if err != nil {
		h.logger.Error("Failed to scan Redis for queues", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to scan queues"})
	}

	queuesMap := make(map[string]bool)
	for _, key := range redisKeys {
		if queueName, ok := mqkeys.ParseQueueFromStreamKey(key); ok {
			queuesMap[queueName] = true
		}
	}

	// Always ensure "default" is in the list, even if it has no active tasks currently
	queuesMap["default"] = true

	stats := []QueueStat{}
	for q := range queuesMap {
		qk := mqkeys.KeysFor(q)

		isPaused, err := h.rdb.Exists(ctx, qk.Paused()).Result()
		status := "Active"
		if err == nil && isPaused > 0 {
			status = "Paused"
		}

		activeCount, _ := h.rdb.XLen(ctx, qk.Stream()).Result()
		scheduledCount, _ := h.rdb.ZCard(ctx, qk.Delayed()).Result()
		dlqCount, _ := h.rdb.ZCard(ctx, qk.DLQ()).Result()

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

	if err := h.admin.Pause(ctx, queue); err != nil {
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

	if err := h.admin.Resume(ctx, queue); err != nil {
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

	tasks, err := h.admin.ListDeadLetters(ctx, queue, limit)
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

	if err := h.admin.RetryDeadLetter(ctx, queue, id); err != nil {
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

	if err := h.admin.DeleteDeadLetter(ctx, queue, id); err != nil {
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

	task := taskmodel.NewTask(req.Name, []byte(req.Payload), taskmodel.TaskOptions{
		Queue: req.Queue,
	})

	var err error
	if req.DelaySec > 0 {
		err = h.enq.EnqueueIn(ctx, task, time.Duration(req.DelaySec)*time.Second)
	} else {
		err = h.enq.Enqueue(ctx, task)
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

	tasks, err := h.admin.ListScheduledTasks(ctx, queue, limit)
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

	if err := h.admin.RunScheduledTask(ctx, queue, id); err != nil {
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

	if err := h.admin.DeleteScheduledTask(ctx, queue, id); err != nil {
		h.logger.Error("Failed to delete scheduled task", zap.String("queue", queue), zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Task '%s' successfully deleted from scheduled tasks", id)})
}

func (h *AdminHandler) ListCron(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	if queue == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue parameter"})
	}

	jobs, err := h.cron.ListCronJobs(ctx, queue)
	if err != nil {
		h.logger.Error("Failed to list cron jobs", zap.String("queue", queue), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if jobs == nil {
		return c.JSON(http.StatusOK, []interface{}{})
	}
	return c.JSON(http.StatusOK, jobs)
}

func (h *AdminHandler) RunCron(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	jobName := c.Param("job_name")
	if queue == "" || jobName == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue or job_name parameter"})
	}

	if err := h.cron.RunCronJob(ctx, queue, jobName); err != nil {
		h.logger.Error("Failed to trigger cron job", zap.String("queue", queue), zap.String("jobName", jobName), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Cron job '%s' successfully triggered to run immediately", jobName)})
}

func (h *AdminHandler) DeleteCron(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	jobName := c.Param("job_name")
	if queue == "" || jobName == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue or job_name parameter"})
	}

	if err := h.cron.DeleteCronJob(ctx, queue, jobName); err != nil {
		h.logger.Error("Failed to delete cron job", zap.String("queue", queue), zap.String("jobName", jobName), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Cron job '%s' successfully deleted", jobName)})
}

func (h *AdminHandler) RetryAllDLQ(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	if queue == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue parameter"})
	}

	count, err := h.admin.RetryAllDeadLetters(ctx, queue)
	if err != nil {
		h.logger.Error("Failed to retry all DLQ tasks", zap.String("queue", queue), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message": fmt.Sprintf("Successfully re-enqueued %d tasks from DLQ", count),
		"count":   count,
	})
}

func (h *AdminHandler) PurgeAllDLQ(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	if queue == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue parameter"})
	}

	count, err := h.admin.PurgeAllDeadLetters(ctx, queue)
	if err != nil {
		h.logger.Error("Failed to purge all DLQ tasks", zap.String("queue", queue), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message": fmt.Sprintf("Successfully purged %d tasks from DLQ", count),
		"count":   count,
	})
}

func (h *AdminHandler) ListActive(c *echo.Context) error {
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

	tasks, err := h.admin.ListActiveTasks(ctx, queue, limit)
	if err != nil {
		h.logger.Error("Failed to list active tasks", zap.String("queue", queue), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if tasks == nil {
		return c.JSON(http.StatusOK, []interface{}{})
	}
	return c.JSON(http.StatusOK, tasks)
}

func (h *AdminHandler) DeleteActive(c *echo.Context) error {
	ctx := c.Request().Context()
	queue := c.Param("queue")
	id := c.Param("id")
	if queue == "" || id == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing queue or id parameter"})
	}

	if err := h.admin.DeleteActiveTask(ctx, queue, id); err != nil {
		h.logger.Error("Failed to delete active task", zap.String("queue", queue), zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": fmt.Sprintf("Active task '%s' successfully deleted/cancelled", id)})
}
