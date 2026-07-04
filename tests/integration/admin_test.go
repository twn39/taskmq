package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/handler"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/server"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestAdminDashboard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var e *echo.Echo
	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			handler.NewAdminHandler,
			server.NewServer,
		),
		fx.Populate(&e, &rdb, &client),
	)

	app.RequireStart()
	defer app.RequireStop()

	t.Run("GET /admin HTML Rendering", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin", nil)
		rec := httptest.NewRecorder()

		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "TaskMQ Dashboard")
		assert.Contains(t, rec.Body.String(), "Discovered Queues")
	})

	t.Run("GET /api/stats JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		rec := httptest.NewRecorder()

		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var stats []handler.QueueStat
		err := json.Unmarshal(rec.Body.Bytes(), &stats)
		assert.NoError(t, err)

		// Assert default queue is present
		found := false
		for _, q := range stats {
			if q.Name == "default" {
				found = true
				assert.Equal(t, "Active", q.Status)
			}
		}
		assert.True(t, found, "default queue should be found in stats")
	})

	t.Run("POST Pause and Resume Queue", func(t *testing.T) {
		// Pause
		req := httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/pause", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "paused successfully")

		// Verify stats status changes to Paused
		reqStats := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		recStats := httptest.NewRecorder()
		e.ServeHTTP(recStats, reqStats)
		var stats []handler.QueueStat
		_ = json.Unmarshal(recStats.Body.Bytes(), &stats)
		for _, q := range stats {
			if q.Name == "test-admin-queue" {
				assert.Equal(t, "Paused", q.Status)
			}
		}

		// Resume
		req = httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/resume", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "resumed successfully")

		// Verify stats status returns to Active
		recStats = httptest.NewRecorder()
		e.ServeHTTP(recStats, reqStats)
		_ = json.Unmarshal(recStats.Body.Bytes(), &stats)
		for _, q := range stats {
			if q.Name == "test-admin-queue" {
				assert.Equal(t, "Active", q.Status)
			}
		}
	})

	t.Run("Enqueue Test Task via API", func(t *testing.T) {
		reqBody, _ := json.Marshal(map[string]interface{}{
			"queue":     "test-admin-queue",
			"name":      "task:test-enqueue",
			"payload":   `{"test":true}`,
			"delay_sec": 0,
		})
		req := httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/enqueue", bytes.NewReader(reqBody))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec := httptest.NewRecorder()

		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "Successfully enqueued task")
		assert.Contains(t, rec.Body.String(), "task_id")
	})

	t.Run("DLQ Management APIs", func(t *testing.T) {
		// Mock a DLQ task in Redis
		queueName := "test-admin-queue"
		dlqKey := taskmq.DLQKey(queueName)
		dlqIndexKey := taskmq.DLQIndexKey(queueName)

		deadTask := taskmq.NewTask("task:failed-test", []byte("bad-data"), taskmq.TaskOptions{Queue: queueName})
		deadTask.ID = "test-dead-id"
		deadTask.Retry = 3
		deadTask.LastError = "some critical failure"
		serializedDead, _ := json.Marshal(deadTask)

		rdb.Del(ctx, dlqKey, dlqIndexKey)
		defer rdb.Del(ctx, dlqKey, dlqIndexKey)

		err := rdb.ZAdd(ctx, dlqKey, goredis.Z{Score: float64(time.Now().UnixMilli()), Member: serializedDead}).Err()
		assert.NoError(t, err)
		err = rdb.HSet(ctx, dlqIndexKey, deadTask.ID, serializedDead).Err()
		assert.NoError(t, err)

		// 1. List DLQ
		req := httptest.NewRequest(http.MethodGet, "/api/queues/test-admin-queue/dlq", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "test-dead-id")
		assert.Contains(t, rec.Body.String(), "some critical failure")

		// 2. Retry DLQ Task
		req = httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/dlq/test-dead-id/retry", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "re-enqueued for retry")

		// Re-add to test deletion
		err = rdb.ZAdd(ctx, dlqKey, goredis.Z{Score: float64(time.Now().UnixMilli()), Member: serializedDead}).Err()
		assert.NoError(t, err)
		err = rdb.HSet(ctx, dlqIndexKey, deadTask.ID, serializedDead).Err()
		assert.NoError(t, err)

		// 3. Delete DLQ Task
		req = httptest.NewRequest(http.MethodDelete, "/api/queues/test-admin-queue/dlq/test-dead-id", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "successfully deleted from DLQ")
	})

	t.Run("DLQ Bulk Operations Management APIs", func(t *testing.T) {
		queueName := "test-admin-queue"
		dlqKey := taskmq.DLQKey(queueName)
		dlqIndexKey := taskmq.DLQIndexKey(queueName)

		rdb.Del(ctx, dlqKey, dlqIndexKey)
		defer rdb.Del(ctx, dlqKey, dlqIndexKey)

		// Create 5 dead tasks
		for i := 0; i < 5; i++ {
			deadTask := taskmq.NewTask("task:bulk-failed", []byte("bad-data"), taskmq.TaskOptions{Queue: queueName})
			deadTask.ID = fmt.Sprintf("dead-id-%d", i)
			deadTask.Retry = 3
			deadTask.LastError = "bulk error"
			serialized, _ := json.Marshal(deadTask)

			rdb.ZAdd(ctx, dlqKey, goredis.Z{Score: float64(time.Now().UnixMilli()), Member: serialized})
			rdb.HSet(ctx, dlqIndexKey, deadTask.ID, serialized)
		}

		// 1. Verify 5 tasks in DLQ
		req := httptest.NewRequest(http.MethodGet, "/api/queues/test-admin-queue/dlq", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var tasks []*taskmq.Task
		json.Unmarshal(rec.Body.Bytes(), &tasks)
		assert.Len(t, tasks, 5)

		// 2. Retry All DLQ tasks
		req = httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/dlq/retry", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "Successfully re-enqueued 5 tasks")

		// 3. Verify DLQ is empty after retry all
		req = httptest.NewRequest(http.MethodGet, "/api/queues/test-admin-queue/dlq", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		json.Unmarshal(rec.Body.Bytes(), &tasks)
		assert.Len(t, tasks, 0)

		// 4. Create 3 more dead tasks to test Purge
		for i := 0; i < 3; i++ {
			deadTask := taskmq.NewTask("task:bulk-failed", []byte("bad-data"), taskmq.TaskOptions{Queue: queueName})
			deadTask.ID = fmt.Sprintf("dead-id-purge-%d", i)
			serialized, _ := json.Marshal(deadTask)
			rdb.ZAdd(ctx, dlqKey, goredis.Z{Score: float64(time.Now().UnixMilli()), Member: serialized})
			rdb.HSet(ctx, dlqIndexKey, deadTask.ID, serialized)
		}

		// 5. Purge DLQ
		req = httptest.NewRequest(http.MethodDelete, "/api/queues/test-admin-queue/dlq", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "Successfully purged 3 tasks")

		// 6. Verify DLQ is empty
		req = httptest.NewRequest(http.MethodGet, "/api/queues/test-admin-queue/dlq", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		json.Unmarshal(rec.Body.Bytes(), &tasks)
		assert.Len(t, tasks, 0)
	})

	t.Run("Scheduled Tasks Management APIs", func(t *testing.T) {
		queueName := "test-admin-queue"
		delayedKey := taskmq.DelayedKey(queueName)

		rdb.Del(ctx, delayedKey)
		defer rdb.Del(ctx, delayedKey)

		// Enqueue a delayed task via the API
		reqBody, _ := json.Marshal(map[string]interface{}{
			"queue":     queueName,
			"name":      "task:delayed-test",
			"payload":   `{"test":true}`,
			"delay_sec": 60,
		})
		req := httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/enqueue", bytes.NewReader(reqBody))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]interface{}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		taskID := resp["task_id"].(string)

		// 1. List Scheduled Tasks
		req = httptest.NewRequest(http.MethodGet, "/api/queues/test-admin-queue/scheduled", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), taskID)
		assert.Contains(t, rec.Body.String(), "task:delayed-test")

		// 2. Promote / Run Scheduled Task Immediately
		req = httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/scheduled/"+taskID+"/run", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "promoted to run immediately")

		// Enqueue another delayed task to test deletion
		req = httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/enqueue", bytes.NewReader(reqBody))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		taskID2 := resp["task_id"].(string)

		// 3. Delete Scheduled Task
		req = httptest.NewRequest(http.MethodDelete, "/api/queues/test-admin-queue/scheduled/"+taskID2, nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "deleted from scheduled tasks")
	})

	t.Run("Cron Schedules Management APIs", func(t *testing.T) {
		queueName := "test-admin-queue"
		configsKey := taskmq.CronConfigsKey(queueName)
		delayedKey := taskmq.DelayedKey(queueName)

		rdb.Del(ctx, configsKey, delayedKey)
		defer rdb.Del(ctx, configsKey, delayedKey)

		// Register a cron job
		jobName := "test-cron-job"
		cronSpec := "*/5 * * * * *" // every 5 seconds
		cronTask := taskmq.NewTask("task:cron-test", []byte("cron-payload"), taskmq.TaskOptions{Queue: queueName})
		
		err := client.RegisterCron(ctx, jobName, cronSpec, cronTask)
		assert.NoError(t, err)

		// 1. List Cron Jobs
		req := httptest.NewRequest(http.MethodGet, "/api/queues/test-admin-queue/cron", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), jobName)
		assert.Contains(t, rec.Body.String(), "Y3Jvbi1wYXlsb2Fk")

		// 2. Trigger Cron Job Immediately
		req = httptest.NewRequest(http.MethodPost, "/api/queues/test-admin-queue/cron/"+jobName+"/run", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "successfully triggered to run immediately")

		// 3. Delete Cron Job
		req = httptest.NewRequest(http.MethodDelete, "/api/queues/test-admin-queue/cron/"+jobName, nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "successfully deleted")

		// Verify deletion
		req = httptest.NewRequest(http.MethodGet, "/api/queues/test-admin-queue/cron", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), jobName)
	})
}
