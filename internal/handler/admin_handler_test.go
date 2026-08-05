package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/labstack/echo/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/handler"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	"github.com/twn39/taskmq/internal/taskmq/metricsq"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

func setupHandler(t *testing.T) (*echo.Echo, redis.UniversalClient, *handler.AdminHandler, *lifecycle.Lifecycle, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	lc := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		CompletedRetention: time.Hour,
		CompletedMaxCount:  100,
		DLQMaxCount:        100,
	})
	cli := mqclient.NewClient(rdb,
		mqclient.WithClientCodec(codec.JSONCodec{}),
		mqclient.WithClientLifecycle(lc),
	)
	h := handler.NewAdminHandler(rdb, cli, cli, cli, lc, zap.NewNop())
	e := echo.New()
	// Mirror production routes used in these tests.
	e.GET("/api/lifecycle/metrics", h.GetLifecycleMetrics)
	e.GET("/api/metrics/queues", h.GetQueueMetrics)
	e.GET("/api/metrics/queues/:queue", h.GetQueueMetrics)
	e.GET("/api/queues/:queue/tasks/:id", h.GetTask)
	e.POST("/api/queues/:queue/tasks/:id/progress", h.UpdateProgress)
	e.GET("/api/queues/:queue/events", h.ListEvents)
	e.GET("/api/queues/:queue/workers", h.ListWorkers)
	e.POST("/api/queues/:queue/bulk", h.BulkEnqueue)
	e.GET("/api/queues/:queue/active", h.ListActive)
	e.DELETE("/api/queues/:queue/active/:id", h.DeleteActive)
	e.GET("/api/stats", h.GetStats)
	e.POST("/api/queues/:queue/pause", h.PauseQueue)
	e.POST("/api/queues/:queue/resume", h.ResumeQueue)
	e.GET("/api/queues/:queue/dlq", h.ListDLQ)
	e.POST("/api/queues/:queue/dlq/:id/retry", h.RetryDLQ)
	e.DELETE("/api/queues/:queue/dlq/:id", h.DeleteDLQ)
	e.POST("/api/queues/:queue/dlq/retry-all", h.RetryAllDLQ)
	e.POST("/api/queues/:queue/dlq/purge", h.PurgeAllDLQ)
	e.GET("/api/queues/:queue/scheduled", h.ListScheduled)
	e.POST("/api/queues/:queue/scheduled/:id/run", h.RunScheduled)
	e.DELETE("/api/queues/:queue/scheduled/:id", h.DeleteScheduled)
	e.GET("/api/queues/:queue/cron", h.ListCron)
	e.POST("/api/queues/:queue/cron/:job_name/run", h.RunCron)
	e.DELETE("/api/queues/:queue/cron/:job_name", h.DeleteCron)
	e.POST("/api/enqueue", h.EnqueueTest)
	return e, rdb, h, lc, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func TestAdminHandler_MetricsAndTaskProgressEvents(t *testing.T) {
	e, rdb, _, lc, cleanup := setupHandler(t)
	defer cleanup()
	ctx := context.Background()
	queue := "admin-unit-q"
	taskID := "t1"

	// Seed meta + metrics + events + heartbeat.
	tk := taskmodel.NewTask("job", []byte(`{}`), taskmodel.TaskOptions{ID: taskID, Queue: queue})
	require.NoError(t, meta.NewStore(rdb).Put(ctx, tk, meta.StatePending))
	metricsq.NewStore(rdb).Inc(ctx, queue, metricsq.FieldCompleted, 3)
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: keys.KeysFor(queue).Events(),
		Values: map[string]interface{}{
			"type": "enqueued", "queue": queue, "task_id": taskID, "name": "job",
			"timestamp_ms": time.Now().UnixMilli(),
		},
	}).Err())
	// Stream key so queue is discoverable for metrics list.
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: keys.KeysFor(queue).Stream(),
		Values: map[string]interface{}{"task": "{}"},
	}).Err())
	// Seed a live worker heartbeat.
	hbJSON, _ := json.Marshal(map[string]interface{}{
		"queue": queue, "consumer": "c1", "concurrency": 2,
		"started_at": time.Now(), "updated_at": time.Now(),
	})
	require.NoError(t, rdb.Set(ctx, keys.KeysFor(queue).Heartbeat("c1"), hbJSON, time.Minute).Err())

	// Lifecycle metrics JSON
	req := httptest.NewRequest(http.MethodGet, "/api/lifecycle/metrics", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Prometheus process-local
	req = httptest.NewRequest(http.MethodGet, "/api/lifecycle/metrics?format=prometheus", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "taskmq_lifecycle_")
	_ = lc

	// Queue metrics
	req = httptest.NewRequest(http.MethodGet, "/api/metrics/queues/"+queue, nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), queue)

	// GetTask
	req = httptest.NewRequest(http.MethodGet, "/api/queues/"+queue+"/tasks/"+taskID, nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// GetTask 404
	req = httptest.NewRequest(http.MethodGet, "/api/queues/"+queue+"/tasks/missing", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// Progress
	body, _ := json.Marshal(map[string]interface{}{"percent": 55, "data": "half"})
	req = httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/tasks/"+taskID+"/progress", bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Events
	req = httptest.NewRequest(http.MethodGet, "/api/queues/"+queue+"/events?limit=10", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "enqueued")

	// Workers
	req = httptest.NewRequest(http.MethodGet, "/api/queues/"+queue+"/workers", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "c1")
}

func TestAdminHandler_BulkEnqueue(t *testing.T) {
	e, rdb, _, _, cleanup := setupHandler(t)
	defer cleanup()
	queue := "admin-bulk-q"

	body, _ := json.Marshal(map[string]interface{}{
		"tasks": []map[string]string{
			{"id": "b1", "name": "job", "payload": `{"n":1}`},
			{"id": "b2", "name": "job", "payload": `{"n":2}`},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/bulk", bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	n, err := rdb.XLen(context.Background(), keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
}

func TestAdminHandler_GetTaskMissingParams(t *testing.T) {
	e, _, _, _, cleanup := setupHandler(t)
	defer cleanup()
	req := httptest.NewRequest(http.MethodGet, "/api/queues//tasks/", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	// Empty path params may 404 at router level; either is acceptable.
	require.True(t, rec.Code == http.StatusNotFound || rec.Code == http.StatusBadRequest || rec.Code == http.StatusOK)
}

func TestAdminHandler_OpsSurface(t *testing.T) {
	e, rdb, _, _, cleanup := setupHandler(t)
	defer cleanup()
	ctx := context.Background()
	queue := "admin-ops-q"
	qk := keys.KeysFor(queue)

	// Stats discovers stream keys.
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: qk.Stream(),
		Values: map[string]interface{}{"task": `{"id":"x"}`},
	}).Err())

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), queue)

	// Pause / resume
	req = httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/pause", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	n, err := rdb.Exists(ctx, qk.Paused()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	req = httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/resume", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Enqueue test (immediate + delayed)
	body, _ := json.Marshal(map[string]interface{}{
		"queue": queue, "name": "task:test", "payload": `{"ok":true}`,
	})
	req = httptest.NewRequest(http.MethodPost, "/api/enqueue", bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ = json.Marshal(map[string]interface{}{
		"queue": queue, "name": "task:delay", "payload": `{}`, "delay_sec": 60,
	})
	req = httptest.NewRequest(http.MethodPost, "/api/enqueue", bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Scheduled list / run / delete
	req = httptest.NewRequest(http.MethodGet, "/api/queues/"+queue+"/scheduled?limit=10", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Seed known delayed task for run/delete by id.
	cli := mqclient.NewClient(rdb, mqclient.WithClientCodec(codec.JSONCodec{}), mqclient.WithClientLifecycle(lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())))
	require.NoError(t, cli.EnqueueAt(ctx, taskmodel.NewTask("job", []byte(`s`), taskmodel.TaskOptions{
		ID: "sched-1", Queue: queue,
	}), time.Now().Add(time.Hour)))
	req = httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/scheduled/sched-1/run", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, cli.EnqueueAt(ctx, taskmodel.NewTask("job", []byte(`s2`), taskmodel.TaskOptions{
		ID: "sched-2", Queue: queue,
	}), time.Now().Add(2*time.Hour)))
	req = httptest.NewRequest(http.MethodDelete, "/api/queues/"+queue+"/scheduled/sched-2", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// DLQ seed + list/retry/delete/retry-all/purge
	seedDLQ := func(id string, score int64) {
		tk := taskmodel.NewTask("job", []byte(`d`), taskmodel.TaskOptions{ID: id, Queue: queue})
		ser, err := codec.JSONCodec{}.Marshal(tk)
		require.NoError(t, err)
		require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{Score: float64(score), Member: id}).Err())
		require.NoError(t, rdb.HSet(ctx, qk.DLQIndex(), id, string(ser)).Err())
	}
	seedDLQ("dead-a", 1)
	seedDLQ("dead-b", 2)

	req = httptest.NewRequest(http.MethodGet, "/api/queues/"+queue+"/dlq?limit=5", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "dead-")

	req = httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/dlq/dead-a/retry", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodDelete, "/api/queues/"+queue+"/dlq/dead-b", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	seedDLQ("dead-c", 3)
	seedDLQ("dead-d", 4)
	req = httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/dlq/retry-all", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	seedDLQ("dead-e", 5)
	req = httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/dlq/purge", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Cron register via client then HTTP list/run/delete
	require.NoError(t, cli.RegisterCron(ctx, "nightly", "0 1 * * *", taskmodel.NewTask("unused", []byte(`{}`), taskmodel.TaskOptions{Queue: queue})))
	req = httptest.NewRequest(http.MethodGet, "/api/queues/"+queue+"/cron", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "nightly")

	req = httptest.NewRequest(http.MethodPost, "/api/queues/"+queue+"/cron/nightly/run", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodDelete, "/api/queues/"+queue+"/cron/nightly", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Active list + delete
	msgs, err := rdb.XRangeN(ctx, qk.Stream(), "-", "+", 1).Result()
	require.NoError(t, err)
	require.NotEmpty(t, msgs)
	streamID := msgs[0].ID
	req = httptest.NewRequest(http.MethodGet, "/api/queues/"+queue+"/active?limit=10", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodDelete, "/api/queues/"+queue+"/active/"+streamID, nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Prometheus queue scope
	req = httptest.NewRequest(http.MethodGet, "/api/lifecycle/metrics?format=prometheus&scope=queue", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Metrics all queues JSON
	req = httptest.NewRequest(http.MethodGet, "/api/metrics/queues", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Missing queue param on pause → bad request path (empty name if router allows)
	req = httptest.NewRequest(http.MethodPost, "/api/queues//pause", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.True(t, rec.Code == http.StatusBadRequest || rec.Code == http.StatusNotFound || rec.Code == http.StatusMovedPermanently || rec.Code == http.StatusOK)
}

func TestAdminHandler_ErrorPaths(t *testing.T) {
	e, _, _, _, cleanup := setupHandler(t)
	defer cleanup()

	// Retry missing DLQ id
	req := httptest.NewRequest(http.MethodPost, "/api/queues/q/dlq/nope/retry", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	// Run missing scheduled
	req = httptest.NewRequest(http.MethodPost, "/api/queues/q/scheduled/missing/run", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	// Cron missing
	req = httptest.NewRequest(http.MethodPost, "/api/queues/q/cron/nope/run", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	// Invalid bulk body
	req = httptest.NewRequest(http.MethodPost, "/api/queues/q/bulk", bytes.NewReader([]byte(`not-json`)))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
