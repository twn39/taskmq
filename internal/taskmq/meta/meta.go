package meta

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// Task state values stored in meta hashes.
const (
	StatePending   = "pending"
	StateActive    = "active"
	StateDelayed   = "delayed"
	StateRetry     = "retry"
	StateCompleted = "completed"
	StateDLQ       = "dlq"
	StateCancelled = "cancelled"
)

// TaskInfo is the inspector view of a task's durable metadata.
type TaskInfo struct {
	ID          string    `json:"id"`
	Queue       string    `json:"queue"`
	Name        string    `json:"name"`
	State       string    `json:"state"`
	Retry       int       `json:"retry"`
	MaxRetry    int       `json:"max_retry"`
	LastError   string    `json:"last_error,omitempty"`
	TimeoutMs   int       `json:"timeout_ms,omitempty"`
	DeadlineMs  int64     `json:"deadline_ms,omitempty"`
	UniqueKey   string    `json:"unique_key,omitempty"`
	GroupKey    string    `json:"group_key,omitempty"`
	StreamID    string    `json:"stream_id,omitempty"`
	Result      []byte    `json:"result,omitempty"`
	Progress    int       `json:"progress,omitempty"` // 0-100
	ProgressData string   `json:"progress_data,omitempty"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

// Store reads/writes per-task meta via keys.QueueKeys.Meta.
type Store struct {
	rdb redis.UniversalClient
}

// NewStore creates a meta Store.
func NewStore(rdb redis.UniversalClient) *Store {
	return &Store{rdb: rdb}
}

// Put writes core task fields and state. Does not store full payload (use stream/DLQ for body).
func (s *Store) Put(ctx context.Context, t *taskmodel.Task, state string) error {
	if s == nil || s.rdb == nil || t == nil || t.ID == "" {
		return nil
	}
	now := time.Now().UnixMilli()
	qk := keys.KeysFor(t.Queue)
	fields := map[string]interface{}{
		"id":          t.ID,
		"queue":       t.Queue,
		"name":        t.Name,
		"state":       state,
		"retry":       t.Retry,
		"max_retry":   t.MaxRetry,
		"timeout_ms":  t.TimeoutMs,
		"deadline_ms": t.DeadlineMs,
		"unique_key":  t.UniqueKey,
		"group_key":   t.GroupKey,
		"last_error":  t.LastError,
		"updated_at":  now,
	}
	if !t.CreatedAt.IsZero() {
		fields["created_at"] = t.CreatedAt.UnixMilli()
	} else {
		fields["created_at"] = now
	}
	return s.rdb.HSet(ctx, qk.Meta(t.ID), fields).Err()
}

// SetState updates state and optional error/stream id.
func (s *Store) SetState(ctx context.Context, queue, taskID, state string, lastError string, streamID string) error {
	if s == nil || s.rdb == nil || taskID == "" {
		return nil
	}
	fields := map[string]interface{}{
		"state":      state,
		"updated_at": time.Now().UnixMilli(),
	}
	if lastError != "" {
		fields["last_error"] = lastError
	}
	if streamID != "" {
		fields["stream_id"] = streamID
	}
	return s.rdb.HSet(ctx, keys.KeysFor(queue).Meta(taskID), fields).Err()
}

// MarkCompleted stores completed state, optional result, indexes completed ZSET, applies TTL/cap.
// If retention <= 0, deletes meta instead (default fire-and-forget success path).
func (s *Store) MarkCompleted(ctx context.Context, t *taskmodel.Task, result []byte, retention time.Duration, maxCount int64) error {
	if s == nil || s.rdb == nil || t == nil || t.ID == "" {
		return nil
	}
	qk := keys.KeysFor(t.Queue)
	metaKey := qk.Meta(t.ID)
	if retention <= 0 {
		return s.rdb.Del(ctx, metaKey).Err()
	}
	now := time.Now()
	nowMs := now.UnixMilli()
	fields := map[string]interface{}{
		"id":           t.ID,
		"queue":        t.Queue,
		"name":         t.Name,
		"state":        StateCompleted,
		"retry":        t.Retry,
		"max_retry":    t.MaxRetry,
		"timeout_ms":   t.TimeoutMs,
		"deadline_ms":  t.DeadlineMs,
		"updated_at":   nowMs,
		"completed_at": nowMs,
		"last_error":   "",
	}
	if len(result) > 0 {
		fields["result"] = result
	}
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, metaKey, fields)
	pipe.PExpire(ctx, metaKey, retention)
	pipe.ZAdd(ctx, qk.Completed(), redis.Z{Score: float64(nowMs), Member: t.ID})
	if maxCount > 0 {
		// Keep newest maxCount; drop oldest members and their meta.
		pipe.ZRemRangeByRank(ctx, qk.Completed(), 0, -(maxCount + 1))
	}
	// Also expire completed index entry via score-based cleanup in janitor; best-effort PExpire on meta is enough for TTL.
	_, err := pipe.Exec(ctx)
	return err
}

// MarkDLQ sets state=dlq and last_error.
func (s *Store) MarkDLQ(ctx context.Context, t *taskmodel.Task, errMsg string) error {
	if t != nil {
		t.LastError = errMsg
	}
	return s.Put(ctx, t, StateDLQ)
}

// MarkRetry sets state=retry.
func (s *Store) MarkRetry(ctx context.Context, t *taskmodel.Task, errMsg string) error {
	if t != nil && errMsg != "" {
		t.LastError = errMsg
	}
	return s.Put(ctx, t, StateRetry)
}

// Delete removes meta for a task.
func (s *Store) Delete(ctx context.Context, queue, taskID string) error {
	if s == nil || s.rdb == nil {
		return nil
	}
	return s.rdb.Del(ctx, keys.KeysFor(queue).Meta(taskID)).Err()
}

// SetProgress updates progress percent (clamped 0-100) and optional data string.
func (s *Store) SetProgress(ctx context.Context, queue, taskID string, percent int, data string) error {
	if s == nil || s.rdb == nil || taskID == "" {
		return nil
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	fields := map[string]interface{}{
		"progress":      percent,
		"progress_data": data,
		"updated_at":    time.Now().UnixMilli(),
	}
	return s.rdb.HSet(ctx, keys.KeysFor(queue).Meta(taskID), fields).Err()
}

// Get loads TaskInfo by queue + id. Returns nil, nil when missing.
func (s *Store) Get(ctx context.Context, queue, taskID string) (*TaskInfo, error) {
	if s == nil || s.rdb == nil {
		return nil, fmt.Errorf("meta: store not configured")
	}
	m, err := s.rdb.HGetAll(ctx, keys.KeysFor(queue).Meta(taskID)).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	return mapToInfo(m), nil
}

func mapToInfo(m map[string]string) *TaskInfo {
	info := &TaskInfo{
		ID:         m["id"],
		Queue:      m["queue"],
		Name:       m["name"],
		State:      m["state"],
		LastError:  m["last_error"],
		UniqueKey:  m["unique_key"],
		GroupKey:   m["group_key"],
		StreamID:   m["stream_id"],
		Retry:      atoi(m["retry"]),
		MaxRetry:   atoi(m["max_retry"]),
		TimeoutMs:  atoi(m["timeout_ms"]),
		DeadlineMs: atoi64(m["deadline_ms"]),
	}
	if r, ok := m["result"]; ok && r != "" {
		info.Result = []byte(r)
	}
	info.Progress = atoi(m["progress"])
	info.ProgressData = m["progress_data"]
	if v := atoi64(m["created_at"]); v > 0 {
		info.CreatedAt = time.UnixMilli(v)
	}
	if v := atoi64(m["updated_at"]); v > 0 {
		info.UpdatedAt = time.UnixMilli(v)
	}
	if v := atoi64(m["completed_at"]); v > 0 {
		info.CompletedAt = time.UnixMilli(v)
	}
	return info
}

// PurgeExpiredCompleted removes completed ZSET members older than beforeMs and deletes meta.
func (s *Store) PurgeExpiredCompleted(ctx context.Context, queue string, beforeMs int64, batch int64) (int64, error) {
	if s == nil || s.rdb == nil {
		return 0, nil
	}
	if batch <= 0 {
		batch = 100
	}
	qk := keys.KeysFor(queue)
	ids, err := s.rdb.ZRangeByScore(ctx, qk.Completed(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(beforeMs, 10),
		Count: batch,
	}).Result()
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	pipe := s.rdb.Pipeline()
	for _, id := range ids {
		pipe.Del(ctx, qk.Meta(id))
		pipe.ZRem(ctx, qk.Completed(), id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return int64(len(ids)), nil
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
