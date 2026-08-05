package events

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
)

// Event type constants written to the queue events stream.
const (
	TypeEnqueued  = "enqueued"
	TypeActive    = "active"
	TypeCompleted = "completed"
	TypeFailed    = "failed"
	TypeRetry     = "retry"
	TypeDelayed   = "delayed"
	TypeProgress  = "progress"
	TypeStalled   = "stalled"
	TypeCancelled = "cancelled"
)

// DefaultMaxLen is the approximate MAXLEN for the events stream.
const DefaultMaxLen int64 = 10_000

// Event is a queue lifecycle notification.
type Event struct {
	ID          string `json:"id,omitempty"` // stream entry id
	Type        string `json:"type"`
	Queue       string `json:"queue"`
	TaskID      string `json:"task_id,omitempty"`
	Name        string `json:"name,omitempty"`
	Error       string `json:"error,omitempty"`
	Progress    int    `json:"progress,omitempty"`
	Data        string `json:"data,omitempty"`
	TimestampMs int64  `json:"timestamp_ms"`
}

// Publisher appends events to the queue events stream (best-effort).
type Publisher struct {
	rdb    redis.UniversalClient
	maxLen int64
}

// NewPublisher creates an events publisher. maxLen 0 uses DefaultMaxLen.
func NewPublisher(rdb redis.UniversalClient, maxLen int64) *Publisher {
	if maxLen <= 0 {
		maxLen = DefaultMaxLen
	}
	return &Publisher{rdb: rdb, maxLen: maxLen}
}

// Publish writes one event. Failures are ignored by callers (best-effort observability).
func (p *Publisher) Publish(ctx context.Context, e Event) error {
	if p == nil || p.rdb == nil || e.Queue == "" || e.Type == "" {
		return nil
	}
	if e.TimestampMs == 0 {
		e.TimestampMs = time.Now().UnixMilli()
	}
	values := map[string]interface{}{
		"type":         e.Type,
		"queue":        e.Queue,
		"task_id":      e.TaskID,
		"name":         e.Name,
		"error":        e.Error,
		"progress":     e.Progress,
		"data":         e.Data,
		"timestamp_ms": e.TimestampMs,
	}
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: keys.KeysFor(e.Queue).Events(),
		MaxLen: p.maxLen,
		Approx: true,
		Values: values,
	}).Err()
}

// Emit is a convenience helper for common fields.
func (p *Publisher) Emit(ctx context.Context, queue, typ, taskID, name, errMsg string) {
	if p == nil {
		return
	}
	_ = p.Publish(ctx, Event{
		Type:   typ,
		Queue:  queue,
		TaskID: taskID,
		Name:   name,
		Error:  errMsg,
	})
}

// Read returns up to count events after lastID ("0-0" or "$" for only new).
// Blocks up to block duration when block > 0.
func Read(ctx context.Context, rdb redis.UniversalClient, queue, lastID string, count int64, block time.Duration) ([]Event, error) {
	if lastID == "" {
		lastID = "0-0"
	}
	if count <= 0 {
		count = 50
	}
	stream := keys.KeysFor(queue).Events()
	args := &redis.XReadArgs{
		Streams: []string{stream, lastID},
		Count:   count,
		Block:   block,
	}
	res, err := rdb.XRead(ctx, args).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Event
	for _, s := range res {
		for _, msg := range s.Messages {
			out = append(out, parseEvent(msg))
		}
	}
	return out, nil
}

// ListRecent returns the last count events (XREVRANGE).
func ListRecent(ctx context.Context, rdb redis.UniversalClient, queue string, count int64) ([]Event, error) {
	if count <= 0 {
		count = 50
	}
	msgs, err := rdb.XRevRangeN(ctx, keys.KeysFor(queue).Events(), "+", "-", count).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(msgs))
	// Reverse so oldest→newest
	for i := len(msgs) - 1; i >= 0; i-- {
		out = append(out, parseEvent(msgs[i]))
	}
	return out, nil
}

func parseEvent(msg redis.XMessage) Event {
	e := Event{ID: msg.ID}
	if v, ok := msg.Values["type"].(string); ok {
		e.Type = v
	}
	if v, ok := msg.Values["queue"].(string); ok {
		e.Queue = v
	}
	if v, ok := msg.Values["task_id"].(string); ok {
		e.TaskID = v
	}
	if v, ok := msg.Values["name"].(string); ok {
		e.Name = v
	}
	if v, ok := msg.Values["error"].(string); ok {
		e.Error = v
	}
	if v, ok := msg.Values["data"].(string); ok {
		e.Data = v
	}
	switch v := msg.Values["progress"].(type) {
	case string:
		e.Progress, _ = strconv.Atoi(v)
	case int64:
		e.Progress = int(v)
	case int:
		e.Progress = v
	}
	switch v := msg.Values["timestamp_ms"].(type) {
	case string:
		e.TimestampMs, _ = strconv.ParseInt(v, 10, 64)
	case int64:
		e.TimestampMs = v
	}
	return e
}
