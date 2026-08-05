package metricsq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
)

// Counter field names stored in the queue metrics HASH (see keys.QueueKeys.Metrics).
const (
	FieldProcessed = "processed_total"
	FieldFailed    = "failed_total"
	FieldRetried   = "retried_total"
	FieldDLQ       = "dlq_total"
	FieldCompleted = "completed_total"
	FieldDeferred  = "deferred_total"
)

// Store increments Redis-backed per-queue counters (shared across processes).
type Store struct {
	rdb redis.UniversalClient
}

// NewStore creates a queue metrics store.
func NewStore(rdb redis.UniversalClient) *Store {
	return &Store{rdb: rdb}
}

// Inc adds delta to a named counter for queue.
func (s *Store) Inc(ctx context.Context, queue, field string, delta int64) {
	if s == nil || s.rdb == nil || queue == "" || field == "" || delta == 0 {
		return
	}
	_ = s.rdb.HIncrBy(ctx, keys.KeysFor(queue).Metrics(), field, delta).Err()
}

// Snapshot returns all counter fields for a queue.
func (s *Store) Snapshot(ctx context.Context, queue string) (map[string]int64, error) {
	if s == nil || s.rdb == nil {
		return map[string]int64{}, nil
	}
	m, err := s.rdb.HGetAll(ctx, keys.KeysFor(queue).Metrics()).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		n, _ := strconv.ParseInt(v, 10, 64)
		out[k] = n
	}
	return out, nil
}

// QueueDepths holds live Redis depth gauges (not cumulative).
type QueueDepths struct {
	Queue     string `json:"queue"`
	StreamLen int64  `json:"stream_len"`
	Delayed   int64  `json:"delayed"`
	DLQ       int64  `json:"dlq"`
	Completed int64  `json:"completed"`
	Paused    bool   `json:"paused"`
}

// Depths reads current sizes for a queue.
func Depths(ctx context.Context, rdb redis.UniversalClient, queue string) (QueueDepths, error) {
	qk := keys.KeysFor(queue)
	var d QueueDepths
	d.Queue = queue
	var err error
	if d.StreamLen, err = rdb.XLen(ctx, qk.Stream()).Result(); err != nil {
		return d, err
	}
	if d.Delayed, err = rdb.ZCard(ctx, qk.Delayed()).Result(); err != nil {
		return d, err
	}
	if d.DLQ, err = rdb.ZCard(ctx, qk.DLQ()).Result(); err != nil {
		return d, err
	}
	d.Completed, _ = rdb.ZCard(ctx, qk.Completed()).Result()
	n, _ := rdb.Exists(ctx, qk.Paused()).Result()
	d.Paused = n > 0
	return d, nil
}

// PrometheusText renders Redis-backed counters + depth gauges for the given queues.
func PrometheusText(ctx context.Context, rdb redis.UniversalClient, queues []string) (string, error) {
	var b strings.Builder
	b.WriteString("# HELP taskmq_queue_processed_total Tasks successfully completed\n")
	b.WriteString("# TYPE taskmq_queue_processed_total counter\n")
	b.WriteString("# HELP taskmq_queue_failed_total Tasks moved to DLQ\n")
	b.WriteString("# TYPE taskmq_queue_failed_total counter\n")
	b.WriteString("# HELP taskmq_queue_retried_total Tasks scheduled for retry\n")
	b.WriteString("# TYPE taskmq_queue_retried_total counter\n")
	b.WriteString("# HELP taskmq_queue_stream_length Current stream XLEN\n")
	b.WriteString("# TYPE taskmq_queue_stream_length gauge\n")
	b.WriteString("# HELP taskmq_queue_delayed_length Current delayed ZCARD\n")
	b.WriteString("# TYPE taskmq_queue_delayed_length gauge\n")
	b.WriteString("# HELP taskmq_queue_dlq_length Current DLQ ZCARD\n")
	b.WriteString("# TYPE taskmq_queue_dlq_length gauge\n")

	store := NewStore(rdb)
	for _, q := range queues {
		snap, err := store.Snapshot(ctx, q)
		if err != nil {
			return "", err
		}
		d, err := Depths(ctx, rdb, q)
		if err != nil {
			return "", err
		}
		lq := promLabel(q)
		fmt.Fprintf(&b, "taskmq_queue_processed_total{queue=%q} %d\n", lq, snap[FieldProcessed]+snap[FieldCompleted])
		fmt.Fprintf(&b, "taskmq_queue_failed_total{queue=%q} %d\n", lq, snap[FieldFailed]+snap[FieldDLQ])
		fmt.Fprintf(&b, "taskmq_queue_retried_total{queue=%q} %d\n", lq, snap[FieldRetried])
		fmt.Fprintf(&b, "taskmq_queue_stream_length{queue=%q} %d\n", lq, d.StreamLen)
		fmt.Fprintf(&b, "taskmq_queue_delayed_length{queue=%q} %d\n", lq, d.Delayed)
		fmt.Fprintf(&b, "taskmq_queue_dlq_length{queue=%q} %d\n", lq, d.DLQ)
	}
	fmt.Fprintf(&b, "# scraped_at_unix_ms %d\n", time.Now().UnixMilli())
	return b.String(), nil
}

func promLabel(q string) string {
	// queue names should already be safe; escape backslash/quotes if needed
	return strings.ReplaceAll(strings.ReplaceAll(q, `\`, `\\`), `"`, `\"`)
}
