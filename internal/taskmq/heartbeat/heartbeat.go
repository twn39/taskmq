package heartbeat

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
)

// Info is written to the heartbeat key by workers.
type Info struct {
	Queue       string    `json:"queue"`
	Consumer    string    `json:"consumer"`
	Host        string    `json:"host,omitempty"`
	PID         int       `json:"pid,omitempty"`
	Concurrency int       `json:"concurrency,omitempty"`
	InUse       int       `json:"in_use,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ActiveTask  string    `json:"active_task,omitempty"`
}

// Reporter periodically refreshes a consumer heartbeat key.
type Reporter struct {
	rdb         redis.UniversalClient
	queue       string
	consumer    string
	concurrency int
	ttl         time.Duration
	interval    time.Duration
	startedAt   time.Time
	inUse       func() int
	activeTask  func() string
}

// NewReporter builds a heartbeat reporter. ttl defaults to 30s, interval to 10s.
func NewReporter(rdb redis.UniversalClient, queue, consumer string, concurrency int, opts ...func(*Reporter)) *Reporter {
	r := &Reporter{
		rdb:         rdb,
		queue:       queue,
		consumer:    consumer,
		concurrency: concurrency,
		ttl:         30 * time.Second,
		interval:    10 * time.Second,
		startedAt:   time.Now(),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// WithInUse supplies live concurrency usage.
func WithInUse(fn func() int) func(*Reporter) {
	return func(r *Reporter) { r.inUse = fn }
}

// WithActiveTask supplies optional current task id.
func WithActiveTask(fn func() string) func(*Reporter) {
	return func(r *Reporter) { r.activeTask = fn }
}

// Run publishes heartbeats until ctx is cancelled.
func (r *Reporter) Run(ctx context.Context) error {
	if r == nil || r.rdb == nil || r.queue == "" || r.consumer == "" {
		<-ctx.Done()
		return ctx.Err()
	}
	_ = r.beat(ctx)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// Best-effort delete on stop so dashboards clear quickly.
			_ = r.rdb.Del(context.Background(), keys.KeysFor(r.queue).Heartbeat(r.consumer)).Err()
			return ctx.Err()
		case <-t.C:
			_ = r.beat(ctx)
		}
	}
}

func (r *Reporter) beat(ctx context.Context) error {
	host, _ := os.Hostname()
	info := Info{
		Queue:       r.queue,
		Consumer:    r.consumer,
		Host:        host,
		PID:         os.Getpid(),
		Concurrency: r.concurrency,
		StartedAt:   r.startedAt,
		UpdatedAt:   time.Now(),
	}
	if r.inUse != nil {
		info.InUse = r.inUse()
	}
	if r.activeTask != nil {
		info.ActiveTask = r.activeTask()
	}
	b, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, keys.KeysFor(r.queue).Heartbeat(r.consumer), b, r.ttl).Err()
}

// List returns live heartbeats for a queue (SCAN + GET).
func List(ctx context.Context, rdb redis.UniversalClient, queue string) ([]Info, error) {
	pattern := keys.KeysFor(queue).HeartbeatScanPattern()
	var (
		cursor uint64
		out    []Info
	)
	for {
		keysFound, next, err := rdb.Scan(ctx, cursor, pattern, 50).Result()
		if err != nil {
			return out, err
		}
		for _, k := range keysFound {
			val, err := rdb.Get(ctx, k).Bytes()
			if err != nil {
				continue
			}
			var info Info
			if json.Unmarshal(val, &info) != nil {
				// Fallback: parse consumer from key suffix
				info.Queue = queue
				if i := strings.LastIndex(k, ":heartbeat:"); i >= 0 {
					info.Consumer = k[i+len(":heartbeat:"):]
				}
			}
			out = append(out, info)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

// ParseConsumerFromKey extracts consumer from a heartbeat redis key (tests/helpers).
func ParseConsumerFromKey(redisKey string) string {
	const marker = ":heartbeat:"
	i := strings.LastIndex(redisKey, marker)
	if i < 0 {
		return ""
	}
	return redisKey[i+len(marker):]
}

// FormatAge is a helper for CLI display.
func FormatAge(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconv.FormatInt(int64(time.Since(t).Seconds()), 10) + "s"
}
