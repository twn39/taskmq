package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/taskmq"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

// redisTestAddr returns REDIS_ADDR when set (CI), else localhost:6379.
func redisTestAddr() string {
	if v := strings.TrimSpace(os.Getenv("REDIS_ADDR")); v != "" {
		return v
	}
	return "localhost:6379"
}

// NewTestConfig provides a configuration for testing.
func NewTestConfig() *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			Port:     ":8081",
			GRPCPort: ":50085",
		},
		Logger: config.LoggerConfig{
			Level: "error", // Quiet logs during test
		},
		Redis: config.RedisConfig{
			Addr: redisTestAddr(),
		},
	}
}

// ProvideSharedLifecycle builds a Lifecycle from config for Fx graphs that use
// taskmq.ProvideWorkers (Lifecycle is required and should be shared with Client).
func ProvideSharedLifecycle(cfg *config.Config) *lifecycle.Lifecycle {
	return lifecycle.NewLifecycle(taskmq.LifecycleFromConfig(cfg))
}

// ProvideClientWithLifecycle wires a client that shares the same Lifecycle as workers.
func ProvideClientWithLifecycle(rdb redis.UniversalClient, lc *lifecycle.Lifecycle) mqclient.Client {
	return mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc))
}

// RequireRedis skips the test when Redis is unavailable (integration suite).
func RequireRedis(t *testing.T) (context.Context, context.CancelFunc, *redis.Client) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	rdb := redis.NewClient(&redis.Options{Addr: redisTestAddr()})
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		t.Skipf("redis unavailable at %s: %v", redisTestAddr(), err)
	}
	t.Cleanup(func() {
		_ = rdb.Close()
		cancel()
	})
	return ctx, cancel, rdb
}

// UniqueQueue returns an isolated queue name derived from the test name.
func UniqueQueue(t *testing.T, prefix string) string {
	t.Helper()
	name := t.Name()
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, " ", "_")
	if prefix == "" {
		prefix = "tq"
	}
	// Keep under Redis key comfort length.
	if len(name) > 80 {
		name = name[:80]
	}
	return fmt.Sprintf("%s_%s_%d", prefix, name, time.Now().UnixNano()%1_000_000)
}

// FlushQueue deletes common TaskMQ keys for a queue (stream, delayed, DLQ, meta, …).
func FlushQueue(ctx context.Context, rdb redis.UniversalClient, queue string, taskIDs ...string) {
	qk := keys.KeysFor(queue)
	toDel := []string{
		qk.Stream(),
		qk.Delayed(),
		qk.DLQ(),
		qk.DLQIndex(),
		qk.Metrics(),
		qk.Completed(),
		qk.Paused(),
		qk.Events(),
		mqworker.RateLimitKey(queue, ""),
	}
	for _, id := range taskIDs {
		toDel = append(toDel, qk.Meta(id), qk.Cancelled(id))
	}
	_ = rdb.Del(ctx, toDel...).Err()
}

// WaitUntil polls cond until true or timeout (test failure).
func WaitUntil(t *testing.T, cond func() bool, timeout, interval time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(interval)
	}
	require.Fail(t, msg)
}

// DefaultTestLifecycle returns a lifecycle suitable for integration workers/clients.
func DefaultTestLifecycle() *lifecycle.Lifecycle {
	return lifecycle.NewLifecycle(lifecycle.LifecycleConfig{
		DLQMaxCount:           1000,
		CompletedRetention:    time.Hour,
		CompletedMaxCount:     1000,
		SafeTrimEnabled:       false,
		PurgeCancelledDelayed: false,
	})
}

// NewTestClient builds a client sharing the given lifecycle (JSON codec).
func NewTestClient(rdb redis.UniversalClient, lc *lifecycle.Lifecycle) mqclient.Client {
	opts := []mqclient.ClientOption{mqclient.WithClientCodec(codec.JSONCodec{})}
	if lc != nil {
		opts = append(opts, mqclient.WithClientLifecycle(lc))
	}
	return mqclient.NewClient(rdb, opts...)
}

// NewTestWorkerPool builds a single-queue pool with common integration defaults.
// Callers Register handlers then Start/Stop.
func NewTestWorkerPool(
	rdb redis.UniversalClient,
	queue, group, consumer string,
	concurrency int,
	lc *lifecycle.Lifecycle,
	extra ...mqworker.WorkerPoolOption,
) mqworker.Worker {
	if concurrency <= 0 {
		concurrency = 1
	}
	if group == "" {
		group = "test-group"
	}
	if consumer == "" {
		consumer = "test-consumer"
	}
	if lc == nil {
		lc = DefaultTestLifecycle()
	}
	opts := []mqworker.WorkerPoolOption{
		mqworker.WithGroup(group),
		mqworker.WithConsumer(consumer),
		mqworker.WithConcurrency(concurrency),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
	}
	opts = append(opts, extra...)
	return mqworker.NewWorkerPool(rdb, zap.NewNop(), queue, opts...)
}

// WaitStreamLen waits until the queue stream reaches at least n messages (or empty if n==0 and exact).
func WaitStreamLen(t *testing.T, ctx context.Context, rdb redis.UniversalClient, queue string, n int64, timeout time.Duration) {
	t.Helper()
	stream := keys.KeysFor(queue).Stream()
	WaitUntil(t, func() bool {
		xlen, err := rdb.XLen(ctx, stream).Result()
		return err == nil && xlen == n
	}, timeout, 50*time.Millisecond, fmt.Sprintf("stream %s len != %d", queue, n))
}

// WaitKeyGone waits until Redis key is deleted.
func WaitKeyGone(t *testing.T, ctx context.Context, rdb redis.UniversalClient, key string, timeout time.Duration) {
	t.Helper()
	WaitUntil(t, func() bool {
		n, err := rdb.Exists(ctx, key).Result()
		return err == nil && n == 0
	}, timeout, 50*time.Millisecond, "key still exists: "+key)
}
