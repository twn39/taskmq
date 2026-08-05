package integration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	mqworker "github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

// TestTaskMQ_ClusterHashTagSmoke exercises multi-key Lua paths on a real Redis
// Cluster when TASKMQ_REDIS_CLUSTER_ADDRS is set (comma-separated seed nodes).
//
// Local: docker compose -f docker-compose.cluster.yml up -d
//   export TASKMQ_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
//
// Skipped in default CI (standalone Redis service only).
func TestTaskMQ_ClusterHashTagSmoke(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv("TASKMQ_REDIS_CLUSTER_ADDRS"))
	if raw == "" {
		t.Skip("set TASKMQ_REDIS_CLUSTER_ADDRS to run cluster smoke (e.g. 127.0.0.1:7000,127.0.0.1:7001)")
	}
	addrs := splitCSV(raw)
	require.NotEmpty(t, addrs)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Ping(ctx).Err(), "cluster ping failed")

	queue := UniqueQueue(t, "cluster")
	FlushQueue(ctx, rdb, queue)
	qk := keys.KeysFor(queue)
	// Document cluster contract: stream and delayed share the same hash tag.
	require.Contains(t, qk.Stream(), "{"+queue+"}")
	require.Contains(t, qk.Delayed(), "{"+queue+"}")
	require.Contains(t, qk.DLQ(), "{"+queue+"}")

	lc := DefaultTestLifecycle()
	var ran, fails int
	pool := mqworker.NewWorkerPool(rdb, zap.NewNop(), queue,
		mqworker.WithGroup("cluster-g"),
		mqworker.WithConsumer("cluster-c"),
		mqworker.WithConcurrency(2),
		mqworker.WithCodec(codec.JSONCodec{}),
		mqworker.WithLifecycle(lc),
		mqworker.WithRetryPolicy(policy.NewExponentialBackoff(10*time.Millisecond, time.Second, false)),
	)
	pool.Register("job", func(ctx context.Context, task *taskmodel.Task) error {
		ran++
		return nil
	})
	// Failure → retry uses Lua across stream+delayed under same tag.
	pool.Register("fail", func(ctx context.Context, task *taskmodel.Task) error {
		fails++
		if fails == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(ctx)

	c := mqclient.NewClient(rdb, mqclient.WithClientLifecycle(lc), mqclient.WithClientCodec(codec.JSONCodec{}))
	// Immediate + unique + delayed all use multi-key / multi-slot-sensitive paths under {queue}.
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`1`), taskmodel.TaskOptions{
		ID: "c-1", Queue: queue,
	})))
	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("job", []byte(`2`), taskmodel.TaskOptions{
		ID: "c-2", Queue: queue, UniqueKey: "uk-cluster", UniqueTTL: time.Minute,
	})))
	require.NoError(t, c.EnqueueIn(ctx, taskmodel.NewTask("job", []byte(`3`), taskmodel.TaskOptions{
		ID: "c-3", Queue: queue,
	}), 200*time.Millisecond))

	WaitUntil(t, func() bool { return ran >= 2 }, 20*time.Second, 50*time.Millisecond, "immediate tasks should complete on cluster")
	WaitUntil(t, func() bool { return ran >= 3 }, 20*time.Second, 50*time.Millisecond, "delayed task should promote and run on cluster")

	require.NoError(t, c.Enqueue(ctx, taskmodel.NewTask("fail", []byte(`f`), taskmodel.TaskOptions{
		ID: "c-fail", Queue: queue, MaxRetry: taskmodel.Ptr(2),
	})))
	WaitUntil(t, func() bool { return fails >= 2 }, 20*time.Second, 50*time.Millisecond, "retry should reschedule under cluster hash tag")

	// CROSSSLOT guard: keys from two queues must not be mixed in a multi-key script.
	// (Sanity: different tags land on potentially different slots.)
	other := UniqueQueue(t, "cluster_other")
	FlushQueue(ctx, rdb, other)
	slotA, err := rdb.ClusterKeySlot(ctx, keys.KeysFor(queue).Stream()).Result()
	require.NoError(t, err)
	slotB, err := rdb.ClusterKeySlot(ctx, keys.KeysFor(other).Stream()).Result()
	require.NoError(t, err)
	// Not required that slots differ, but tags must differ.
	require.NotEqual(t, keys.KeysFor(queue).Stream(), keys.KeysFor(other).Stream())
	t.Logf("cluster slots queueA=%d queueB=%d", slotA, slotB)

	// Negative: EVAL with keys from two queues must fail with CROSSSLOT when slots differ.
	// If unlucky slots collide, skip the assertion (cluster has finite slot range).
	if slotA != slotB {
		err = rdb.Eval(ctx, "return 1", []string{
			keys.KeysFor(queue).Stream(),
			keys.KeysFor(other).Stream(),
		}).Err()
		require.Error(t, err)
		require.Contains(t, strings.ToUpper(err.Error()), "CROSSSLOT")
	} else {
		t.Logf("slots collided (%d); CROSSSLOT negative skipped", slotA)
	}

	// Positive: same-tag multi-key EVAL is allowed (stream + delayed).
	err = rdb.Eval(ctx, "return 1", []string{
		keys.KeysFor(queue).Stream(),
		keys.KeysFor(queue).Delayed(),
	}).Err()
	require.NoError(t, err, "same-tag multi-key EVAL must succeed on cluster")
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
