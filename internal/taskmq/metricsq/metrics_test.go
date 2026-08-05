package metricsq_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/metricsq"
)

func setup(t *testing.T) (context.Context, redis.UniversalClient, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return context.Background(), rdb, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func TestStore_IncAndSnapshot(t *testing.T) {
	ctx, rdb, cleanup := setup(t)
	defer cleanup()

	s := metricsq.NewStore(rdb)
	s.Inc(ctx, "q1", metricsq.FieldCompleted, 2)
	s.Inc(ctx, "q1", metricsq.FieldRetried, 1)
	s.Inc(ctx, "q1", metricsq.FieldFailed, 3)
	// no-ops
	s.Inc(ctx, "", metricsq.FieldCompleted, 1)
	s.Inc(ctx, "q1", "", 1)
	s.Inc(ctx, "q1", metricsq.FieldCompleted, 0)
	var nilStore *metricsq.Store
	nilStore.Inc(ctx, "q1", metricsq.FieldCompleted, 1)

	snap, err := s.Snapshot(ctx, "q1")
	require.NoError(t, err)
	require.Equal(t, int64(2), snap[metricsq.FieldCompleted])
	require.Equal(t, int64(1), snap[metricsq.FieldRetried])
	require.Equal(t, int64(3), snap[metricsq.FieldFailed])

	empty, err := s.Snapshot(ctx, "missing")
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestDepths_AndPrometheusText(t *testing.T) {
	ctx, rdb, cleanup := setup(t)
	defer cleanup()

	queue := "mq-depth"
	qk := keys.KeysFor(queue)
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: qk.Stream(),
		Values: map[string]interface{}{"task": "{}"},
	}).Err())
	require.NoError(t, rdb.ZAdd(ctx, qk.Delayed(), redis.Z{Score: 1, Member: "a"}).Err())
	require.NoError(t, rdb.ZAdd(ctx, qk.DLQ(), redis.Z{Score: 1, Member: "b"}).Err())
	require.NoError(t, rdb.Set(ctx, qk.Paused(), "1", 0).Err())

	d, err := metricsq.Depths(ctx, rdb, queue)
	require.NoError(t, err)
	require.Equal(t, queue, d.Queue)
	require.Equal(t, int64(1), d.StreamLen)
	require.Equal(t, int64(1), d.Delayed)
	require.Equal(t, int64(1), d.DLQ)
	require.True(t, d.Paused)

	metricsq.NewStore(rdb).Inc(ctx, queue, metricsq.FieldProcessed, 5)
	text, err := metricsq.PrometheusText(ctx, rdb, []string{queue})
	require.NoError(t, err)
	require.Contains(t, text, "taskmq_queue_processed_total")
	require.Contains(t, text, "taskmq_queue_stream_length")
	require.Contains(t, text, queue)
}
