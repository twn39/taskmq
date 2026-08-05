package lifecycle_test

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
)

func TestPrometheusText(t *testing.T) {
	t.Parallel()
	text := lifecycle.PrometheusText(map[string]int64{
		"enqueue_rejected_total": 3,
		"soft_limit_hits_total":  0,
	})
	assert.Contains(t, text, "# HELP taskmq_lifecycle_enqueue_rejected_total")
	assert.Contains(t, text, "# TYPE taskmq_lifecycle_enqueue_rejected_total counter")
	assert.Contains(t, text, "taskmq_lifecycle_enqueue_rejected_total 3")
	assert.Contains(t, text, "taskmq_lifecycle_soft_limit_hits_total 0")
	// Stable alphabetical order: enqueue_* before soft_*
	ei := strings.Index(text, "taskmq_lifecycle_enqueue_rejected_total 3")
	si := strings.Index(text, "taskmq_lifecycle_soft_limit_hits_total 0")
	assert.True(t, ei >= 0 && si > ei, "expected sorted metric names")
	assert.True(t, strings.HasSuffix(text, "\n"))
}

func TestMetricsSinkMirror(t *testing.T) {
	t.Parallel()
	var seen atomic.Int64
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	lc.WithMetricsSink(lifecycle.FuncSink(func(name string, delta int64) {
		if name == "payload_rejected_total" {
			seen.Add(delta)
		}
	}))

	err := lc.CheckPayloadSize(make([]byte, 10))
	require.NoError(t, err)

	lc2 := lifecycle.NewLifecycle(lifecycle.LifecycleConfig{MaxPayloadBytes: 4})
	lc2.WithMetricsSink(lifecycle.FuncSink(func(name string, delta int64) {
		if name == "payload_rejected_total" {
			seen.Add(delta)
		}
	}))
	err = lc2.CheckPayloadSize([]byte("too-big"))
	require.ErrorIs(t, err, lifecycle.ErrPayloadTooLarge)
	assert.Equal(t, int64(1), seen.Load())
	assert.Equal(t, int64(1), lc2.Metrics().PayloadRejectedTotal.Load())
}

func TestIncMetric(t *testing.T) {
	t.Parallel()
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	lc.IncMetric("dlq_evicted_total", 5)
	assert.Equal(t, int64(5), lc.Metrics().DLQEvictedTotal.Load())
	snap := lc.Metrics().Snapshot()
	assert.Equal(t, int64(5), snap["dlq_evicted_total"])
}
