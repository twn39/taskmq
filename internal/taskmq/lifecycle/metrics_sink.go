package lifecycle

import (
	"sort"
	"strings"
)

// MetricsSink is an optional observer for lifecycle counters.
// The built-in LifecycleMetrics always records; a sink can mirror increments
// into an external system (Prometheus, statsd, etc.) without coupling core code.
type MetricsSink interface {
	// Inc adds delta to the named counter (see Snapshot keys for names).
	Inc(name string, delta int64)
}

// MultiSink fans out to multiple sinks (nil entries skipped).
type MultiSink []MetricsSink

// Inc implements MetricsSink.
func (m MultiSink) Inc(name string, delta int64) {
	for _, s := range m {
		if s != nil {
			s.Inc(name, delta)
		}
	}
}

// NopSink discards all increments.
type NopSink struct{}

// Inc implements MetricsSink.
func (NopSink) Inc(string, int64) {}

// FuncSink adapts a function to MetricsSink.
type FuncSink func(name string, delta int64)

// Inc implements MetricsSink.
func (f FuncSink) Inc(name string, delta int64) {
	if f != nil {
		f(name, delta)
	}
}

// metricHelp maps Snapshot counter names to Prometheus HELP text.
var metricHelp = map[string]string{
	"enqueue_rejected_total":       "Tasks rejected by stream enqueue hard limit.",
	"delayed_rejected_total":       "Delayed tasks rejected by delayed capacity policy.",
	"payload_rejected_total":       "Tasks rejected for exceeding max payload size.",
	"safe_trim_deleted_total":      "Stream entries deleted by safe MINID trim janitor.",
	"dlq_evicted_total":            "DLQ entries evicted by dlq_max_count policy.",
	"cancelled_delayed_purged":     "Cancelled delayed tasks purged by retention janitor.",
	"idle_consumers_removed_total": "Idle consumer group members removed by janitor.",
	"soft_limit_hits_total":        "Soft enqueue limit observations (stream length).",
}

// PrometheusText renders a Snapshot map as Prometheus exposition format
// (no client dependency). Includes # HELP / # TYPE and stable name order.
// Metric names are prefixed with taskmq_lifecycle_.
func PrometheusText(snapshot map[string]int64) string {
	if len(snapshot) == 0 {
		return ""
	}
	names := make([]string, 0, len(snapshot))
	for k := range snapshot {
		names = append(names, k)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		full := "taskmq_lifecycle_" + name
		help := metricHelp[name]
		if help == "" {
			help = "TaskMQ lifecycle counter " + name + "."
		}
		b.WriteString("# HELP ")
		b.WriteString(full)
		b.WriteByte(' ')
		b.WriteString(help)
		b.WriteByte('\n')
		b.WriteString("# TYPE ")
		b.WriteString(full)
		b.WriteString(" counter\n")
		b.WriteString(full)
		b.WriteByte(' ')
		b.WriteString(itoa(snapshot[name]))
		b.WriteByte('\n')
	}
	return b.String()
}

func itoa(v int64) string {
	// tiny helper to avoid fmt for hot path-free metrics render
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
