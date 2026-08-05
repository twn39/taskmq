package lifecycle

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

// PrometheusText renders a Snapshot map as Prometheus exposition format (no client dependency).
func PrometheusText(snapshot map[string]int64) string {
	if len(snapshot) == 0 {
		return ""
	}
	// Stable-ish order is not required for Prometheus; keep insertion-friendly iteration.
	var b []byte
	for k, v := range snapshot {
		// Prefix with taskmq_lifecycle_ for scrape uniqueness.
		b = append(b, "taskmq_lifecycle_"...)
		b = append(b, k...)
		b = append(b, ' ')
		b = append(b, []byte(itoa(v))...)
		b = append(b, '\n')
	}
	return string(b)
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
