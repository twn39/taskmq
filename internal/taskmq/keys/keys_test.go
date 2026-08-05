package keys

import "testing"

func TestQueueKeys_StableSchema(t *testing.T) {
	qk := KeysFor("orders")

	cases := map[string]string{
		"Stream":               qk.Stream(),
		"Delayed":              qk.Delayed(),
		"DLQ":                  qk.DLQ(),
		"DLQIndex":             qk.DLQIndex(),
		"Unique":               qk.Unique("uk"),
		"CronConfigs":          qk.CronConfigs(),
		"CronSelfHealingLock":  qk.CronSelfHealingLock(),
		"Paused":               qk.Paused(),
		"Control":              qk.Control(),
		"Cancelled":            qk.Cancelled("tid"),
		"CancelChannel":        qk.CancelChannel(),
		"DelayedWakeupChannel": qk.DelayedWakeupChannel(),
		"RateLimit":            qk.RateLimit(""),
		"RateLimitGroup":       qk.RateLimit("tenant-A"),
	}

	want := map[string]string{
		"Stream":               "taskmq:{orders}:queue",
		"Delayed":              "taskmq:{orders}:delayed",
		"DLQ":                  "taskmq:{orders}:dlq",
		"DLQIndex":             "taskmq:{orders}:dlq_index",
		"Unique":               "taskmq:{orders}:unique:uk",
		"CronConfigs":          "taskmq:{orders}:cron_configs",
		"CronSelfHealingLock":  "taskmq:{orders}:cron_self_healing_lock",
		"Paused":               "taskmq:{orders}:paused",
		"Control":              "taskmq:{orders}:control",
		"Cancelled":            "taskmq:{orders}:cancelled:tid",
		"CancelChannel":        "taskmq:{orders}:cancel",
		"DelayedWakeupChannel": "taskmq:{orders}:delayed_wakeup",
		"RateLimit":            "taskmq:{orders}:rate_limit",
		"RateLimitGroup":       "taskmq:{orders}:rate_limit:tenant-A",
	}

	for name, got := range cases {
		if got != want[name] {
			t.Errorf("%s: got %q want %q", name, got, want[name])
		}
	}
}

func TestParseQueueFromStreamKey(t *testing.T) {
	q, ok := ParseQueueFromStreamKey("taskmq:{billing}:queue")
	if !ok || q != "billing" {
		t.Fatalf("got %q ok=%v", q, ok)
	}
	if _, ok := ParseQueueFromStreamKey("taskmq:{billing}:delayed"); ok {
		t.Fatal("expected non-stream key to fail")
	}
	if _, ok := ParseQueueFromStreamKey("taskmq:{}:queue"); ok {
		t.Fatal("expected empty queue to fail")
	}
}

func TestStreamScanPattern(t *testing.T) {
	if StreamScanPattern() != "taskmq:{*}:queue" {
		t.Fatalf("unexpected pattern %q", StreamScanPattern())
	}
}

// TestQueueKeys_HashTagAffinity documents the Redis Cluster contract: every
// multi-key Lua path must only touch keys under the same {queue} hash tag.
func TestQueueKeys_HashTagAffinity(t *testing.T) {
	const q = "billing-v2"
	tag := "{" + q + "}"
	qk := KeysFor(q)
	all := []string{
		qk.Stream(),
		qk.Delayed(),
		qk.DLQ(),
		qk.DLQIndex(),
		qk.Unique("k"),
		qk.Meta("tid"),
		qk.Metrics(),
		qk.Events(),
		qk.Completed(),
		qk.Paused(),
		qk.CronConfigs(),
		qk.CronSelfHealingLock(),
		qk.Cancelled("tid"),
		qk.CancelChannel(),
		qk.DelayedWakeupChannel(),
		qk.RateLimit(""),
		qk.RateLimit("g1"),
		qk.Heartbeat("c1"),
	}
	for _, k := range all {
		if k == "" {
			t.Fatal("empty key")
		}
		// Each key must contain exactly one hash tag for the queue.
		if !containsOnce(k, tag) {
			t.Errorf("key %q missing hash tag %q (cluster slot affinity broken)", k, tag)
		}
	}
}

func containsOnce(s, sub string) bool {
	i := indexOf(s, sub)
	if i < 0 {
		return false
	}
	return indexOf(s[i+len(sub):], sub) < 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
