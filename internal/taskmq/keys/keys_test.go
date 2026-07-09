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
