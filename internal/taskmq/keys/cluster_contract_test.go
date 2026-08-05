package keys_test

import (
	"strings"
	"testing"

	"github.com/twn39/taskmq/internal/taskmq/keys"
)

// TestQueueKeys_AllShareHashTag is the unit-level cluster contract:
// every multi-key Lua participant must include taskmq:{queue}: so Cluster
// keeps them on one slot. Failures here are CROSSSLOT bugs waiting to happen.
func TestQueueKeys_AllShareHashTag(t *testing.T) {
	queue := "billing-v2"
	tag := "{" + queue + "}"
	qk := keys.KeysFor(queue)

	mustContain := []string{
		qk.Stream(),
		qk.Delayed(),
		qk.DLQ(),
		qk.DLQIndex(),
		qk.Unique("uk"),
		qk.CronConfigs(),
		qk.CronSelfHealingLock(),
		qk.Paused(),
		qk.Control(),
		qk.Cancelled("tid"),
		qk.CancelChannel(),
		qk.DelayedWakeupChannel(),
		qk.RateLimit(""),
		qk.RateLimit("g1"),
		qk.Meta("tid"),
		qk.Completed(),
		qk.Metrics(),
		qk.Heartbeat("c1"),
		qk.Events(),
	}
	for _, k := range mustContain {
		if !strings.Contains(k, tag) {
			t.Errorf("key %q missing hash tag %s", k, tag)
		}
		if !strings.HasPrefix(k, "taskmq:") {
			t.Errorf("key %q missing taskmq: prefix", k)
		}
	}

	// Different queues → different tags (may map to different slots).
	other := keys.KeysFor("other-q")
	if qk.Stream() == other.Stream() {
		t.Fatal("distinct queues must produce distinct stream keys")
	}
	if !strings.Contains(other.Stream(), "{other-q}") {
		t.Fatal(other.Stream())
	}
}

// TestQueueKeys_NoCrossQueueCollision documents that stream/delayed/dlq
// for the same queue share one tag substring (single-slot multi-key scripts).
func TestQueueKeys_NoCrossQueueCollision(t *testing.T) {
	qk := keys.KeysFor("q")
	// Extract tag region from stream and require delayed shares it.
	stream := qk.Stream()
	start := strings.Index(stream, "{")
	end := strings.Index(stream, "}")
	if start < 0 || end <= start {
		t.Fatalf("no tag in %s", stream)
	}
	tag := stream[start : end+1]
	for _, k := range []string{qk.Delayed(), qk.DLQ(), qk.Unique("u"), qk.Meta("id")} {
		if !strings.Contains(k, tag) {
			t.Fatalf("%s missing %s", k, tag)
		}
	}
}
