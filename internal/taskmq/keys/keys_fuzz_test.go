package keys_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/keys"
)

// FuzzParseQueueFromStreamKey verifies that ParseQueueFromStreamKey never panics
// on arbitrary key strings and satisfies strict bi-directional invariants.
func FuzzParseQueueFromStreamKey(f *testing.F) {
	// Seed corpus
	f.Add("taskmq:{default}:queue")
	f.Add("taskmq:{email-high-priority}:queue")
	f.Add("taskmq:{}:queue")
	f.Add("taskmq:{")
	f.Add("}:queue")
	f.Add("taskmq:{queue")
	f.Add("taskmq:{a}:queue:extra")
	f.Add("prefix:taskmq:{q}:queue")
	f.Add("")
	f.Add("taskmq:{{nested}}:queue")
	f.Add("taskmq:{queue_with_特殊字符_🚀}:queue")
	f.Add(strings.Repeat("a", 1024))

	f.Fuzz(func(t *testing.T, key string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ParseQueueFromStreamKey panicked on key %q: %v", key, r)
			}
		}()

		queue, ok := keys.ParseQueueFromStreamKey(key)
		if ok {
			// Invariants when parse succeeds:
			require.NotEmpty(t, queue)
			require.True(t, strings.HasPrefix(key, "taskmq:{"))
			require.True(t, strings.HasSuffix(key, "}:queue"))

			// Re-constructing must reproduce the exact key
			k := keys.KeysFor(queue)
			require.Equal(t, key, k.Stream())

			// Idempotence
			q2, ok2 := keys.ParseQueueFromStreamKey(k.Stream())
			require.True(t, ok2)
			require.Equal(t, queue, q2)
		}
	})
}

// FuzzKeysFor_ClusterHashTags verifies that for arbitrary queue names,
// all generated keys correctly adhere to Redis Cluster {queue} hash-tagging.
func FuzzKeysFor_ClusterHashTags(f *testing.F) {
	// Seed corpus
	f.Add("default", "task-1", "uniq-key", "grp-1", "consumer-1")
	f.Add("orders_v2", "uuid-xyz", "uk-123", "", "worker-pod-0")
	f.Add("队列_中_文", "任务-1", "唯一-1", "分组-1", "消费者-1")
	f.Add("", "", "", "", "")

	f.Fuzz(func(t *testing.T, queue, taskID, uniqueKey, groupKey, consumer string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("KeysFor operations panicked: queue=%q, taskID=%q: %v", queue, taskID, r)
			}
		}()

		k := keys.KeysFor(queue)
		require.Equal(t, queue, k.Queue())

		expectedTag := "{" + queue + "}"

		allKeys := []string{
			k.Stream(),
			k.Delayed(),
			k.DLQ(),
			k.DLQIndex(),
			k.CronConfigs(),
			k.CronSelfHealingLock(),
			k.Paused(),
			k.Control(),
			k.CancelChannel(),
			k.DelayedWakeupChannel(),
			k.Completed(),
			k.Metrics(),
			k.Events(),
			k.HeartbeatScanPattern(),
			k.Unique(uniqueKey),
			k.Cancelled(taskID),
			k.RateLimit(groupKey),
			k.Meta(taskID),
			k.Heartbeat(consumer),
		}

		for _, redisKey := range allKeys {
			// Every key in TaskMQ must contain the {queue} hash tag for cluster co-location
			require.Contains(t, redisKey, "taskmq:"+expectedTag)
		}

		// If queue name does not contain braces and is non-empty, ParseQueueFromStreamKey
		// must always round-trip cleanly back to the queue name.
		if queue != "" && !strings.ContainsAny(queue, "{}") {
			parsedQueue, ok := keys.ParseQueueFromStreamKey(k.Stream())
			require.True(t, ok)
			require.Equal(t, queue, parsedQueue)
		}
	})
}
