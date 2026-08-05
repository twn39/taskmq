package keys

import (
	"fmt"
	"strings"
)

// QueueKeys is the single source of truth for Redis key names for a queue.
// All TaskMQ Redis keys that participate in multi-key Lua scripts use the
// hash-tag form taskmq:{queue}:… so they stay on one Cluster slot.
type QueueKeys struct {
	queue string
}

// KeysFor returns a typed QueueKeys generator for the specified queue name.
func KeysFor(queue string) QueueKeys {
	return QueueKeys{queue: queue}
}

// Queue returns the queue name this generator was created for.
func (k QueueKeys) Queue() string {
	return k.queue
}

func (k QueueKeys) Stream() string {
	return fmt.Sprintf("taskmq:{%s}:queue", k.queue)
}

func (k QueueKeys) Delayed() string {
	return fmt.Sprintf("taskmq:{%s}:delayed", k.queue)
}

func (k QueueKeys) DLQ() string {
	return fmt.Sprintf("taskmq:{%s}:dlq", k.queue)
}

func (k QueueKeys) DLQIndex() string {
	return fmt.Sprintf("taskmq:{%s}:dlq_index", k.queue)
}

func (k QueueKeys) Unique(uniqueKey string) string {
	return fmt.Sprintf("taskmq:{%s}:unique:%s", k.queue, uniqueKey)
}

func (k QueueKeys) CronConfigs() string {
	return fmt.Sprintf("taskmq:{%s}:cron_configs", k.queue)
}

// CronSelfHealingLock is the distributed lock used by the cron self-healing scanner.
func (k QueueKeys) CronSelfHealingLock() string {
	return fmt.Sprintf("taskmq:{%s}:cron_self_healing_lock", k.queue)
}

func (k QueueKeys) Paused() string {
	return fmt.Sprintf("taskmq:{%s}:paused", k.queue)
}

func (k QueueKeys) Control() string {
	return fmt.Sprintf("taskmq:{%s}:control", k.queue)
}

func (k QueueKeys) Cancelled(taskID string) string {
	return fmt.Sprintf("taskmq:{%s}:cancelled:%s", k.queue, taskID)
}

func (k QueueKeys) CancelChannel() string {
	return fmt.Sprintf("taskmq:{%s}:cancel", k.queue)
}

func (k QueueKeys) DelayedWakeupChannel() string {
	return fmt.Sprintf("taskmq:{%s}:delayed_wakeup", k.queue)
}

// RateLimit returns the GCRA rate-limit key for the queue, optionally scoped by groupKey.
func (k QueueKeys) RateLimit(groupKey string) string {
	if groupKey != "" {
		return fmt.Sprintf("taskmq:{%s}:rate_limit:%s", k.queue, groupKey)
	}
	return fmt.Sprintf("taskmq:{%s}:rate_limit", k.queue)
}

// Meta is the per-task metadata hash (state, errors, optional result).
// Cluster-safe under the same {queue} hash tag as other queue keys.
func (k QueueKeys) Meta(taskID string) string {
	return fmt.Sprintf("taskmq:{%s}:meta:%s", k.queue, taskID)
}

// Completed is a ZSET of completed task IDs scored by completion time (unix ms).
// Used when completed_retention is enabled for TTL/count eviction.
func (k QueueKeys) Completed() string {
	return fmt.Sprintf("taskmq:{%s}:completed", k.queue)
}

// Metrics is a HASH of queue-level counters (processed, failed, retried, …).
func (k QueueKeys) Metrics() string {
	return fmt.Sprintf("taskmq:{%s}:metrics", k.queue)
}

// Heartbeat is the key for a live worker/consumer registration (TTL-refreshed).
func (k QueueKeys) Heartbeat(consumer string) string {
	return fmt.Sprintf("taskmq:{%s}:heartbeat:%s", k.queue, consumer)
}

// HeartbeatScanPattern matches all heartbeat keys for this queue (SCAN).
func (k QueueKeys) HeartbeatScanPattern() string {
	return fmt.Sprintf("taskmq:{%s}:heartbeat:*", k.queue)
}

// Events is a Redis Stream of queue lifecycle events (completed/failed/retry/…).
// Trimmed with approximate MAXLEN by the publisher.
func (k QueueKeys) Events() string {
	return fmt.Sprintf("taskmq:{%s}:events", k.queue)
}

// StreamScanPattern is the Redis KEYS/SCAN pattern that matches all queue stream keys.
func StreamScanPattern() string {
	return "taskmq:{*}:queue"
}

// ParseQueueFromStreamKey extracts the queue name from a stream key produced by Stream().
// Returns ok=false when the key does not match the stream key shape.
func ParseQueueFromStreamKey(redisKey string) (queue string, ok bool) {
	const prefix = "taskmq:{"
	const suffix = "}:queue"
	if !strings.HasPrefix(redisKey, prefix) || !strings.HasSuffix(redisKey, suffix) {
		return "", false
	}
	// Reject empty queue names: "taskmq:{}:queue"
	name := redisKey[len(prefix) : len(redisKey)-len(suffix)]
	if name == "" {
		return "", false
	}
	return name, true
}
