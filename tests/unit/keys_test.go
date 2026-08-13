package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/twn39/taskmq/internal/taskmq/keys"
)

func TestKeysFor_FormattingAndHashTag(t *testing.T) {
	qk := keys.KeysFor("orders-queue")

	// Ensure all generated Redis keys contain the `{orders-queue}` HashTag for Redis Cluster hash-slot alignment
	assert.Equal(t, "orders-queue", qk.Queue())
	assert.Equal(t, "taskmq:{orders-queue}:queue", qk.Stream())
	assert.Equal(t, "taskmq:{orders-queue}:delayed", qk.Delayed())
	assert.Equal(t, "taskmq:{orders-queue}:dlq", qk.DLQ())
	assert.Equal(t, "taskmq:{orders-queue}:paused", qk.Paused())
	assert.Equal(t, "taskmq:{orders-queue}:control", qk.Control())
	assert.Equal(t, "taskmq:{orders-queue}:unique:key-123", qk.Unique("key-123"))
	assert.Equal(t, "taskmq:{orders-queue}:meta:task-456", qk.Meta("task-456"))
}

func TestKeysFor_StreamScanPattern(t *testing.T) {
	assert.Equal(t, "taskmq:{*}:queue", keys.StreamScanPattern())

	queue, ok := keys.ParseQueueFromStreamKey("taskmq:{orders-queue}:queue")
	assert.True(t, ok)
	assert.Equal(t, "orders-queue", queue)

	_, ok = keys.ParseQueueFromStreamKey("invalid-stream-key")
	assert.False(t, ok)

	_, ok = keys.ParseQueueFromStreamKey("taskmq:{}:queue")
	assert.False(t, ok)
}
