package unit

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/taskmq"
)

func TestCodecs(t *testing.T) {
	task := &taskmq.Task{
		ID:          "test-id-123",
		Queue:       "my-priority-queue",
		Name:        "task:send:email",
		Payload:     []byte("hello world this is a test payload for binary codec"),
		Retry:       2,
		MaxRetry:    5,
		TimeoutMs:   12000,
		UniqueKey:   "unique-email-123",
		UniqueTTLMs: 3600000,
		LastError:   "smtp server timeout, retrying later",
		CronSpec:    "*/5 * * * *",
		CreatedAt:   time.Unix(1712345678, 999000000),
	}

	codecs := []struct {
		name  string
		codec taskmq.Codec
	}{
		{"JSONCodec", taskmq.JSONCodec{}},
		{"BinaryCodec", taskmq.BinaryCodec{}},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.codec.Marshal(task)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			var decoded taskmq.Task
			err = tc.codec.Unmarshal(data, &decoded)
			assert.NoError(t, err)

			assert.Equal(t, task.ID, decoded.ID)
			assert.Equal(t, task.Queue, decoded.Queue)
			assert.Equal(t, task.Name, decoded.Name)
			assert.True(t, bytes.Equal(task.Payload, decoded.Payload))
			assert.Equal(t, task.Retry, decoded.Retry)
			assert.Equal(t, task.MaxRetry, decoded.MaxRetry)
			assert.Equal(t, task.TimeoutMs, decoded.TimeoutMs)
			assert.Equal(t, task.UniqueKey, decoded.UniqueKey)
			assert.Equal(t, task.UniqueTTLMs, decoded.UniqueTTLMs)
			assert.Equal(t, task.LastError, decoded.LastError)
			assert.Equal(t, task.CronSpec, decoded.CronSpec)
			assert.True(t, task.CreatedAt.Equal(decoded.CreatedAt))
		})
	}
}

func TestBinaryCodec_ErrorCases(t *testing.T) {
	codec := taskmq.BinaryCodec{}
	var decoded taskmq.Task

	// Too short data
	err := codec.Unmarshal([]byte{1}, &decoded)
	assert.Error(t, err)

	// Out of bounds data
	err = codec.Unmarshal([]byte{0, 5, 'a', 'b'}, &decoded)
	assert.Error(t, err)
}
