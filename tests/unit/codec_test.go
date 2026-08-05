package unit

import (
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"testing"
	"time"
)

func TestCodecs(t *testing.T) {
	task := &taskmodel.Task{
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
		codec codec.Codec
	}{
		{"JSONCodec", codec.JSONCodec{}},
		{"BinaryCodec", codec.BinaryCodec{}},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.codec.Marshal(task)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			var decoded taskmodel.Task
			err = tc.codec.Unmarshal(data, &decoded)
			assert.NoError(t, err)

			timeComparer := cmp.Comparer(func(x, y time.Time) bool {
				return x.Equal(y)
			})
			if diff := cmp.Diff(task, &decoded, timeComparer); diff != "" {
				t.Errorf("Codec round-trip mismatch (-expected +actual):\n%s", diff)
			}
		})
	}
}

func TestBinaryCodec_ErrorCases(t *testing.T) {
	codec := codec.BinaryCodec{}
	var decoded taskmodel.Task

	// Too short data
	err := codec.Unmarshal([]byte{1}, &decoded)
	assert.Error(t, err)

	// Out of bounds data
	err = codec.Unmarshal([]byte{0, 5, 'a', 'b'}, &decoded)
	assert.Error(t, err)
}
