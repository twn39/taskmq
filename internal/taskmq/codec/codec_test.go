package codec_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func sampleTask() *taskmodel.Task {
	t := taskmodel.NewTask("job.do", []byte(`{"n":42,"s":"x"}`), taskmodel.TaskOptions{
		ID:       "id-1",
		Queue:    "q1",
		MaxRetry: taskmodel.Ptr(3),
	})
	t.Retry = 1
	t.TimeoutMs = 5000
	t.UniqueKey = "uk"
	t.UniqueTTLMs = 60000
	t.UniqueScope = taskmodel.UniqueUntilSucceeded
	t.GroupKey = "g1"
	t.DeadlineMs = time.Now().Add(time.Hour).UnixMilli()
	t.LastError = "prev"
	t.CronSpec = "*/5 * * * *"
	t.CreatedAt = time.Unix(0, 1_700_000_000_000_000_000)
	return t
}

func assertTaskEqual(t *testing.T, want, got *taskmodel.Task) {
	t.Helper()
	require.Equal(t, want.ID, got.ID)
	require.Equal(t, want.Queue, got.Queue)
	require.Equal(t, want.Name, got.Name)
	require.Equal(t, want.Payload, got.Payload)
	require.Equal(t, want.Retry, got.Retry)
	require.Equal(t, want.MaxRetry, got.MaxRetry)
	require.Equal(t, want.TimeoutMs, got.TimeoutMs)
	require.Equal(t, want.UniqueKey, got.UniqueKey)
	require.Equal(t, want.UniqueTTLMs, got.UniqueTTLMs)
	require.Equal(t, want.UniqueScope, got.UniqueScope)
	require.Equal(t, want.GroupKey, got.GroupKey)
	require.Equal(t, want.DeadlineMs, got.DeadlineMs)
	require.Equal(t, want.LastError, got.LastError)
	require.Equal(t, want.CronSpec, got.CronSpec)
	require.Equal(t, want.CreatedAt.UnixNano(), got.CreatedAt.UnixNano())
}

func TestJSONCodec_RoundTrip(t *testing.T) {
	c := codec.JSONCodec{}
	src := sampleTask()
	b, err := c.Marshal(src)
	require.NoError(t, err)
	require.NotEmpty(t, b)

	var dst taskmodel.Task
	require.NoError(t, c.Unmarshal(b, &dst))
	assertTaskEqual(t, src, &dst)
}

func TestJSONCodec_UnmarshalCorrupt(t *testing.T) {
	c := codec.JSONCodec{}
	var dst taskmodel.Task
	require.Error(t, c.Unmarshal([]byte(`not-json`), &dst))
	require.Error(t, c.Unmarshal([]byte(``), &dst))
}

func TestBinaryCodec_RoundTrip(t *testing.T) {
	c := codec.BinaryCodec{}
	src := sampleTask()
	b, err := c.Marshal(src)
	require.NoError(t, err)
	require.NotEmpty(t, b)

	var dst taskmodel.Task
	require.NoError(t, c.Unmarshal(b, &dst))
	assertTaskEqual(t, src, &dst)
}

func TestBinaryCodec_EmptyAndMinimal(t *testing.T) {
	c := codec.BinaryCodec{}
	src := taskmodel.NewTask("n", nil, taskmodel.TaskOptions{ID: "a", Queue: "q"})
	b, err := c.Marshal(src)
	require.NoError(t, err)

	var dst taskmodel.Task
	require.NoError(t, c.Unmarshal(b, &dst))
	require.Equal(t, "a", dst.ID)
	require.Equal(t, "q", dst.Queue)
	require.Equal(t, "n", dst.Name)
	require.Empty(t, dst.Payload)
}

func TestBinaryCodec_UnmarshalTooShort(t *testing.T) {
	c := codec.BinaryCodec{}
	var dst taskmodel.Task
	require.Error(t, c.Unmarshal(nil, &dst))
	require.Error(t, c.Unmarshal([]byte{0x00}, &dst))
	// Length claims more bytes than available.
	require.Error(t, c.Unmarshal([]byte{0x00, 0x05, 0x61}, &dst))
}

func TestUnsafeStringToBytes(t *testing.T) {
	require.Nil(t, codec.UnsafeStringToBytes(""))
	b := codec.UnsafeStringToBytes("hello")
	require.Equal(t, []byte("hello"), b)
}

func TestInspectJSONField(t *testing.T) {
	payload := []byte(`{"n":42,"s":"hi","nested":{"x":1}}`)
	v, err := codec.InspectJSONField(payload, "n")
	require.NoError(t, err)
	require.Equal(t, "42", v)

	v, err = codec.InspectJSONField(payload, "s")
	require.NoError(t, err)
	require.Equal(t, "hi", v)

	_, err = codec.InspectJSONField(payload, "missing")
	require.Error(t, err)

	v, err = codec.InspectJSONField(nil, "n")
	require.NoError(t, err)
	require.Equal(t, "", v)

	_, err = codec.InspectJSONField([]byte(`[1,2]`), "n")
	require.Error(t, err)
}

func TestJSONCodec_InspectField(t *testing.T) {
	c := codec.JSONCodec{}
	v, err := c.InspectField([]byte(`{"a":true}`), "a")
	require.NoError(t, err)
	require.Equal(t, "true", v)
}

func TestBinaryCodec_InspectField(t *testing.T) {
	c := codec.BinaryCodec{}
	// JSON-shaped payload still inspectable.
	v, err := c.InspectField([]byte(`{"k":"v"}`), "k")
	require.NoError(t, err)
	require.Equal(t, "v", v)

	// True binary body cannot be inspected without a schema.
	src := sampleTask()
	b, err := c.Marshal(src)
	require.NoError(t, err)
	_, err = c.InspectField(b, "n")
	require.Error(t, err)

	v, err = c.InspectField(nil, "k")
	require.NoError(t, err)
	require.Equal(t, "", v)
}
