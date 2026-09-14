package codec_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// FuzzBinaryCodec_Unmarshal verifies that BinaryCodec.Unmarshal never panics
// on arbitrary, potentially malicious or truncated byte inputs.
func FuzzBinaryCodec_Unmarshal(f *testing.F) {
	c := codec.BinaryCodec{}

	// 1. Seed corpus
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x00, 0x01})
	f.Add([]byte{0x00, 0x05, 'a', 'b'})

	// Valid sample task bytes
	validBytes, err := c.Marshal(sampleTask())
	if err == nil {
		f.Add(validBytes)
	}

	// Minimal task bytes
	minBytes, err := c.Marshal(taskmodel.NewTask("task.min", []byte("data"), taskmodel.TaskOptions{ID: "m1", Queue: "default"}))
	if err == nil {
		f.Add(minBytes)
	}

	// Malicious/Extreme length simulation
	bomb := make([]byte, 16)
	binary.BigEndian.PutUint32(bomb[0:4], 0xFFFFFFFF)
	f.Add(bomb)

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("BinaryCodec.Unmarshal panicked on input (len %d): %v", len(data), r)
			}
		}()

		var dst taskmodel.Task
		_ = c.Unmarshal(data, &dst)
	})
}

// FuzzBinaryCodec_RoundTrip verifies Dmitry Vyukov's 4-step canonical roundtrip property:
// Task -> Marshal -> Unmarshal -> Marshal -> Unmarshal
// asserting bit-exact serializations and identical reconstructed state.
func FuzzBinaryCodec_RoundTrip(f *testing.F) {
	c := codec.BinaryCodec{}

	// Seed corpus
	f.Add("id-100", "q-high", "send:email", []byte("payload data"), int32(1), int32(5), int64(3000), "uk-100", int64(60000), "some err", "*/5 * * * *", int64(1700000000000), int32(1), "grp-1", int64(1700005000000))
	f.Add("", "", "", []byte{}, int32(0), int32(0), int64(0), "", int64(0), "", "", int64(0), int32(0), "", int64(0))
	f.Add("unicode-编号", "队列_1", "任务:执行", []byte(`{"key":"值"}`), int32(-1), int32(100), int64(999999), "唯一键-1", int64(123456), "报错：超时", "@every 1m", int64(-1000), int32(2), "分组A", int64(-500))

	f.Fuzz(func(t *testing.T,
		id, queue, name string,
		payload []byte,
		retry, maxRetry int32,
		timeoutMs int64,
		uniqueKey string,
		uniqueTTLMs int64,
		lastError, cronSpec string,
		createdAtNano int64,
		uniqueScope int32,
		groupKey string,
		deadlineMs int64,
	) {
		// uint16 string lengths in BinaryCodec have a maximum capacity of 65535 bytes
		if len(id) > 65535 || len(queue) > 65535 || len(name) > 65535 || len(uniqueKey) > 65535 || len(cronSpec) > 65535 || len(groupKey) > 65535 {
			return
		}

		orig := &taskmodel.Task{
			ID:          id,
			Queue:       queue,
			Name:        name,
			Payload:     payload,
			Retry:       int(retry),
			MaxRetry:    int(maxRetry),
			TimeoutMs:   int(timeoutMs),
			UniqueKey:   uniqueKey,
			UniqueTTLMs: int(uniqueTTLMs),
			LastError:   lastError,
			CronSpec:    cronSpec,
			CreatedAt:   time.Unix(0, createdAtNano),
			UniqueScope: taskmodel.UniqueScope(uniqueScope),
			GroupKey:    groupKey,
			DeadlineMs:  deadlineMs,
		}

		// Step 1: Marshal
		b1, err := c.Marshal(orig)
		require.NoError(t, err)
		require.NotEmpty(t, b1)

		// Step 2: Unmarshal
		var t1 taskmodel.Task
		err = c.Unmarshal(b1, &t1)
		require.NoError(t, err)

		// Verify fields match
		require.Equal(t, orig.ID, t1.ID)
		require.Equal(t, orig.Queue, t1.Queue)
		require.Equal(t, orig.Name, t1.Name)
		if len(orig.Payload) == 0 {
			require.Empty(t, t1.Payload)
		} else {
			require.Equal(t, orig.Payload, t1.Payload)
		}
		require.Equal(t, orig.Retry, t1.Retry)
		require.Equal(t, orig.MaxRetry, t1.MaxRetry)
		require.Equal(t, orig.TimeoutMs, t1.TimeoutMs)
		require.Equal(t, orig.UniqueKey, t1.UniqueKey)
		require.Equal(t, orig.UniqueTTLMs, t1.UniqueTTLMs)
		require.Equal(t, orig.LastError, t1.LastError)
		require.Equal(t, orig.CronSpec, t1.CronSpec)
		require.Equal(t, orig.CreatedAt.UnixNano(), t1.CreatedAt.UnixNano())
		require.Equal(t, orig.UniqueScope, t1.UniqueScope)
		require.Equal(t, orig.GroupKey, t1.GroupKey)
		require.Equal(t, orig.DeadlineMs, t1.DeadlineMs)

		// Step 3: Re-Marshal (Canonical Form / Idempotence)
		b2, err := c.Marshal(&t1)
		require.NoError(t, err)
		require.Equal(t, b1, b2, "Re-marshaled bytes must be bit-exact identical to original marshaled bytes")

		// Step 4: Re-Unmarshal
		var t2 taskmodel.Task
		err = c.Unmarshal(b2, &t2)
		require.NoError(t, err)
		require.Equal(t, t1.ID, t2.ID)
		require.Equal(t, t1.Queue, t2.Queue)
	})
}

// FuzzInspectJSONField tests streaming JSON token extraction on arbitrary payloads,
// and applies Differential Fuzzing against standard library encoding/json as the Oracle.
func FuzzInspectJSONField(f *testing.F) {
	// Seed corpus
	f.Add([]byte(`{"n":42,"s":"hi","nested":{"x":1}}`), "n")
	f.Add([]byte(`{"n":42,"s":"hi","nested":{"x":1}}`), "s")
	f.Add([]byte(`{"n":42,"s":"hi","nested":{"x":1}}`), "missing")
	f.Add([]byte(`{"b":true,"f":3.1415926,"str":"hello world"}`), "b")
	f.Add([]byte(`{"b":true,"f":3.1415926,"str":"hello world"}`), "str")
	f.Add([]byte(`{"b":true,"f":3.1415926,"str":"hello world"}`), "f")
	f.Add([]byte(`{"arr":[1,2,3],"obj":{"inner":true}}`), "arr")
	f.Add([]byte(`{"arr":[1,2,3],"obj":{"inner":true}}`), "obj")
	f.Add([]byte(`[1,2,3]`), "item")
	f.Add([]byte(`not json at all`), "any")
	f.Add([]byte(`{"unclosed": "val`), "unclosed")
	f.Add([]byte{}, "")
	f.Add([]byte(`{"k":""}`), "k")
	f.Add([]byte(`{"":"empty_key"}`), "")

	f.Fuzz(func(t *testing.T, payload []byte, field string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("InspectJSONField panicked on field %q, payload: %q: %v", field, string(payload), r)
			}
		}()

		got, err := codec.InspectJSONField(payload, field)

		// Differential Oracle:
		// If payload is a valid JSON object, compare with json.Unmarshal
		if err == nil && len(payload) > 0 && field != "" && payload[0] == '{' {
			var obj map[string]interface{}
			if uerr := json.Unmarshal(payload, &obj); uerr == nil {
				if val, exists := obj[field]; exists {
					switch val.(type) {
					case string, float64, bool:
						expected := fmt.Sprintf("%v", val)
						require.Equal(t, expected, got, "Differential mismatch between InspectJSONField and json.Unmarshal")
					}
				}
			}
		}
	})
}

// FuzzUnsafeStringToBytes verifies zero-copy conversion safety on arbitrary strings.
func FuzzUnsafeStringToBytes(f *testing.F) {
	f.Add("")
	f.Add("hello")
	f.Add("TaskMQ high throughput distributed queue")
	f.Add("测试中文字符与Emoji 🚀 🔥")
	f.Add(string(make([]byte, 1024)))

	f.Fuzz(func(t *testing.T, s string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("UnsafeStringToBytes panicked on input %q: %v", s, r)
			}
		}()

		b := codec.UnsafeStringToBytes(s)
		if s == "" {
			require.Nil(t, b)
		} else {
			require.Equal(t, len(s), len(b))
			require.Equal(t, s, string(b))
		}
	})
}
