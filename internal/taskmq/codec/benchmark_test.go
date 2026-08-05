package codec_test

import (
	"testing"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/codec"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func benchTask() *taskmodel.Task {
	t := taskmodel.NewTask("bench.job", []byte(`{"n":42,"s":"hello-world-payload"}`), taskmodel.TaskOptions{
		ID: "bench-id-1", Queue: "bench-q", MaxRetry: taskmodel.Ptr(5),
	})
	t.TimeoutMs = 30000
	t.UniqueKey = "uk"
	t.UniqueTTLMs = 60000
	t.GroupKey = "g"
	t.DeadlineMs = time.Now().Add(time.Hour).UnixMilli()
	t.CreatedAt = time.Now()
	return t
}

func BenchmarkJSONCodec_Marshal(b *testing.B) {
	c := codec.JSONCodec{}
	t := benchTask()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.Marshal(t); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONCodec_Unmarshal(b *testing.B) {
	c := codec.JSONCodec{}
	raw, err := c.Marshal(benchTask())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var dst taskmodel.Task
		if err := c.Unmarshal(raw, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBinaryCodec_Marshal(b *testing.B) {
	c := codec.BinaryCodec{}
	t := benchTask()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.Marshal(t); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBinaryCodec_Unmarshal(b *testing.B) {
	c := codec.BinaryCodec{}
	raw, err := c.Marshal(benchTask())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var dst taskmodel.Task
		if err := c.Unmarshal(raw, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONCodec_InspectField(b *testing.B) {
	c := codec.JSONCodec{}
	payload := []byte(`{"n":42,"s":"x","nested":true}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.InspectField(payload, "n"); err != nil {
			b.Fatal(err)
		}
	}
}
