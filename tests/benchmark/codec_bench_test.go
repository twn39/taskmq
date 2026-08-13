package benchmark

import (
	"bytes"
	"testing"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/codec"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func createSampleTask(payloadSize int) *taskmodel.Task {
	payload := bytes.Repeat([]byte("a"), payloadSize)
	return &taskmodel.Task{
		ID:          "task-bench-1002",
		Queue:       "benchmark-queue",
		Name:        "task:send:email",
		Payload:     payload,
		Retry:       2,
		MaxRetry:    5,
		TimeoutMs:   12000,
		UniqueKey:   "uniq-bench-key-99",
		UniqueTTLMs: 3600000,
		LastError:   "connection reset by peer, retrying",
		CronSpec:    "*/5 * * * *",
		GroupKey:    "group-email-service",
		CreatedAt:   time.Unix(1712345678, 0),
	}
}

func BenchmarkCodec_Marshal_JSON(b *testing.B) {
	task := createSampleTask(1024) // 1KB payload
	jsonCodec := codec.JSONCodec{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := jsonCodec.Marshal(task)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodec_Marshal_Binary(b *testing.B) {
	task := createSampleTask(1024) // 1KB payload
	binCodec := codec.BinaryCodec{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := binCodec.Marshal(task)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodec_Unmarshal_JSON(b *testing.B) {
	task := createSampleTask(1024)
	jsonCodec := codec.JSONCodec{}
	data, _ := jsonCodec.Marshal(task)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded taskmodel.Task
		err := jsonCodec.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodec_Unmarshal_Binary(b *testing.B) {
	task := createSampleTask(1024)
	binCodec := codec.BinaryCodec{}
	data, _ := binCodec.Marshal(task)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded taskmodel.Task
		err := binCodec.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
