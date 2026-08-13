package benchmark

import (
	"testing"
	"time"

	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func BenchmarkTask_NewTask_Default(b *testing.B) {
	payload := []byte(`{"user_id":12345}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = taskmodel.NewTask("benchmark:task", payload)
	}
}

func BenchmarkTask_NewTask_WithOptions(b *testing.B) {
	payload := []byte(`{"user_id":12345}`)
	deadline := time.Now().Add(time.Hour)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = taskmodel.NewTask("benchmark:task", payload, taskmodel.TaskOptions{
			ID:        "task-bench-99",
			Queue:     "high-priority",
			MaxRetry:  taskmodel.Ptr(5),
			Timeout:   15 * time.Second,
			UniqueKey: "unique-user-12345",
			UniqueTTL: 60 * time.Second,
			GroupKey:  "group-a",
			Deadline:  deadline,
		})
	}
}

func BenchmarkTask_EffectiveTimeout(b *testing.B) {
	now := time.Now()
	task := &taskmodel.Task{
		TimeoutMs:  30000,
		DeadlineMs: now.Add(10 * time.Minute).UnixMilli(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = task.EffectiveTimeout(now)
	}
}
