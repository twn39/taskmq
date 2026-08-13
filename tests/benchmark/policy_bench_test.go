package benchmark

import (
	"testing"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func BenchmarkPolicy_ExponentialBackoff(b *testing.B) {
	p := policy.NewExponentialBackoff(1*time.Second, 30*time.Second, false)
	task := &taskmodel.Task{Retry: 3, MaxRetry: 5}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.NextBackoff(task)
	}
}

func BenchmarkPolicy_ParallelBackoff(b *testing.B) {
	p := policy.NewExponentialBackoff(1*time.Second, 30*time.Second, false)
	task := &taskmodel.Task{Retry: 3, MaxRetry: 5}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = p.NextBackoff(task)
		}
	})
}
