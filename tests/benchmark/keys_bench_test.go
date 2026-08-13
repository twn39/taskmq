package benchmark

import (
	"testing"

	"github.com/twn39/taskmq/internal/taskmq/keys"
)

func BenchmarkKeys_KeysFor(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qk := keys.KeysFor("orders-queue")
		_ = qk.Stream()
		_ = qk.Delayed()
		_ = qk.DLQ()
		_ = qk.Unique("key-12345")
		_ = qk.Meta("task-67890")
	}
}

func BenchmarkKeys_ParseQueueFromStreamKey(b *testing.B) {
	streamKey := keys.KeysFor("orders-queue").Stream()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = keys.ParseQueueFromStreamKey(streamKey)
	}
}
