package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func BenchmarkClient_Enqueue(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}), WithClientLifecycle(lc))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := taskmodel.NewTask("bench", []byte(`{"n":1}`), taskmodel.TaskOptions{
			ID: fmt.Sprintf("e-%d", i), Queue: "bench-enq",
		})
		if err := c.Enqueue(ctx, task); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClient_EnqueueBulk(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	c := NewClient(rdb, WithClientCodec(codec.JSONCodec{}))
	ctx := context.Background()

	const batch = 50
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tasks := make([]*taskmodel.Task, batch)
		for j := 0; j < batch; j++ {
			tasks[j] = taskmodel.NewTask("bench", []byte(`{}`), taskmodel.TaskOptions{
				ID: fmt.Sprintf("b-%d-%d", i, j), Queue: "bench-bulk",
			})
		}
		if _, err := c.EnqueueBulk(ctx, tasks); err != nil {
			b.Fatal(err)
		}
	}
}
