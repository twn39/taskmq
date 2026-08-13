package benchmark

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/twn39/taskmq/internal/taskmq/client"
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
	c := client.NewClient(rdb, client.WithClientLifecycle(lc), client.WithClientCodec(codec.BinaryCodec{}))
	ctx := context.Background()

	task := taskmodel.NewTask("bench:job", []byte(`{"data":"sample payload"}`), taskmodel.TaskOptions{Queue: "bench-q"})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c.Enqueue(ctx, task)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClient_EnqueueBulk_1000(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	c := client.NewClient(rdb, client.WithClientLifecycle(lc), client.WithClientCodec(codec.BinaryCodec{}))
	ctx := context.Background()

	tasks := make([]*taskmodel.Task, 1000)
	for i := 0; i < 1000; i++ {
		tasks[i] = taskmodel.NewTask("bench:job", []byte(`{"data":"sample payload"}`), taskmodel.TaskOptions{
			ID:    fmt.Sprintf("bulk-task-%d", i),
			Queue: "bench-bulk-q",
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.EnqueueBulk(ctx, tasks)
		if err != nil {
			b.Fatal(err)
		}
	}
}
