package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/ratelimit"
)

func BenchmarkGCRA_TryConsume(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	lim := ratelimit.NewGCRALimiter(rdb)
	ctx := context.Background()

	// High limit so most ops succeed (measure happy path).
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := lim.TryConsume(ctx, "bench:rl", 1_000_000, time.Second); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGCRA_Check(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	lim := ratelimit.NewGCRALimiter(rdb)
	ctx := context.Background()
	_, _ = lim.TryConsume(ctx, "bench:check", 100, time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := lim.Check(ctx, "bench:check", 100, time.Second); err != nil {
			b.Fatal(err)
		}
	}
}
