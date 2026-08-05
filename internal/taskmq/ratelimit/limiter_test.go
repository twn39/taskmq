package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/ratelimit"
)

func setupLimiter(t *testing.T) (context.Context, *ratelimit.GCRALimiter, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return context.Background(), ratelimit.NewGCRALimiter(rdb), func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func TestGCRA_DisabledWhenMaxOrDurationZero(t *testing.T) {
	ctx, lim, cleanup := setupLimiter(t)
	defer cleanup()

	wait, err := lim.TryConsume(ctx, "k", 0, time.Second)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), wait)

	wait, err = lim.Check(ctx, "k", 10, 0)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), wait)
}

func TestGCRA_TryConsume_AllowsBurstThenBlocks(t *testing.T) {
	ctx, lim, cleanup := setupLimiter(t)
	defer cleanup()

	// max=2 per second → burst = max-1 = 1 free token after first? Actually:
	// emission = duration/max = 0.5s; burst = 1.
	// First TryConsume always allowed (tat starts at now); second may be allowed within burst.
	key := "rl:burst"
	max := int64(2)
	window := time.Second

	// Consume until limited.
	var limited int
	for i := 0; i < 10; i++ {
		wait, err := lim.TryConsume(ctx, key, max, window)
		require.NoError(t, err)
		if wait > 0 {
			limited++
			require.Greater(t, wait, time.Duration(0))
			// Check (non-consuming) should also report limited.
			cw, err := lim.Check(ctx, key, max, window)
			require.NoError(t, err)
			require.Greater(t, cw, time.Duration(0))
			break
		}
	}
	require.Equal(t, 1, limited, "expected to hit rate limit within burst window")
}

func TestGCRA_GroupKeysAreIsolated(t *testing.T) {
	ctx, lim, cleanup := setupLimiter(t)
	defer cleanup()

	max := int64(1)
	window := time.Second

	// Exhaust group A.
	wait, err := lim.TryConsume(ctx, "rl:group-a", max, window)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), wait)
	wait, err = lim.TryConsume(ctx, "rl:group-a", max, window)
	require.NoError(t, err)
	require.Greater(t, wait, time.Duration(0), "group A should be limited")

	// Group B still free.
	wait, err = lim.TryConsume(ctx, "rl:group-b", max, window)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), wait)
}

func TestGCRA_CheckDoesNotConsume(t *testing.T) {
	ctx, lim, cleanup := setupLimiter(t)
	defer cleanup()

	key := "rl:check-only"
	max := int64(1)
	window := 5 * time.Second

	// Pure checks never mutate TAT → always allowed when unused.
	for i := 0; i < 5; i++ {
		wait, err := lim.Check(ctx, key, max, window)
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), wait)
	}
	// One consume.
	wait, err := lim.TryConsume(ctx, key, max, window)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), wait)
	// Subsequent checks see the limit without consuming further.
	wait, err = lim.Check(ctx, key, max, window)
	require.NoError(t, err)
	require.Greater(t, wait, time.Duration(0))
}
