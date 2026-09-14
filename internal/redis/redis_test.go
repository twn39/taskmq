package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func TestBuildUniversalOptions_Standalone(t *testing.T) {
	opts, mode, err := buildUniversalOptions(config.RedisConfig{
		Mode: "standalone",
		Addr: "localhost:6379",
		DB:   2,
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "standalone" {
		t.Fatalf("mode=%s", mode)
	}
	if len(opts.Addrs) != 1 || opts.Addrs[0] != "localhost:6379" {
		t.Fatalf("addrs=%v", opts.Addrs)
	}
	if opts.DB != 2 {
		t.Fatalf("db=%d", opts.DB)
	}
}

func TestBuildUniversalOptions_Cluster(t *testing.T) {
	opts, mode, err := buildUniversalOptions(config.RedisConfig{
		Mode:  "cluster",
		Addrs: []string{"a:7000", "b:7001"},
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "cluster" {
		t.Fatalf("mode=%s", mode)
	}
	if len(opts.Addrs) != 2 {
		t.Fatalf("addrs=%v", opts.Addrs)
	}
	if opts.MasterName != "" {
		t.Fatal("cluster must not set master name")
	}
}

func TestBuildUniversalOptions_DefaultAndFallback(t *testing.T) {
	// Mode empty defaults to standalone
	opts, mode, err := buildUniversalOptions(config.RedisConfig{
		Mode: "",
		Addr: "127.0.0.1:6379",
	}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "standalone" {
		t.Fatalf("expected mode standalone, got %s", mode)
	}
	if len(opts.Addrs) != 1 || opts.Addrs[0] != "127.0.0.1:6379" {
		t.Fatalf("unexpected addrs: %v", opts.Addrs)
	}

	// Addrs takes precedence over Addr
	opts, _, err = buildUniversalOptions(config.RedisConfig{
		Mode:  "standalone",
		Addr:  "127.0.0.1:6379",
		Addrs: []string{"127.0.0.1:6380"},
	}, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts.Addrs) != 1 || opts.Addrs[0] != "127.0.0.1:6380" {
		t.Fatalf("expected 127.0.0.1:6380, got %v", opts.Addrs)
	}
}

func TestBuildUniversalOptions_ErrorCases(t *testing.T) {
	// Standalone without addr or addrs
	_, _, err := buildUniversalOptions(config.RedisConfig{Mode: "standalone"}, 10)
	if err == nil {
		t.Fatal("expected error for standalone mode without addr")
	}

	// Cluster without addrs
	_, _, err = buildUniversalOptions(config.RedisConfig{Mode: "cluster"}, 10)
	if err == nil {
		t.Fatal("expected error for cluster mode without addrs")
	}

	// Sentinel without addrs
	_, _, err = buildUniversalOptions(config.RedisConfig{
		Mode:       "sentinel",
		MasterName: "mymaster",
	}, 10)
	if err == nil {
		t.Fatal("expected error for sentinel mode without addrs")
	}

	// Sentinel without master name
	_, _, err = buildUniversalOptions(config.RedisConfig{
		Mode:  "sentinel",
		Addrs: []string{"127.0.0.1:26379"},
	}, 10)
	if err == nil {
		t.Fatal("expected error for sentinel mode without master_name")
	}

	// Sentinel success
	opts, mode, err := buildUniversalOptions(config.RedisConfig{
		Mode:       "sentinel",
		MasterName: "mymaster",
		Addrs:      []string{"127.0.0.1:26379"},
		Password:   "secret",
		DB:         3,
	}, 10)
	if err != nil {
		t.Fatalf("unexpected error for sentinel: %v", err)
	}
	if mode != "sentinel" {
		t.Fatalf("expected mode sentinel, got %s", mode)
	}
	if opts.MasterName != "mymaster" || opts.DB != 3 || opts.Password != "secret" {
		t.Fatalf("unexpected sentinel options: %+v", opts)
	}

	// Unknown mode
	_, _, err = buildUniversalOptions(config.RedisConfig{Mode: "invalid-mode"}, 10)
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

func TestNewRedisClient_Standalone(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	cfg := &config.Config{
		Redis: config.RedisConfig{
			Mode:     "standalone",
			Addr:     mr.Addr(),
			PoolSize: 20,
		},
	}

	app := fx.New(
		fx.NopLogger,
		fx.Provide(
			func() *config.Config { return cfg },
			zap.NewNop,
			NewRedisClient,
		),
		fx.Invoke(func(rdb redis.UniversalClient) {
			if rdb == nil {
				t.Fatal("expected non-nil redis.UniversalClient")
			}
		}),
	)

	startCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("failed to start fx app with NewRedisClient: %v", err)
	}

	stopCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop fx app: %v", err)
	}
}

func TestNewRedisClient_Errors(t *testing.T) {
	// Invalid config (empty address in standalone)
	cfg := &config.Config{
		Redis: config.RedisConfig{
			Mode: "standalone",
		},
	}
	app := fx.New(
		fx.NopLogger,
		fx.Provide(
			func() *config.Config { return cfg },
			zap.NewNop,
			NewRedisClient,
		),
		fx.Invoke(func(rdb redis.UniversalClient) {}),
	)
	if err := app.Start(context.Background()); err == nil {
		t.Fatal("expected error starting app with empty redis addr")
	}

	// Ping failure (unreachable port)
	cfg2 := &config.Config{
		Redis: config.RedisConfig{
			Mode: "standalone",
			Addr: "127.0.0.1:54321",
		},
	}
	app2 := fx.New(
		fx.NopLogger,
		fx.Provide(
			func() *config.Config { return cfg2 },
			zap.NewNop,
			NewRedisClient,
		),
		fx.Invoke(func(rdb redis.UniversalClient) {}),
	)
	if err := app2.Start(context.Background()); err == nil {
		t.Fatal("expected ping error connecting to closed port")
	}
}

