package redis

import (
	"testing"

	"github.com/twn39/taskmq/internal/config"
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

	// Unknown mode
	_, _, err = buildUniversalOptions(config.RedisConfig{Mode: "invalid-mode"}, 10)
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

