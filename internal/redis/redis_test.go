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

func TestBuildUniversalOptions_Sentinel(t *testing.T) {
	_, _, err := buildUniversalOptions(config.RedisConfig{
		Mode:  "sentinel",
		Addrs: []string{"s1:26379"},
	}, 10)
	if err == nil {
		t.Fatal("expected error without master_name")
	}
	opts, mode, err := buildUniversalOptions(config.RedisConfig{
		Mode:       "sentinel",
		MasterName: "mymaster",
		Addrs:      []string{"s1:26379"},
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "sentinel" || opts.MasterName != "mymaster" {
		t.Fatalf("mode=%s master=%s", mode, opts.MasterName)
	}
}
