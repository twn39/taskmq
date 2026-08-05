package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewConfig_Defaults(t *testing.T) {
	// Isolate from ambient APP_ENV / cwd config files.
	t.Setenv("APP_ENV", "")
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	cfg, err := NewConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != ":8080" {
		t.Fatalf("port: %s", cfg.Server.Port)
	}
	if cfg.Server.GRPCPort != ":50051" {
		t.Fatalf("grpc: %s", cfg.Server.GRPCPort)
	}
	if cfg.Redis.Mode != "standalone" {
		t.Fatalf("mode: %s", cfg.Redis.Mode)
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Fatalf("addr: %s", cfg.Redis.Addr)
	}
	if cfg.TaskMQ.Codec != "binary" {
		t.Fatalf("codec: %s", cfg.TaskMQ.Codec)
	}
	if cfg.TaskMQ.DefaultUniqueTTL != time.Hour {
		t.Fatalf("unique ttl: %v", cfg.TaskMQ.DefaultUniqueTTL)
	}
	if cfg.TaskMQ.EventsMaxLen != 0 {
		t.Fatalf("events max len default should be 0: %d", cfg.TaskMQ.EventsMaxLen)
	}
	if cfg.TaskMQ.Lifecycle.CancelledTTL != 24*time.Hour {
		t.Fatalf("cancelled ttl: %v", cfg.TaskMQ.Lifecycle.CancelledTTL)
	}
	if len(cfg.TaskMQ.Queues) == 0 {
		t.Fatal("expected default queues")
	}
}

func TestNewConfig_EnvOverride(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("TASKMQ_SERVER_PORT", ":9090")
	t.Setenv("TASKMQ_REDIS_ADDR", "127.0.0.1:6380")
	t.Setenv("TASKMQ_TASKMQ_EVENTS_MAX_LEN", "5000")
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	cfg, err := NewConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != ":9090" {
		t.Fatalf("port: %s", cfg.Server.Port)
	}
	if cfg.Redis.Addr != "127.0.0.1:6380" {
		t.Fatalf("addr: %s", cfg.Redis.Addr)
	}
	if cfg.TaskMQ.EventsMaxLen != 5000 {
		t.Fatalf("events_max_len: %d", cfg.TaskMQ.EventsMaxLen)
	}
}

func TestNewConfig_YAMLFile(t *testing.T) {
	t.Setenv("APP_ENV", "")
	// Clear env overrides from other tests in same process.
	for _, k := range []string{
		"TASKMQ_SERVER_PORT", "TASKMQ_REDIS_ADDR", "TASKMQ_TASKMQ_EVENTS_MAX_LEN",
	} {
		_ = os.Unsetenv(k)
	}
	dir := t.TempDir()
	yaml := []byte(`
server:
  port: ":7070"
taskmq:
  events_max_len: 2500
  completed_retention: 1h
  lifecycle:
    enqueue_hard_limit: 100
`)
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), yaml, 0o644); err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	cfg, err := NewConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != ":7070" {
		t.Fatalf("port: %s", cfg.Server.Port)
	}
	if cfg.TaskMQ.EventsMaxLen != 2500 {
		t.Fatalf("events_max_len: %d", cfg.TaskMQ.EventsMaxLen)
	}
	if cfg.TaskMQ.CompletedRetention != time.Hour {
		t.Fatalf("retention: %v", cfg.TaskMQ.CompletedRetention)
	}
	if cfg.TaskMQ.Lifecycle.EnqueueHardLimit != 100 {
		t.Fatalf("hard limit: %d", cfg.TaskMQ.Lifecycle.EnqueueHardLimit)
	}
}
