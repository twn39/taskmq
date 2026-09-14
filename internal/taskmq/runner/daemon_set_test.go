package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"go.uber.org/zap"
)

type mockRunner struct {
	started atomic.Bool
	stopped atomic.Bool
	err     error
}

func (m *mockRunner) Run(ctx context.Context) error {
	m.started.Store(true)
	defer m.stopped.Store(true)
	if m.err != nil {
		return m.err
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestDaemonSet_RunAndStop(t *testing.T) {
	ds := NewDaemonSet(zap.NewNop())
	r1 := &mockRunner{}
	r2 := &mockRunner{}

	ds.Add("r1", r1)
	ds.Add("r2", r2)

	runners := ds.Runners()
	if len(runners) != 2 {
		t.Fatalf("expected 2 runners, got %d", len(runners))
	}

	ctx, cancel := context.WithCancel(context.Background())

	runDone := make(chan error, 1)
	go func() {
		runDone <- ds.Run(ctx)
	}()

	// Wait for start
	time.Sleep(50 * time.Millisecond)
	if !r1.started.Load() || !r2.started.Load() {
		t.Fatal("expected runners to start")
	}

	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for daemonset to exit")
	}

	if !r1.stopped.Load() || !r2.stopped.Load() {
		t.Fatal("expected runners to stop")
	}
}

func TestDaemonSet_Empty(t *testing.T) {
	ds := NewDaemonSet(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := ds.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDaemonSet_RunnerError(t *testing.T) {
	ds := NewDaemonSet(zap.NewNop())
	expectedErr := errors.New("boom")
	r := &mockRunner{err: expectedErr}
	ds.Add("fail-runner", r)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := ds.Run(ctx)
	if err == nil || !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestBuildQueueDaemons(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	logger := zap.NewNop()
	c := codec.JSONCodec{}
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())

	// 1. All enabled
	ds := NewDaemonSet(logger)
	BuildQueueDaemons(ds, rdb, logger, c, lc, QueueDaemonConfig{
		Queue: "q1",
	})
	if len(ds.Runners()) != 4 {
		t.Fatalf("expected 4 daemons, got %d", len(ds.Runners()))
	}

	// 2. Selective disable
	ds2 := NewDaemonSet(logger)
	BuildQueueDaemons(ds2, rdb, logger, c, lc, QueueDaemonConfig{
		Queue:            "q2",
		DisableScheduler: true,
		DisableJanitor:   true,
	})
	runners2 := ds2.Runners()
	if len(runners2) != 2 {
		t.Fatalf("expected 2 daemons, got %d", len(runners2))
	}
	if _, ok := runners2["q2:scheduler"]; ok {
		t.Error("scheduler should be disabled")
	}
	if _, ok := runners2["q2:janitor"]; ok {
		t.Error("janitor should be disabled")
	}
	if _, ok := runners2["q2:cron"]; !ok {
		t.Error("cron should be enabled")
	}
	if _, ok := runners2["q2:retention"]; !ok {
		t.Error("retention should be enabled")
	}
}
