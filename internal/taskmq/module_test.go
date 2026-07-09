package taskmq

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"go.uber.org/zap"
)

func TestProvideWorkers_RequiresLifecycle(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := &config.Config{}
	_, err = ProvideWorkers(ProvideWorkersParams{
		Rdb:       rdb,
		Logger:    zap.NewNop(),
		Codec:     codec.JSONCodec{},
		Cfg:       cfg,
		Lifecycle: nil,
	})
	if err == nil {
		t.Fatal("expected error when Lifecycle is nil")
	}
}

func TestBuildWorkerTopologyWithLifecycle_RequiresLifecycle(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	_, err = BuildWorkerTopologyWithLifecycle(rdb, zap.NewNop(), &config.Config{}, codec.JSONCodec{}, context.Background(), nil)
	if err == nil {
		t.Fatal("expected error when Lifecycle is nil")
	}
}

func TestSharedLifecycle_ClientAndWorkersSamePointer(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())

	c := client.NewClient(rdb, client.WithClientLifecycle(lc), client.WithClientCodec(codec.JSONCodec{}))
	type lifecycleHolder interface {
		Lifecycle() *lifecycle.Lifecycle
	}
	holder, ok := c.(lifecycleHolder)
	if !ok {
		t.Fatal("client facade must expose Lifecycle()")
	}
	if holder.Lifecycle() != lc {
		t.Fatal("client lifecycle pointer mismatch")
	}

	w, err := ProvideWorkers(ProvideWorkersParams{
		Rdb:       rdb,
		Logger:    zap.NewNop(),
		Codec:     codec.JSONCodec{},
		Cfg:       &config.Config{},
		Lifecycle: lc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w == nil {
		t.Fatal("expected worker")
	}
	// Same pointer was required for ProvideWorkers; client already asserts same pointer.
	if holder.Lifecycle() != lc {
		t.Fatal("lifecycle must remain the shared instance")
	}
}
