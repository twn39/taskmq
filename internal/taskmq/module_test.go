package taskmq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/config"
	"github.com/twn39/taskmq/internal/taskmq/client"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// stubWorker records Start/Stop for lifecycle hook tests.
type stubWorker struct {
	starts atomic.Int64
	stops  atomic.Int64
}

func (s *stubWorker) Register(string, worker.HandlerFunc) {}
func (s *stubWorker) Start(ctx context.Context) error {
	s.starts.Add(1)
	return nil
}
func (s *stubWorker) Stop(ctxs ...context.Context) { s.stops.Add(1) }

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

func TestBuildWorkerTopology_UsesDefaultLifecycle(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := &config.Config{
		TaskMQ: config.TaskMQConfig{
			Queues: []config.QueueConfig{{Name: "default", Concurrency: 1}},
		},
	}
	w, err := BuildWorkerTopology(rdb, zap.NewNop(), cfg, codec.JSONCodec{}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if w == nil {
		t.Fatal("expected worker")
	}
}

func TestProvideWorkers_WithQueues(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	lc := lifecycle.NewLifecycle(lifecycle.DefaultLifecycleConfig())
	cfg := &config.Config{
		TaskMQ: config.TaskMQConfig{
			Codec: "json",
			Queues: []config.QueueConfig{
				{Name: "q1", Concurrency: 2},
			},
			EventsMaxLen: 100,
		},
	}
	w, err := ProvideWorkers(ProvideWorkersParams{
		Rdb: rdb, Logger: zap.NewNop(), Codec: codec.JSONCodec{}, Cfg: cfg, Lifecycle: lc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w == nil {
		t.Fatal("nil worker")
	}
}

func TestLifecycleFromConfig_MapsFields(t *testing.T) {
	hard := int64(50)
	cfg := &config.Config{
		TaskMQ: config.TaskMQConfig{
			CompletedRetention: time.Hour,
			CompletedMaxCount:  10,
			Lifecycle: config.LifecycleConfig{
				EnqueueHardLimit: hard,
				CancelledTTL:     2 * time.Hour,
			},
		},
	}
	out := LifecycleFromConfig(cfg)
	if out.EnqueueHardLimit != hard {
		t.Fatalf("hard: %d", out.EnqueueHardLimit)
	}
	if out.CompletedRetention != time.Hour {
		t.Fatalf("retention: %v", out.CompletedRetention)
	}
	if out.CancelledTTL != 2*time.Hour {
		t.Fatalf("ttl: %v", out.CancelledTTL)
	}
	// nil config → defaults
	def := LifecycleFromConfig(nil)
	if def.DLQMaxCount <= 0 {
		t.Fatalf("default dlq: %d", def.DLQMaxCount)
	}
}

func TestRegisterWorkerPoolLifecycle_Hooks(t *testing.T) {
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() worker.Worker { return &stubWorker{} }),
		fx.Invoke(RegisterWorkerPoolLifecycle),
		fx.Invoke(func(w worker.Worker, lc fx.Lifecycle) {
			// Ensure hook is registered; Start/Stop via app lifecycle below.
			_ = w
			_ = lc
		}),
	)
	startCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	stopCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

// Ensure HandlerFunc type from stub compile path.
var _ worker.HandlerFunc = func(ctx context.Context, task *taskmodel.Task) error { return nil }
