package worker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"github.com/twn39/taskmq/internal/taskmq/worker"
	"go.uber.org/zap"
)

func TestCancelations_AddCancelDelete(t *testing.T) {
	c := worker.NewCancelations()
	var cancelled atomic.Bool
	c.Add("t1", func() { cancelled.Store(true) })
	c.Cancel("t1")
	require.True(t, cancelled.Load())

	// Delete then cancel is a no-op.
	cancelled.Store(false)
	c.Add("t2", func() { cancelled.Store(true) })
	c.Delete("t2")
	c.Cancel("t2")
	require.False(t, cancelled.Load())

	// Missing id is safe.
	c.Cancel("missing")
}

func TestCancelHub_SubscriberCancelsActiveTask(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	hub := worker.NewCancelHub(rdb, zap.NewNop())
	var cancelled atomic.Bool
	hub.Add("job-1", func() { cancelled.Store(true) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	queue := "cancel-hub-q"
	hub.StartSubscriber(ctx, &wg, []string{queue})

	// Empty queues is a no-op.
	hub.StartSubscriber(ctx, &wg, nil)

	// Allow subscription to attach.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, rdb.Publish(ctx, keys.KeysFor(queue).CancelChannel(), "job-1").Err())

	require.Eventually(t, func() bool { return cancelled.Load() }, 2*time.Second, 20*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestPauseController_SetWaitReconcile(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	pc := worker.NewPauseController(rdb, zap.NewNop())
	queue := "pause-q"
	require.False(t, pc.IsPaused(queue))

	pc.SetPaused(queue, true)
	require.True(t, pc.IsPaused(queue))
	ch := pc.WaitChan(queue)

	// Resume closes wait channel.
	done := make(chan struct{})
	go func() {
		<-ch
		close(done)
	}()
	pc.SetPaused(queue, false)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait chan not closed on resume")
	}
	require.False(t, pc.IsPaused(queue))

	// Reconcile from Redis pause key.
	require.NoError(t, rdb.Set(context.Background(), keys.KeysFor(queue).Paused(), "1", 0).Err())
	pc.Reconcile(context.Background(), []string{queue})
	require.True(t, pc.IsPaused(queue))
}

func TestPauseController_SubscriberPauseResume(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	pc := worker.NewPauseController(rdb, zap.NewNop())
	queue := "pause-sub-q"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	pc.StartSubscriber(ctx, &wg, []string{queue})
	pc.StartSubscriber(ctx, &wg, nil) // no-op

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, rdb.Publish(ctx, keys.KeysFor(queue).Control(), "pause").Err())
	require.Eventually(t, func() bool { return pc.IsPaused(queue) }, 2*time.Second, 20*time.Millisecond)

	require.NoError(t, rdb.Publish(ctx, keys.KeysFor(queue).Control(), "resume").Err())
	require.Eventually(t, func() bool { return !pc.IsPaused(queue) }, 2*time.Second, 20*time.Millisecond)

	cancel()
	wg.Wait()
}

// fakeWorker records Register/Start/Stop for MultiQueueWorker tests.
type fakeWorker struct {
	name     string
	starts   atomic.Int64
	stops    atomic.Int64
	register atomic.Int64
	startErr error
}

func (f *fakeWorker) Register(taskName string, handler worker.HandlerFunc) { f.register.Add(1) }
func (f *fakeWorker) Start(ctx context.Context) error {
	f.starts.Add(1)
	return f.startErr
}
func (f *fakeWorker) Stop(ctxs ...context.Context) { f.stops.Add(1) }

func TestMultiQueueWorker_LifecycleAndDedup(t *testing.T) {
	shared := &fakeWorker{name: "shared"}
	w1 := &fakeWorker{name: "a"}
	mw := worker.NewMultiQueueWorker(map[string]worker.Worker{
		"high": shared,
		"low":  shared,
		"med":  w1,
	})

	require.Equal(t, shared, mw.Queue("high"))
	require.Nil(t, mw.Queue("missing"))

	mw.Register("job", func(ctx context.Context, task *taskmodel.Task) error { return nil })
	// shared appears twice in map but Register dedups by pointer → 1 + w1 = 2.
	require.Equal(t, int64(1), shared.register.Load())
	require.Equal(t, int64(1), w1.register.Load())

	require.NoError(t, mw.Start(context.Background()))
	require.Equal(t, int64(1), shared.starts.Load())
	require.Equal(t, int64(1), w1.starts.Load())

	mw.Stop(context.Background())
	require.Equal(t, int64(1), shared.stops.Load())
	require.Equal(t, int64(1), w1.stops.Load())

	// Start error propagates.
	bad := &fakeWorker{startErr: context.Canceled}
	mw2 := worker.NewMultiQueueWorker(map[string]worker.Worker{"q": bad})
	require.ErrorIs(t, mw2.Start(context.Background()), context.Canceled)
}
