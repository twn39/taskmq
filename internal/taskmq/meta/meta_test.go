package meta

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestStore_PutGetMarkCompleted(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	s := NewStore(rdb)
	ctx := context.Background()
	task := &taskmodel.Task{
		ID:       "t1",
		Queue:    "default",
		Name:     "demo",
		MaxRetry: 3,
	}
	if err := s.Put(ctx, task, StatePending); err != nil {
		t.Fatal(err)
	}
	info, err := s.Get(ctx, "default", "t1")
	if err != nil || info == nil || info.State != StatePending {
		t.Fatalf("get: %+v err=%v", info, err)
	}

	if err := s.MarkCompleted(ctx, task, []byte("ok"), time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	info, err = s.Get(ctx, "default", "t1")
	if err != nil || info == nil || info.State != StateCompleted {
		t.Fatalf("completed: %+v err=%v", info, err)
	}
	if string(info.Result) != "ok" {
		t.Fatalf("result=%q", info.Result)
	}

	// retention 0 deletes meta
	if err := s.MarkCompleted(ctx, task, nil, 0, 0); err != nil {
		t.Fatal(err)
	}
	info, err = s.Get(ctx, "default", "t1")
	if err != nil || info != nil {
		t.Fatalf("expected deleted meta, got %+v", info)
	}
}

func TestStore_PurgeExpiredCompleted(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	s := NewStore(rdb)
	ctx := context.Background()
	queue := "meta-purge"
	old := &taskmodel.Task{ID: "old", Queue: queue, Name: "job"}
	newT := &taskmodel.Task{ID: "new", Queue: queue, Name: "job"}
	if err := s.MarkCompleted(ctx, old, []byte("o"), time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCompleted(ctx, newT, []byte("n"), time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	// Force old completed index score into the past.
	qkScore := time.Now().Add(-2 * time.Hour).UnixMilli()
	if err := rdb.ZAdd(ctx, keys.KeysFor(queue).Completed(), redis.Z{
		Score: float64(qkScore), Member: "old",
	}).Err(); err != nil {
		t.Fatal(err)
	}

	before := time.Now().Add(-time.Hour).UnixMilli()
	n, err := s.PurgeExpiredCompleted(ctx, queue, before, 50)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("purged=%d want 1", n)
	}
	info, err := s.Get(ctx, queue, "old")
	if err != nil || info != nil {
		t.Fatalf("old meta should be gone: %+v %v", info, err)
	}
	info, err = s.Get(ctx, queue, "new")
	if err != nil || info == nil {
		t.Fatalf("new meta should remain: %+v %v", info, err)
	}
}

func TestStore_PipelinedAndStateOperations(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	s := NewStore(rdb)
	ctx := context.Background()
	task := &taskmodel.Task{
		ID:        "t-pipe",
		Queue:     "pipe-queue",
		Name:      "pipe-job",
		MaxRetry:  5,
		CreatedAt: time.Now(),
	}

	// 1. PutPipelined
	pipe := rdb.Pipeline()
	s.PutPipelined(ctx, pipe, task, StatePending)
	_, err = pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("exec put pipelined: %v", err)
	}

	info, err := s.Get(ctx, "pipe-queue", "t-pipe")
	if err != nil || info == nil || info.State != StatePending {
		t.Fatalf("expected pending state from pipelined put, got %+v", info)
	}

	// 2. SetState
	if err := s.SetState(ctx, "pipe-queue", "t-pipe", StateActive, "", "stream-123"); err != nil {
		t.Fatal(err)
	}
	info, _ = s.Get(ctx, "pipe-queue", "t-pipe")
	if info.State != StateActive || info.StreamID != "stream-123" {
		t.Fatalf("expected active state and stream id, got %+v", info)
	}

	// 3. MarkRetry & MarkDLQ
	if err := s.MarkRetry(ctx, task, "temporary network glitch"); err != nil {
		t.Fatal(err)
	}
	info, _ = s.Get(ctx, "pipe-queue", "t-pipe")
	if info.State != StateRetry || info.LastError != "temporary network glitch" {
		t.Fatalf("expected retry state, got %+v", info)
	}

	if err := s.MarkDLQ(ctx, task, "fatal unrecoverable error"); err != nil {
		t.Fatal(err)
	}
	info, _ = s.Get(ctx, "pipe-queue", "t-pipe")
	if info.State != StateDLQ || info.LastError != "fatal unrecoverable error" {
		t.Fatalf("expected dlq state, got %+v", info)
	}

	// 4. SetProgress
	if err := s.SetProgress(ctx, "pipe-queue", "t-pipe", 50, "halfway done"); err != nil {
		t.Fatal(err)
	}
	info, _ = s.Get(ctx, "pipe-queue", "t-pipe")
	if info.Progress != 50 || info.ProgressData != "halfway done" {
		t.Fatalf("expected progress 50, got %+v", info)
	}

	// Clamp progress boundaries
	_ = s.SetProgress(ctx, "pipe-queue", "t-pipe", -10, "")
	info, _ = s.Get(ctx, "pipe-queue", "t-pipe")
	if info.Progress != 0 {
		t.Fatalf("expected clamped min progress 0, got %d", info.Progress)
	}

	_ = s.SetProgress(ctx, "pipe-queue", "t-pipe", 150, "")
	info, _ = s.Get(ctx, "pipe-queue", "t-pipe")
	if info.Progress != 100 {
		t.Fatalf("expected clamped max progress 100, got %d", info.Progress)
	}

	// 5. Delete
	if err := s.Delete(ctx, "pipe-queue", "t-pipe"); err != nil {
		t.Fatal(err)
	}
	info, err = s.Get(ctx, "pipe-queue", "t-pipe")
	if err != nil || info != nil {
		t.Fatalf("expected deleted meta, got %+v", info)
	}

	// 6. Nil Store Guard checks
	var nilStore *Store
	_ = nilStore.Put(ctx, task, StatePending)
	nilStore.PutPipelined(ctx, pipe, task, StatePending)
	_ = nilStore.SetState(ctx, "q", "id", StateActive, "", "")
	_ = nilStore.MarkCompleted(ctx, task, nil, 0, 0)
	_ = nilStore.Delete(ctx, "q", "id")
	_ = nilStore.SetProgress(ctx, "q", "id", 10, "")
	_, _ = nilStore.PurgeExpiredCompleted(ctx, "q", 0, 10)
	_, err = nilStore.Get(ctx, "q", "id")
	if err == nil {
		t.Fatal("expected error from nilStore.Get")
	}
}
