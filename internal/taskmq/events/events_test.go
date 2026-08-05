package events

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
)

func setupEvents(t *testing.T) (context.Context, redis.UniversalClient, *miniredis.Miniredis, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return context.Background(), rdb, mr, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

func TestPublishAndList(t *testing.T) {
	ctx, rdb, _, cleanup := setupEvents(t)
	defer cleanup()

	p := NewPublisher(rdb, 100)
	if err := p.Publish(ctx, Event{Type: TypeCompleted, Queue: "q1", TaskID: "t1", Name: "n"}); err != nil {
		t.Fatal(err)
	}
	list, err := ListRecent(ctx, rdb, "q1", 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("list=%v err=%v", list, err)
	}
	if list[0].Type != TypeCompleted || list[0].TaskID != "t1" {
		t.Fatalf("%+v", list[0])
	}
	if list[0].TimestampMs == 0 {
		t.Fatal("timestamp should be set by Publish")
	}
	if list[0].ID == "" {
		t.Fatal("stream id should be populated")
	}
}

func TestPublish_NoopGuards(t *testing.T) {
	ctx, rdb, _, cleanup := setupEvents(t)
	defer cleanup()

	// Nil publisher / missing fields must not panic and not write.
	var nilPub *Publisher
	nilPub.Emit(ctx, "q", TypeEnqueued, "t", "n", "")
	if err := nilPub.Publish(ctx, Event{Type: TypeEnqueued, Queue: "q"}); err != nil {
		t.Fatalf("nil publish: %v", err)
	}

	p := NewPublisher(rdb, 0) // maxLen 0 → DefaultMaxLen
	if p.maxLen != DefaultMaxLen {
		t.Fatalf("default maxlen=%d", p.maxLen)
	}
	// Empty type/queue is no-op success.
	if err := p.Publish(ctx, Event{}); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(ctx, Event{Type: TypeFailed}); err != nil {
		t.Fatal(err)
	}
	n, err := rdb.XLen(ctx, keys.KeysFor("q").Events()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected no events, got %d", n)
	}
}

func TestEmit_AndListOrder(t *testing.T) {
	ctx, rdb, _, cleanup := setupEvents(t)
	defer cleanup()

	p := NewPublisher(rdb, 50)
	queue := "ev-order"
	p.Emit(ctx, queue, TypeEnqueued, "t1", "job", "")
	p.Emit(ctx, queue, TypeActive, "t1", "job", "")
	p.Emit(ctx, queue, TypeCompleted, "t1", "job", "")
	p.Emit(ctx, queue, TypeFailed, "t2", "job", "boom")
	p.Emit(ctx, queue, TypeProgress, "t1", "job", "")
	_ = p.Publish(ctx, Event{
		Type: TypeProgress, Queue: queue, TaskID: "t1", Progress: 40, Data: "half",
	})

	list, err := ListRecent(ctx, rdb, queue, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) < 5 {
		t.Fatalf("want >=5 events, got %d", len(list))
	}
	// ListRecent is oldest→newest.
	if list[0].Type != TypeEnqueued {
		t.Fatalf("first=%s", list[0].Type)
	}
	// Progress with data should round-trip.
	var sawProgress bool
	for _, e := range list {
		if e.Type == TypeProgress && e.Progress == 40 && e.Data == "half" {
			sawProgress = true
		}
	}
	if !sawProgress {
		t.Fatalf("progress event missing: %+v", list)
	}
}

func TestMaxLenApprox_Trims(t *testing.T) {
	ctx, rdb, _, cleanup := setupEvents(t)
	defer cleanup()

	// Tight MAXLEN; approximate trim may leave a few extra but should bound growth.
	p := NewPublisher(rdb, 5)
	queue := "ev-trim"
	for i := 0; i < 40; i++ {
		if err := p.Publish(ctx, Event{Type: TypeEnqueued, Queue: queue, TaskID: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := rdb.XLen(ctx, keys.KeysFor(queue).Events()).Result()
	if err != nil {
		t.Fatal(err)
	}
	// Approx MAXLEN can keep slightly more than maxLen; require well below raw write count.
	if n > 20 {
		t.Fatalf("events stream not trimmed enough: len=%d", n)
	}
}

func TestRead_WithTimeout(t *testing.T) {
	ctx, rdb, _, cleanup := setupEvents(t)
	defer cleanup()

	p := NewPublisher(rdb, 100)
	queue := "ev-read"
	_ = p.Publish(ctx, Event{Type: TypeDelayed, Queue: queue, TaskID: "d1"})

	// After 0-0 should see existing. Prefer ListRecent for deterministic non-block read.
	// XRead with Block=0 blocks forever when idle — always bound with ctx deadline.
	readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	evs, err := Read(readCtx, rdb, queue, "0-0", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Type != TypeDelayed {
		t.Fatalf("%+v", evs)
	}

	// From last id: short block, expect empty (timeout/Nil → no error or deadline).
	last := evs[0].ID
	readCtx2, cancel2 := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel2()
	evs2, err := Read(readCtx2, rdb, queue, last, 10, 50*time.Millisecond)
	// Either empty list (redis.Nil) or context deadline when blocked.
	if err != nil && readCtx2.Err() == nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err == nil && len(evs2) != 0 {
		t.Fatalf("expected empty after last id, got %+v", evs2)
	}

	// Defaults for empty lastID / count with short block.
	readCtx3, cancel3 := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel3()
	_, _ = Read(readCtx3, rdb, queue, "", 0, 20*time.Millisecond)
}

func TestPublish_AfterRedisClosed(t *testing.T) {
	ctx, rdb, mr, _ := setupEvents(t)
	// Close manually to force errors (miniredis already closed; skip defer cleanup).
	p := NewPublisher(rdb, 10)
	_ = rdb.Close()
	mr.Close()

	err := p.Publish(ctx, Event{Type: TypeStalled, Queue: "q", TaskID: "t"})
	if err == nil {
		t.Fatal("expected error after redis closed")
	}
	// Emit swallows errors (best-effort).
	p.Emit(ctx, "q", TypeStalled, "t", "n", "x")
}

func TestListRecent_DefaultCount(t *testing.T) {
	ctx, rdb, _, cleanup := setupEvents(t)
	defer cleanup()
	p := NewPublisher(rdb, 200)
	for i := 0; i < 3; i++ {
		_ = p.Publish(ctx, Event{Type: TypeEnqueued, Queue: "qdef", TaskID: "t"})
	}
	list, err := ListRecent(ctx, rdb, "qdef", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("len=%d", len(list))
	}
	_ = time.Now() // keep time import if needed by future clock asserts
}
