package heartbeat

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
)

func TestReporterAndList(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	rep := NewReporter(rdb, "q1", "c1", 5,
		WithInUse(func() int { return 2 }),
		WithActiveTask(func() string { return "task-42" }),
	)
	rep.interval = 50 * time.Millisecond
	rep.ttl = time.Second
	done := make(chan struct{})
	go func() {
		_ = rep.Run(ctx)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	list, err := List(ctx, rdb, "q1")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Consumer != "c1" || list[0].InUse != 2 {
		t.Fatalf("%+v", list)
	}
	if list[0].ActiveTask != "task-42" {
		t.Fatalf("active task: %+v", list[0])
	}
	if list[0].Concurrency != 5 {
		t.Fatalf("concurrency: %+v", list[0])
	}
	cancel()
	<-done

	// After stop, heartbeat key is deleted.
	time.Sleep(20 * time.Millisecond)
	list2, err := List(context.Background(), rdb, "q1")
	if err != nil {
		t.Fatal(err)
	}
	if len(list2) != 0 {
		t.Fatalf("expected empty after stop, got %+v", list2)
	}
}

func TestReporter_NilSafe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var rep *Reporter
	err := rep.Run(ctx)
	if err == nil {
		t.Fatal("expected ctx error")
	}
}

func TestParseConsumerFromKeyAndFormatAge(t *testing.T) {
	// Build key via keys package so check_keys_schema.sh stays green.
	hbKey := keys.KeysFor("q").Heartbeat("c9")
	if got := ParseConsumerFromKey(hbKey); got != "c9" {
		t.Fatalf("got %q from %q", got, hbKey)
	}
	if got := ParseConsumerFromKey("no-marker"); got != "" {
		t.Fatalf("got %q", got)
	}
	if FormatAge(time.Time{}) != "" {
		t.Fatal("zero time")
	}
	if FormatAge(time.Now().Add(-2*time.Second)) == "" {
		t.Fatal("expected age string")
	}
}

func TestList_EmptyQueue(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	list, err := List(context.Background(), rdb, "empty-q")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("%+v", list)
	}
}
