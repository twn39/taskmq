package task

import (
	"errors"
	"testing"
	"time"
)

func TestSkipRetryAndUnrecoverable(t *testing.T) {
	cause := errors.New("bad payload")
	err := SkipRetry(cause)
	if !IsSkipRetry(err) {
		t.Fatal("expected IsSkipRetry")
	}
	if !errors.Is(err, ErrSkipRetry) {
		t.Fatal("expected errors.Is ErrSkipRetry")
	}
	if !errors.Is(err, cause) {
		t.Fatal("expected unwrap to cause")
	}

	u := Unrecoverable(cause)
	if !IsSkipRetry(u) {
		t.Fatal("Unrecoverable should count as skip retry")
	}
	if !errors.Is(u, ErrUnrecoverable) {
		t.Fatal("expected ErrUnrecoverable")
	}
}

func TestEffectiveTimeout(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	task := &Task{TimeoutMs: 5000}
	if d := task.EffectiveTimeout(now); d != 5*time.Second {
		t.Fatalf("got %v", d)
	}
	task.DeadlineMs = now.Add(2 * time.Second).UnixMilli()
	if d := task.EffectiveTimeout(now); d != 2*time.Second {
		t.Fatalf("deadline should win: %v", d)
	}
	task.DeadlineMs = now.Add(-time.Second).UnixMilli()
	if d := task.EffectiveTimeout(now); d != time.Nanosecond {
		t.Fatalf("expired deadline: %v", d)
	}
}
