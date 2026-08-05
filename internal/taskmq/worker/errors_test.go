package worker

import (
	"errors"
	"fmt"
	"testing"
)

func TestHandled_IsAndUnwrap(t *testing.T) {
	t.Parallel()
	cause := errors.New("handler boom")
	err := Handled(cause)

	if !IsHandled(err) {
		t.Fatal("expected IsHandled true")
	}
	if !errors.Is(err, ErrHandled) {
		t.Fatal("expected errors.Is ErrHandled")
	}
	if !errors.Is(err, cause) {
		t.Fatal("expected errors.Is cause")
	}
	if Handled(nil) != ErrHandled {
		t.Fatal("Handled(nil) should be ErrHandled")
	}
	// Idempotent
	again := Handled(err)
	if !IsHandled(again) {
		t.Fatal("Handled of already-handled should stay handled")
	}
	wrapped := fmt.Errorf("outer: %w", err)
	if !IsHandled(wrapped) {
		t.Fatal("errors.Is should pierce outer wrap")
	}
}
