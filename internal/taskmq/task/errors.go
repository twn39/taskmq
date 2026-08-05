package task

import "errors"

// Sentinel errors for handler control of retry behavior.
// Prefer wrapping business errors: return SkipRetry(err) or Unrecoverable(err).
var (
	// ErrSkipRetry means do not retry; move straight to DLQ (or terminal failure).
	// Use for permanent validation errors (e.g. bad payload).
	ErrSkipRetry = errors.New("taskmq: skip retry")

	// ErrUnrecoverable is an alias semantics for permanent failure without retry.
	// Distinct name for callers migrating from BullMQ UnrecoverableError.
	ErrUnrecoverable = errors.New("taskmq: unrecoverable")
)

type wrappedSentinel struct {
	sentinel error
	cause    error
}

func (e *wrappedSentinel) Error() string {
	if e.cause == nil {
		return e.sentinel.Error()
	}
	return e.sentinel.Error() + ": " + e.cause.Error()
}

func (e *wrappedSentinel) Unwrap() error { return e.cause }

func (e *wrappedSentinel) Is(target error) bool {
	return target == e.sentinel
}

// SkipRetry wraps cause so retries are skipped and the task goes to DLQ.
// If cause is nil, returns ErrSkipRetry alone.
func SkipRetry(cause error) error {
	if cause == nil {
		return ErrSkipRetry
	}
	if errors.Is(cause, ErrSkipRetry) || errors.Is(cause, ErrUnrecoverable) {
		return cause
	}
	return &wrappedSentinel{sentinel: ErrSkipRetry, cause: cause}
}

// Unrecoverable wraps cause as a permanent failure (no retry). Same settlement as SkipRetry.
func Unrecoverable(cause error) error {
	if cause == nil {
		return ErrUnrecoverable
	}
	if errors.Is(cause, ErrSkipRetry) || errors.Is(cause, ErrUnrecoverable) {
		return cause
	}
	return &wrappedSentinel{sentinel: ErrUnrecoverable, cause: cause}
}

// IsSkipRetry reports whether err requests no further retries.
func IsSkipRetry(err error) bool {
	return errors.Is(err, ErrSkipRetry) || errors.Is(err, ErrUnrecoverable)
}
