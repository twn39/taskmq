package worker

import "errors"

// ErrHandled means the stream message outcome was already applied
// (retry scheduled, moved to DLQ, rate-limit deferred, etc.).
// Outer layers must not CompleteTask and should not treat this as an
// "unsettled" failure. Use errors.Is(err, ErrHandled) or IsHandled.
var ErrHandled = errors.New("taskmq: message outcome already handled")

// handledError wraps the original handler/business error while marking
// the queue outcome as settled.
type handledError struct {
	cause error
}

// Handled marks cause as settled. If cause is nil, returns ErrHandled alone.
func Handled(cause error) error {
	if cause == nil {
		return ErrHandled
	}
	if errors.Is(cause, ErrHandled) {
		return cause
	}
	return &handledError{cause: cause}
}

func (e *handledError) Error() string {
	return "taskmq: handled: " + e.cause.Error()
}

func (e *handledError) Unwrap() error { return e.cause }

func (e *handledError) Is(target error) bool { return target == ErrHandled }

// IsHandled reports whether err indicates a settled message outcome.
func IsHandled(err error) bool {
	return errors.Is(err, ErrHandled)
}
