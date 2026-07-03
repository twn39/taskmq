package taskmq

import (
	"math"
	"time"
)

// RetryPolicy defines the interface for deciding task retries and backoff intervals.
type RetryPolicy interface {
	ShouldRetry(task *Task) bool
	NextBackoff(task *Task) time.Duration
}

type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func NewExponentialBackoff(base, max time.Duration) RetryPolicy {
	return &ExponentialBackoff{
		BaseDelay: base,
		MaxDelay:  max,
	}
}

func (e *ExponentialBackoff) ShouldRetry(task *Task) bool {
	return task.Retry < task.MaxRetry
}

func (e *ExponentialBackoff) NextBackoff(task *Task) time.Duration {
	backoff := e.BaseDelay * time.Duration(math.Pow(2, float64(task.Retry)))
	if backoff > e.MaxDelay {
		return e.MaxDelay
	}
	return backoff
}
