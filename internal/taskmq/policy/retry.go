package policy

import (
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/task"
)

// RetryPolicy defines the interface for deciding task retries and backoff intervals (Error-Aware).
type RetryPolicy interface {
	ShouldRetry(t *task.Task, err error) bool
	NextBackoff(t *task.Task) time.Duration
}

// ExponentialBackoff is a RetryPolicy with exponential delay growth and optional jitter.
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    bool
}

// NewExponentialBackoff creates an exponential backoff retry policy.
func NewExponentialBackoff(base, max time.Duration, jitter bool) RetryPolicy {
	return &ExponentialBackoff{
		BaseDelay: base,
		MaxDelay:  max,
		Jitter:    jitter,
	}
}

// ShouldRetry reports whether the task has remaining retries.
func (e *ExponentialBackoff) ShouldRetry(t *task.Task, err error) bool {
	return t.Retry < t.MaxRetry
}

// NextBackoff returns the next delay before retrying.
func (e *ExponentialBackoff) NextBackoff(t *task.Task) time.Duration {
	backoff := e.BaseDelay * time.Duration(math.Pow(2, float64(t.Retry)))
	if backoff > e.MaxDelay {
		backoff = e.MaxDelay
	}

	if e.Jitter && backoff > 0 {
		return time.Duration(rand.Int63n(int64(backoff)))
	}
	return backoff
}

// ErrorFilterRetryPolicy wraps a base policy and skips non-retryable errors.
type ErrorFilterRetryPolicy struct {
	BasePolicy         RetryPolicy
	NonRetryableErrors []error
}

// NewErrorFilterRetryPolicy creates a filter over a base RetryPolicy.
func NewErrorFilterRetryPolicy(base RetryPolicy, nonRetryable []error) RetryPolicy {
	return &ErrorFilterRetryPolicy{
		BasePolicy:         base,
		NonRetryableErrors: nonRetryable,
	}
}

// ShouldRetry defers to the base policy unless the error is non-retryable.
func (p *ErrorFilterRetryPolicy) ShouldRetry(t *task.Task, err error) bool {
	if !p.BasePolicy.ShouldRetry(t, err) {
		return false
	}
	for _, targetErr := range p.NonRetryableErrors {
		if errors.Is(err, targetErr) {
			return false
		}
	}
	return true
}

// NextBackoff defers to the base policy.
func (p *ErrorFilterRetryPolicy) NextBackoff(t *task.Task) time.Duration {
	return p.BasePolicy.NextBackoff(t)
}
