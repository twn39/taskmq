package taskmq

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

// RetryPolicy defines the interface for deciding task retries and backoff intervals (Error-Aware).
type RetryPolicy interface {
	ShouldRetry(task *Task, err error) bool
	NextBackoff(task *Task) time.Duration
}

type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    bool
}

func NewExponentialBackoff(base, max time.Duration, jitter bool) RetryPolicy {
	return &ExponentialBackoff{
		BaseDelay: base,
		MaxDelay:  max,
		Jitter:    jitter,
	}
}

func (e *ExponentialBackoff) ShouldRetry(task *Task, err error) bool {
	return task.Retry < task.MaxRetry
}

func (e *ExponentialBackoff) NextBackoff(task *Task) time.Duration {
	backoff := e.BaseDelay * time.Duration(math.Pow(2, float64(task.Retry)))
	if backoff > e.MaxDelay {
		backoff = e.MaxDelay
	}

	if e.Jitter && backoff > 0 {
		return time.Duration(rand.Int63n(int64(backoff)))
	}
	return backoff
}

type ErrorFilterRetryPolicy struct {
	BasePolicy        RetryPolicy
	NonRetryableErrors []error
}

func NewErrorFilterRetryPolicy(base RetryPolicy, nonRetryable []error) RetryPolicy {
	return &ErrorFilterRetryPolicy{
		BasePolicy:        base,
		NonRetryableErrors: nonRetryable,
	}
}

func (p *ErrorFilterRetryPolicy) ShouldRetry(task *Task, err error) bool {
	if !p.BasePolicy.ShouldRetry(task, err) {
		return false
	}
	for _, targetErr := range p.NonRetryableErrors {
		if errors.Is(err, targetErr) {
			return false
		}
	}
	return true
}

func (p *ErrorFilterRetryPolicy) NextBackoff(task *Task) time.Duration {
	return p.BasePolicy.NextBackoff(task)
}
