package policy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

func TestExponentialBackoff_ShouldRetry(t *testing.T) {
	p := policy.NewExponentialBackoff(time.Second, time.Minute, false)
	biz := errors.New("temporary")

	cases := []struct {
		name  string
		retry int
		max   int
		err   error
		want  bool
	}{
		{"under max", 0, 3, biz, true},
		{"at max-1", 2, 3, biz, true},
		{"at max", 3, 3, biz, false},
		{"over max", 5, 3, biz, false},
		{"skip retry sentinel", 0, 5, taskmodel.SkipRetry(biz), false},
		{"unrecoverable", 0, 5, taskmodel.Unrecoverable(biz), false},
		{"max zero", 0, 0, biz, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk := &taskmodel.Task{Retry: tc.retry, MaxRetry: tc.max}
			require.Equal(t, tc.want, p.ShouldRetry(tk, tc.err))
		})
	}
}

func TestExponentialBackoff_NextBackoff(t *testing.T) {
	p := policy.NewExponentialBackoff(100*time.Millisecond, 800*time.Millisecond, false).(*policy.ExponentialBackoff)

	// Retry N → Base * 2^N (clamped to Max).
	// Note: NextBackoff uses t.Retry as the exponent (already-incremented counter in middleware).
	cases := []struct {
		retry int
		want  time.Duration
	}{
		{0, 100 * time.Millisecond},  // 100 * 2^0
		{1, 200 * time.Millisecond},  // 100 * 2^1
		{2, 400 * time.Millisecond},  // 100 * 2^2
		{3, 800 * time.Millisecond},  // 100 * 2^3 = 800
		{4, 800 * time.Millisecond},  // clamped
		{10, 800 * time.Millisecond}, // clamped
	}
	for _, tc := range cases {
		got := p.NextBackoff(&taskmodel.Task{Retry: tc.retry})
		require.Equal(t, tc.want, got, "retry=%d", tc.retry)
	}
}

func TestExponentialBackoff_JitterIsBounded(t *testing.T) {
	p := policy.NewExponentialBackoff(time.Second, 10*time.Second, true)
	tk := &taskmodel.Task{Retry: 0}
	// With jitter, delay is in [0, base) for Retry=0 base=1s.
	for i := 0; i < 20; i++ {
		d := p.NextBackoff(tk)
		require.GreaterOrEqual(t, d, time.Duration(0))
		require.Less(t, d, time.Second)
	}
}

func TestErrorFilterRetryPolicy(t *testing.T) {
	nonRetry := errors.New("validation")
	base := policy.NewExponentialBackoff(time.Millisecond, time.Second, false)
	p := policy.NewErrorFilterRetryPolicy(base, []error{nonRetry})

	tk := &taskmodel.Task{Retry: 0, MaxRetry: 3}
	require.True(t, p.ShouldRetry(tk, errors.New("other")))
	require.False(t, p.ShouldRetry(tk, nonRetry))
	require.False(t, p.ShouldRetry(tk, taskmodel.SkipRetry(errors.New("x"))))
	// Wrapped non-retryable still matches via errors.Is.
	require.False(t, p.ShouldRetry(tk, errors.Join(errors.New("wrap"), nonRetry)))

	// NextBackoff defers to base.
	require.Equal(t, base.NextBackoff(tk), p.NextBackoff(tk))

	// Base already exhausted.
	tk.Retry = 3
	require.False(t, p.ShouldRetry(tk, errors.New("other")))
}

func TestStandardDeadLetterPolicy(t *testing.T) {
	var hooked bool
	p := policy.NewStandardDeadLetterPolicy("custom-dlq", func(ctx context.Context, t *taskmodel.Task, err error) {
		hooked = true
	})

	tk := &taskmodel.Task{ID: "t1", Queue: "main"}
	require.Equal(t, "custom-dlq", p.DLQQueueName(tk))

	// Empty custom → task queue.
	p2 := policy.NewStandardDeadLetterPolicy("", nil)
	require.Equal(t, "main", p2.DLQQueueName(tk))

	err := errors.New("fatal")
	p.BeforeDeadLetter(context.Background(), tk, err)
	require.True(t, hooked)
	require.Equal(t, "fatal", tk.LastError)

	// No ID → hook skipped but LastError still set.
	hooked = false
	tk2 := &taskmodel.Task{Queue: "main"}
	p.BeforeDeadLetter(context.Background(), tk2, err)
	require.False(t, hooked)
	require.Equal(t, "fatal", tk2.LastError)
}
