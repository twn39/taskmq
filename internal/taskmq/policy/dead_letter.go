package policy

import (
	"context"

	"github.com/twn39/taskmq/internal/taskmq/task"
)

// DeadLetterHook is invoked before a task is moved to the DLQ.
type DeadLetterHook func(ctx context.Context, t *task.Task, err error)

// DeadLetterPolicy abstracts dynamic routing and side effects for permanently failed tasks.
type DeadLetterPolicy interface {
	DLQQueueName(t *task.Task) string
	BeforeDeadLetter(ctx context.Context, t *task.Task, err error)
}

// StandardDeadLetterPolicy is the default DLQ routing policy.
type StandardDeadLetterPolicy struct {
	CustomDLQName string
	BeforeHook    DeadLetterHook
}

// NewStandardDeadLetterPolicy creates a standard dead-letter policy.
func NewStandardDeadLetterPolicy(customDLQ string, hook DeadLetterHook) DeadLetterPolicy {
	return &StandardDeadLetterPolicy{
		CustomDLQName: customDLQ,
		BeforeHook:    hook,
	}
}

// DLQQueueName returns the DLQ queue name for the task.
func (p *StandardDeadLetterPolicy) DLQQueueName(t *task.Task) string {
	if p.CustomDLQName != "" {
		return p.CustomDLQName
	}
	return t.Queue
}

// BeforeDeadLetter records last error and invokes the optional hook.
func (p *StandardDeadLetterPolicy) BeforeDeadLetter(ctx context.Context, t *task.Task, err error) {
	// Inject failures metadata
	t.LastError = err.Error()
	if t.ID != "" {
		// Side-effects / Hook trigger
		if p.BeforeHook != nil {
			p.BeforeHook(ctx, t, err)
		}
	}
}
