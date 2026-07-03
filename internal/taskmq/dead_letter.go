package taskmq

import (
	"context"
)

type DeadLetterHook func(ctx context.Context, task *Task, err error)

// DeadLetterPolicy abstracts dynamic routing and side effects for permanently failed tasks.
type DeadLetterPolicy interface {
	DLQQueueName(task *Task) string
	BeforeDeadLetter(ctx context.Context, task *Task, err error)
}

type StandardDeadLetterPolicy struct {
	CustomDLQName string
	BeforeHook    DeadLetterHook
}

func NewStandardDeadLetterPolicy(customDLQ string, hook DeadLetterHook) DeadLetterPolicy {
	return &StandardDeadLetterPolicy{
		CustomDLQName: customDLQ,
		BeforeHook:    hook,
	}
}

func (p *StandardDeadLetterPolicy) DLQQueueName(task *Task) string {
	if p.CustomDLQName != "" {
		return p.CustomDLQName
	}
	return task.Queue
}

func (p *StandardDeadLetterPolicy) BeforeDeadLetter(ctx context.Context, task *Task, err error) {
	// Inject failures metadata
	task.LastError = err.Error()
	if task.ID != "" {
		// Side-effects / Hook trigger
		if p.BeforeHook != nil {
			p.BeforeHook(ctx, task, err)
		}
	}
}
