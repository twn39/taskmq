package client

import (
	"context"
	"time"

	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// ScheduledTask is a delayed task with its run-at timestamp.
type ScheduledTask struct {
	*taskmodel.Task
	RunAt time.Time `json:"run_at"`
}

// CronJob is a registered cron configuration with next run estimate.
type CronJob struct {
	JobName string `json:"job_name"`
	*taskmodel.Task
	NextRunTime time.Time `json:"next_run_time"`
}

// ActiveTask is a stream message (pending or processing).
type ActiveTask struct {
	*taskmodel.Task
	StreamID   string    `json:"stream_id"`
	Status     string    `json:"status"` // "Pending" or "Processing"
	Consumer   string    `json:"consumer,omitempty"`
	Deliveries int64     `json:"deliveries,omitempty"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// TaskOption mutates a task before enqueue/register operations.
type TaskOption func(*taskmodel.Task) error

// EnqueueClient is the production-path port (hot path).
type EnqueueClient interface {
	Enqueue(ctx context.Context, task *taskmodel.Task, opts ...TaskOption) error
	EnqueueIn(ctx context.Context, task *taskmodel.Task, delay time.Duration, opts ...TaskOption) error
	EnqueueAt(ctx context.Context, task *taskmodel.Task, at time.Time, opts ...TaskOption) error
}

// Enqueuer is an alias for EnqueueClient.
type Enqueuer = EnqueueClient

// CronClient manages periodic job registration.
type CronClient interface {
	RegisterCron(ctx context.Context, jobName string, spec string, task *taskmodel.Task, opts ...TaskOption) error
	ListCronJobs(ctx context.Context, queue string) ([]*CronJob, error)
	RunCronJob(ctx context.Context, queue string, jobName string) error
	DeleteCronJob(ctx context.Context, queue string, jobName string) error
}

// CronRegistrar is an alias for CronClient.
type CronRegistrar = CronClient

// DLQManager manages dead-letter queues.
type DLQManager interface {
	ListDeadLetters(ctx context.Context, queue string, limit int) ([]*taskmodel.Task, error)
	DeleteDeadLetter(ctx context.Context, queue string, taskID string) error
	RetryDeadLetter(ctx context.Context, queue string, taskID string) error
	RetryAllDeadLetters(ctx context.Context, queue string) (int64, error)
	PurgeAllDeadLetters(ctx context.Context, queue string) (int64, error)
}

// QueueController pauses and resumes consumption.
type QueueController interface {
	Pause(ctx context.Context, queue string) error
	Resume(ctx context.Context, queue string) error
	IsPaused(ctx context.Context, queue string) (bool, error)
}

// ScheduledTaskManager inspects and mutates delayed tasks.
type ScheduledTaskManager interface {
	ListScheduledTasks(ctx context.Context, queue string, limit int) ([]*ScheduledTask, error)
	RunScheduledTask(ctx context.Context, queue string, taskID string) error
	DeleteScheduledTask(ctx context.Context, queue string, taskID string) error
}

// ActiveTaskManager inspects and mutates in-flight stream messages.
type ActiveTaskManager interface {
	ListActiveTasks(ctx context.Context, queue string, limit int) ([]*ActiveTask, error)
	DeleteActiveTask(ctx context.Context, queue string, streamID string) error
}

// TaskCanceler marks tasks as cancelled.
type TaskCanceler interface {
	CancelTask(ctx context.Context, queue, taskID string) error
}

// AdminClient aggregates operational surfaces.
type AdminClient interface {
	DLQManager
	QueueController
	ScheduledTaskManager
	ActiveTaskManager
	TaskCanceler
}

// Client is the convenience facade combining enqueue, cron, and admin ports.
type Client interface {
	EnqueueClient
	CronClient
	AdminClient
}
