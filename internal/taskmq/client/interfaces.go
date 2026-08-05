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
	// EnqueueBulk pipelines immediate non-unique tasks; unique tasks use single-path enqueue.
	EnqueueBulk(ctx context.Context, tasks []*taskmodel.Task, opts ...BulkOption) (*BulkResult, error)
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

// TaskInspector looks up durable per-task metadata and progress.
type TaskInspector interface {
	GetTaskInfo(ctx context.Context, queue, taskID string) (*TaskInfoView, error)
	// UpdateProgress sets 0-100 progress (and optional data) on task meta + emits a progress event.
	UpdateProgress(ctx context.Context, queue, taskID string, percent int, data string) error
}

// TaskInfoView is the client-facing task metadata (alias of meta.TaskInfo fields).
type TaskInfoView struct {
	ID           string    `json:"id"`
	Queue        string    `json:"queue"`
	Name         string    `json:"name"`
	State        string    `json:"state"`
	Retry        int       `json:"retry"`
	MaxRetry     int       `json:"max_retry"`
	LastError    string    `json:"last_error,omitempty"`
	TimeoutMs    int       `json:"timeout_ms,omitempty"`
	DeadlineMs   int64     `json:"deadline_ms,omitempty"`
	UniqueKey    string    `json:"unique_key,omitempty"`
	GroupKey     string    `json:"group_key,omitempty"`
	StreamID     string    `json:"stream_id,omitempty"`
	Result       []byte    `json:"result,omitempty"`
	Progress     int       `json:"progress,omitempty"`
	ProgressData string    `json:"progress_data,omitempty"`
	CreatedAt    time.Time `json:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
	CompletedAt  time.Time `json:"completed_at,omitempty"`
}

// EventReader lists recent queue lifecycle events.
type EventReader interface {
	ListEvents(ctx context.Context, queue string, limit int64) ([]EventView, error)
}

// EventView is a client-facing queue event.
type EventView struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Queue       string `json:"queue"`
	TaskID      string `json:"task_id,omitempty"`
	Name        string `json:"name,omitempty"`
	Error       string `json:"error,omitempty"`
	Progress    int    `json:"progress,omitempty"`
	Data        string `json:"data,omitempty"`
	TimestampMs int64  `json:"timestamp_ms"`
}

// WorkerViewer lists live worker heartbeats for a queue.
type WorkerViewer interface {
	ListWorkers(ctx context.Context, queue string) ([]WorkerView, error)
}

// WorkerView is a live consumer heartbeat.
type WorkerView struct {
	Queue       string    `json:"queue"`
	Consumer    string    `json:"consumer"`
	Host        string    `json:"host,omitempty"`
	PID         int       `json:"pid,omitempty"`
	Concurrency int       `json:"concurrency,omitempty"`
	InUse       int       `json:"in_use,omitempty"`
	ActiveTask  string    `json:"active_task,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

// AdminClient aggregates operational surfaces.
type AdminClient interface {
	DLQManager
	QueueController
	ScheduledTaskManager
	ActiveTaskManager
	TaskCanceler
	TaskInspector
	EventReader
	WorkerViewer
}

// Client is the convenience facade combining enqueue, cron, and admin ports.
type Client interface {
	EnqueueClient
	CronClient
	AdminClient
}
