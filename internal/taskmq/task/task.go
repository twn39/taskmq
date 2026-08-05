package task

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrNoHandler = errors.New("taskmq: no handler registered")

// UniqueScope defines when the unique lock should be released
type UniqueScope int

const (
	// UniqueUntilSucceeded keeps the lock until the task completes successfully or moves to DLQ. (Default)
	UniqueUntilSucceeded UniqueScope = iota
	// UniqueUntilStart releases the lock as soon as the task starts executing.
	UniqueUntilStart
	// UniqueUntilSuccess keeps the lock during active execution but releases it on failure/retry.
	UniqueUntilSuccess
)

// Task represents a unit of work to be executed asynchronously
type Task struct {
	ID          string      `json:"id"`
	Queue       string      `json:"queue"`
	Name        string      `json:"name"`
	Payload     []byte      `json:"payload"`
	Retry       int         `json:"retry"`
	MaxRetry    int         `json:"max_retry"`
	TimeoutMs   int         `json:"timeout_ms"`
	// DeadlineMs is an absolute unix-millisecond deadline; 0 means none.
	// At execution, the effective deadline is min(now+TimeoutMs, DeadlineMs) when both set.
	DeadlineMs  int64       `json:"deadline_ms,omitempty"`
	UniqueKey   string      `json:"unique_key"`
	UniqueTTLMs int         `json:"unique_ttl_ms"`
	UniqueScope UniqueScope `json:"unique_scope"`
	LastError   string      `json:"last_error"`
	CronSpec    string      `json:"cron_spec,omitempty"`
	GroupKey    string      `json:"group_key,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	// Result is optional handler output retained when completed_retention > 0.
	// Not serialized into the stream payload by default; stored in task meta.
	Result []byte `json:"result,omitempty"`
}

// TaskOptions defines configurations applied when creating a task
type TaskOptions struct {
	ID          string
	Queue       string
	MaxRetry    *int
	Timeout     time.Duration
	// Deadline is an absolute wall-clock deadline; zero means none.
	Deadline    time.Time
	UniqueKey   string
	UniqueTTL   time.Duration
	UniqueScope UniqueScope
	GroupKey    string
}

// NewTask creates a new Task instance with default settings
func NewTask(name string, payload []byte, opts ...TaskOptions) *Task {
	// Set defaults
	task := &Task{
		Queue:     "default",
		Name:      name,
		Payload:   payload,
		MaxRetry:  3,
		TimeoutMs: 30000, // 30 seconds
		CreatedAt: time.Now(),
	}

	if len(opts) > 0 {
		opt := opts[0]
		if opt.ID != "" {
			task.ID = opt.ID
		}
		if opt.Queue != "" {
			task.Queue = opt.Queue
		}
		if opt.MaxRetry != nil {
			task.MaxRetry = *opt.MaxRetry
		}
		if opt.Timeout > 0 {
			task.TimeoutMs = int(opt.Timeout.Milliseconds())
		}
		if !opt.Deadline.IsZero() {
			task.DeadlineMs = opt.Deadline.UnixMilli()
		}
		if opt.UniqueKey != "" {
			task.UniqueKey = opt.UniqueKey
			task.UniqueTTLMs = int(opt.UniqueTTL.Milliseconds())
			task.UniqueScope = opt.UniqueScope
		}
		if opt.GroupKey != "" {
			task.GroupKey = opt.GroupKey
		}
	}

	return task
}

// EffectiveTimeout returns how long the handler may run from now, considering
// TimeoutMs and absolute DeadlineMs. Zero means no timeout.
func (t *Task) EffectiveTimeout(now time.Time) time.Duration {
	if t == nil {
		return 0
	}
	var until time.Duration
	if t.TimeoutMs > 0 {
		until = time.Duration(t.TimeoutMs) * time.Millisecond
	}
	if t.DeadlineMs > 0 {
		d := time.UnixMilli(t.DeadlineMs).Sub(now)
		if d <= 0 {
			return time.Nanosecond // already expired — force immediate cancel
		}
		if until <= 0 || d < until {
			until = d
		}
	}
	return until
}

// Serialize converts the task to a JSON string
func (t *Task) Serialize() (string, error) {
	data, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// DeserializeTask parses a task from a JSON string
func DeserializeTask(data string) (*Task, error) {
	var t Task
	if err := json.Unmarshal([]byte(data), &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// Ptr returns a pointer to the passed value. Useful for setting optional fields in struct literals.
func Ptr[T any](v T) *T {
	return &v
}
