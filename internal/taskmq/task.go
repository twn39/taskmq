package taskmq

import (
	"encoding/json"
	"time"
)

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
	UniqueKey   string      `json:"unique_key"`
	UniqueTTLMs int         `json:"unique_ttl_ms"`
	UniqueScope UniqueScope `json:"unique_scope"`
	LastError   string      `json:"last_error"`
	CronSpec    string      `json:"cron_spec,omitempty"`
	GroupKey    string      `json:"group_key,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
}

// TaskOptions defines configurations applied when creating a task
type TaskOptions struct {
	ID          string
	Queue       string
	MaxRetry    int
	Timeout     time.Duration
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
		if opt.MaxRetry > 0 {
			task.MaxRetry = opt.MaxRetry
		}
		if opt.Timeout > 0 {
			task.TimeoutMs = int(opt.Timeout.Milliseconds())
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
	var task Task
	if err := json.Unmarshal(unsafeStringToBytes(data), &task); err != nil {
		return nil, err
	}
	return &task, nil
}
