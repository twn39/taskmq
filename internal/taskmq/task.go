package taskmq

import (
	"encoding/json"
	"time"
)

// Task represents a unit of work to be executed asynchronously
type Task struct {
	ID        string    `json:"id"`
	Queue     string    `json:"queue"`
	Name      string    `json:"name"`
	Payload   []byte    `json:"payload"`
	Retry     int       `json:"retry"`
	MaxRetry  int       `json:"max_retry"`
	TimeoutMs int       `json:"timeout_ms"`
	CreatedAt time.Time `json:"created_at"`
}

// NewTask creates a new Task instance with default settings
type TaskOptions struct {
	ID        string
	Queue     string
	MaxRetry  int
	Timeout   time.Duration
}

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
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		return nil, err
	}
	return &task, nil
}
