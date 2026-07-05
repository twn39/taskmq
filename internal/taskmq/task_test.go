package taskmq

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestNewTask_Defaults(t *testing.T) {
	task := NewTask("test-name", []byte("test-payload"))

	expected := &Task{
		Queue:     "default",
		Name:      "test-name",
		Payload:   []byte("test-payload"),
		MaxRetry:  3,
		TimeoutMs: 30000,
		CreatedAt: time.Now(),
	}

	// Compare using TaskCmpOptsIgnoreID since ID is blank/not set by default (it's generated on Enqueue)
	if diff := cmp.Diff(expected, task, TaskCmpOptsIgnoreID...); diff != "" {
		t.Errorf("NewTask defaults mismatch (-expected +actual):\n%s", diff)
	}
}

func TestNewTask_WithOptions(t *testing.T) {
	opts := TaskOptions{
		ID:          "custom-id",
		Queue:       "custom-q",
		MaxRetry:    5,
		Timeout:     5 * time.Second,
		UniqueKey:   "uniq-1",
		UniqueTTL:   10 * time.Minute,
		UniqueScope: UniqueUntilStart,
		GroupKey:    "group-a",
	}

	task := NewTask("test-name", []byte("payload"), opts)

	expected := &Task{
		ID:          "custom-id",
		Queue:       "custom-q",
		Name:        "test-name",
		Payload:     []byte("payload"),
		MaxRetry:    5,
		TimeoutMs:   5000,
		UniqueKey:   "uniq-1",
		UniqueTTLMs: 600000,
		UniqueScope: UniqueUntilStart,
		GroupKey:    "group-a",
		CreatedAt:   time.Now(),
	}

	if diff := cmp.Diff(expected, task, TaskCmpOpts...); diff != "" {
		t.Errorf("NewTask options mismatch (-expected +actual):\n%s", diff)
	}
}

func TestTask_SerializationJSON(t *testing.T) {
	task := &Task{
		ID:          "serialized-id",
		Queue:       "q",
		Name:        "task:send",
		Payload:     []byte("some-data"),
		Retry:       1,
		MaxRetry:    4,
		TimeoutMs:   15000,
		UniqueKey:   "u-key",
		UniqueTTLMs: 120000,
		UniqueScope: UniqueUntilSuccess,
		LastError:   "some error",
		CronSpec:    "*/2 * * * *",
		GroupKey:    "grp",
		CreatedAt:   time.Now(),
	}

	serialized, err := task.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize task: %v", err)
	}

	var decoded Task
	codec := JSONCodec{}
	err = codec.Unmarshal([]byte(serialized), &decoded)
	if err != nil {
		t.Fatalf("failed to unmarshal task: %v", err)
	}

	if diff := cmp.Diff(task, &decoded, TaskCmpOpts...); diff != "" {
		t.Errorf("JSON serialization round-trip mismatch (-expected +actual):\n%s", diff)
	}
}
