package task

import (
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var taskCmpOpts = []cmp.Option{
	cmp.Comparer(func(x, y time.Time) bool {
		if x.IsZero() && y.IsZero() {
			return true
		}
		return math.Abs(float64(x.Sub(y))) < float64(time.Second)
	}),
}

var taskCmpOptsIgnoreID = append(taskCmpOpts, cmpopts.IgnoreFields(Task{}, "ID"))

func TestNewTask_Defaults(t *testing.T) {
	got := NewTask("test-name", []byte("test-payload"))

	expected := &Task{
		Queue:     "default",
		Name:      "test-name",
		Payload:   []byte("test-payload"),
		MaxRetry:  3,
		TimeoutMs: 30000,
		CreatedAt: time.Now(),
	}

	if diff := cmp.Diff(expected, got, taskCmpOptsIgnoreID...); diff != "" {
		t.Errorf("NewTask defaults mismatch (-expected +actual):\n%s", diff)
	}
}

func TestNewTask_WithOptions(t *testing.T) {
	opts := TaskOptions{
		ID:          "custom-id",
		Queue:       "custom-q",
		MaxRetry:    Ptr(5),
		Timeout:     5 * time.Second,
		UniqueKey:   "uniq-1",
		UniqueTTL:   10 * time.Minute,
		UniqueScope: UniqueUntilStart,
		GroupKey:    "group-a",
	}

	got := NewTask("test-name", []byte("payload"), opts)

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

	if diff := cmp.Diff(expected, got, taskCmpOpts...); diff != "" {
		t.Errorf("NewTask options mismatch (-expected +actual):\n%s", diff)
	}
}

func TestTask_SerializationJSON(t *testing.T) {
	original := &Task{
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

	serialized, err := original.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize task: %v", err)
	}

	decoded, err := DeserializeTask(serialized)
	if err != nil {
		t.Fatalf("failed to deserialize task: %v", err)
	}

	if diff := cmp.Diff(original, decoded, taskCmpOpts...); diff != "" {
		t.Errorf("JSON serialization round-trip mismatch (-expected +actual):\n%s", diff)
	}
}
