package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// ClientOption configures NewClient.
type ClientOption func(*deps)

// WithClientCodec sets the task codec used by the client.
func WithClientCodec(c codec.Codec) ClientOption {
	return func(d *deps) {
		d.codec = c
	}
}

// WithDefaultUniqueTTL sets the default TTL for unique task locks.
func WithDefaultUniqueTTL(ttl time.Duration) ClientOption {
	return func(d *deps) {
		d.defaultUniqueTTL = ttl
	}
}

// WithClientLifecycle attaches retention / admission policy to the client.
func WithClientLifecycle(lc *lifecycle.Lifecycle) ClientOption {
	return func(d *deps) {
		d.lifecycle = lc
	}
}

// WithTaskID sets a custom task ID.
func WithTaskID(id string) TaskOption {
	return func(t *taskmodel.Task) error {
		if id == "" {
			return errors.New("taskmq: task ID cannot be empty")
		}
		t.ID = id
		return nil
	}
}

// WithTaskMaxRetry sets max retry count.
func WithTaskMaxRetry(max int) TaskOption {
	return func(t *taskmodel.Task) error {
		if max < 0 {
			return fmt.Errorf("taskmq: max retry must be non-negative: %d", max)
		}
		t.MaxRetry = max
		return nil
	}
}

// WithTaskTimeout sets the task timeout.
func WithTaskTimeout(timeout time.Duration) TaskOption {
	return func(t *taskmodel.Task) error {
		if timeout <= 0 {
			return fmt.Errorf("taskmq: timeout must be positive: %v", timeout)
		}
		t.TimeoutMs = int(timeout.Milliseconds())
		return nil
	}
}

// WithTaskUnique configures uniqueness for a task.
func WithTaskUnique(uniqueKey string, ttl time.Duration, scope taskmodel.UniqueScope) TaskOption {
	return func(t *taskmodel.Task) error {
		if uniqueKey == "" {
			return errors.New("taskmq: unique key cannot be empty")
		}
		if ttl <= 0 {
			return fmt.Errorf("taskmq: unique TTL must be positive: %v", ttl)
		}
		t.UniqueKey = uniqueKey
		t.UniqueTTLMs = int(ttl.Milliseconds())
		t.UniqueScope = scope
		return nil
	}
}

// WithTaskGroupKey sets the group key used for group-scoped rate limiting.
func WithTaskGroupKey(groupKey string) TaskOption {
	return func(t *taskmodel.Task) error {
		t.GroupKey = groupKey
		return nil
	}
}

// WithTaskQueue sets the destination queue.
func WithTaskQueue(queue string) TaskOption {
	return func(t *taskmodel.Task) error {
		if queue == "" {
			return errors.New("taskmq: queue name cannot be empty")
		}
		t.Queue = queue
		return nil
	}
}
