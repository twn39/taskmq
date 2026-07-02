package taskmq

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrDuplicateTask is returned when a unique task cannot be enqueued because a duplicate already exists.
var ErrDuplicateTask = errors.New("taskmq: duplicate task in queue")

type Client interface {
	Enqueue(ctx context.Context, task *Task) error
	EnqueueIn(ctx context.Context, task *Task, delay time.Duration) error
	EnqueueAt(ctx context.Context, task *Task, at time.Time) error
	ListDeadLetters(ctx context.Context, queue string, limit int) ([]*Task, error)
	DeleteDeadLetter(ctx context.Context, queue string, taskID string) error
	RetryDeadLetter(ctx context.Context, queue string, taskID string) error
}

type client struct {
	rdb *redis.Client
}

func NewClient(rdb *redis.Client) Client {
	return &client{rdb: rdb}
}

// generateUUID generates a lightweight pseudo-random UUID-v4-like string
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (c *client) acquireUniqueLock(ctx context.Context, task *Task) (bool, error) {
	if task.UniqueKey == "" {
		return true, nil
	}

	if task.ID == "" {
		task.ID = generateUUID()
	}

	uniqueKey := UniqueKey(task.Queue, task.UniqueKey)
	ttl := time.Duration(task.UniqueTTLMs) * time.Millisecond
	if ttl <= 0 {
		ttl = 1 * time.Hour // Default to 1 hour
	}

	return c.rdb.SetNX(ctx, uniqueKey, task.ID, ttl).Result()
}

// Enqueue adds a task to the Redis stream immediately (active queue)
func (c *client) Enqueue(ctx context.Context, task *Task) error {
	if task.ID == "" {
		task.ID = generateUUID()
	}

	// Try acquiring unique lock
	ok, err := c.acquireUniqueLock(ctx, task)
	if err != nil {
		return err
	}
	if !ok {
		return ErrDuplicateTask
	}

	serialized, err := task.Serialize()
	if err != nil {
		return err
	}

	streamKey := StreamKey(task.Queue)
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"task": serialized,
		},
	}).Err()
}

// EnqueueIn adds a task to the delayed queue with a delay duration
func (c *client) EnqueueIn(ctx context.Context, task *Task, delay time.Duration) error {
	return c.EnqueueAt(ctx, task, time.Now().Add(delay))
}

// EnqueueAt adds a task to the delayed queue to be executed at a specific time
func (c *client) EnqueueAt(ctx context.Context, task *Task, at time.Time) error {
	if task.ID == "" {
		task.ID = generateUUID()
	}

	// Try acquiring unique lock
	ok, err := c.acquireUniqueLock(ctx, task)
	if err != nil {
		return err
	}
	if !ok {
		return ErrDuplicateTask
	}

	serialized, err := task.Serialize()
	if err != nil {
		return err
	}

	delayedKey := DelayedKey(task.Queue)
	return c.rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: serialized,
	}).Err()
}

// ListDeadLetters returns the list of dead-letter tasks in the queue, sorted by descending death time
func (c *client) ListDeadLetters(ctx context.Context, queue string, limit int) ([]*Task, error) {
	dlqKey := DLQKey(queue)
	members, err := c.rdb.ZRevRange(ctx, dlqKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*Task, 0, len(members))
	for _, m := range members {
		task, err := DeserializeTask(m)
		if err != nil {
			continue // skip corrupted data
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// DeleteDeadLetter removes a specific task from the dead-letter queue by task ID
func (c *client) DeleteDeadLetter(ctx context.Context, queue string, taskID string) error {
	dlqKey := DLQKey(queue)
	members, err := c.rdb.ZRange(ctx, dlqKey, 0, -1).Result()
	if err != nil {
		return err
	}

	for _, m := range members {
		task, err := DeserializeTask(m)
		if err == nil && task.ID == taskID {
			return c.rdb.ZRem(ctx, dlqKey, m).Err()
		}
	}
	return nil // Not found is a no-op
}

// RetryDeadLetter retries a dead-letter task by resetting retry counts and re-enqueuing it
func (c *client) RetryDeadLetter(ctx context.Context, queue string, taskID string) error {
	dlqKey := DLQKey(queue)
	members, err := c.rdb.ZRange(ctx, dlqKey, 0, -1).Result()
	if err != nil {
		return err
	}

	var targetMember string
	var targetTask *Task
	for _, m := range members {
		task, err := DeserializeTask(m)
		if err == nil && task.ID == taskID {
			targetMember = m
			targetTask = task
			break
		}
	}

	if targetTask == nil {
		return fmt.Errorf("task ID %s not found in DLQ", taskID)
	}

	// Reset execution metrics
	targetTask.Retry = 0
	targetTask.LastError = ""

	// Re-enqueue the task
	err = c.Enqueue(ctx, targetTask)
	if err != nil {
		return err
	}

	// Remove from DLQ on successful re-enqueue
	return c.rdb.ZRem(ctx, dlqKey, targetMember).Err()
}
