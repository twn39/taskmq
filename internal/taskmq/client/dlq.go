package client

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

//go:embed scripts/delete_dead_letter.lua
var deleteDeadLetterScript string

var deleteDeadLetterCmd = redis.NewScript(deleteDeadLetterScript)

// dlq implements DLQManager and depends only on EnqueueClient for retries.
type dlq struct {
	d   deps
	enq EnqueueClient
}

// ListDeadLetters returns dead-letter tasks sorted by descending death time.
func (s *dlq) ListDeadLetters(ctx context.Context, queue string, limit int) ([]*taskmodel.Task, error) {
	dlqKey := keys.KeysFor(queue).DLQ()
	taskIDs, err := s.d.rdb.ZRevRange(ctx, dlqKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	if len(taskIDs) == 0 {
		return nil, nil
	}

	dlqIndexKey := keys.KeysFor(queue).DLQIndex()
	members, err := s.d.rdb.HMGet(ctx, dlqIndexKey, taskIDs...).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*taskmodel.Task, 0, len(members))
	for _, m := range members {
		if m == nil {
			continue
		}
		str, ok := m.(string)
		if !ok {
			continue
		}
		task := &taskmodel.Task{}
		err := s.d.codec.Unmarshal(codec.UnsafeStringToBytes(str), task)
		if err != nil {
			continue // skip corrupted data
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// DeleteDeadLetter removes a specific task from the dead-letter queue by task ID.
func (s *dlq) DeleteDeadLetter(ctx context.Context, queue string, taskID string) error {
	qk := keys.KeysFor(queue)
	_, err := deleteDeadLetterCmd.Run(ctx, s.d.rdb, []string{qk.DLQ(), qk.DLQIndex()}, taskID).Result()
	if err == redis.Nil {
		return nil
	}
	return err
}

// RetryDeadLetter resets retry counts and re-enqueues a dead-letter task.
func (s *dlq) RetryDeadLetter(ctx context.Context, queue string, taskID string) error {
	dlqIndexKey := keys.KeysFor(queue).DLQIndex()

	serialized, err := s.d.rdb.HGet(ctx, dlqIndexKey, taskID).Result()
	if err == redis.Nil {
		return fmt.Errorf("task ID %s not found in DLQ", taskID)
	} else if err != nil {
		return err
	}

	task := &taskmodel.Task{}
	err = s.d.codec.Unmarshal(codec.UnsafeStringToBytes(serialized), task)
	if err != nil {
		return fmt.Errorf("failed to unmarshal dead letter task: %w", err)
	}

	task.Retry = 0
	task.LastError = ""

	if err := s.enq.Enqueue(ctx, task); err != nil {
		return err
	}

	return s.DeleteDeadLetter(ctx, queue, taskID)
}

// RetryAllDeadLetters retries all DLQ tasks, returning how many succeeded.
func (s *dlq) RetryAllDeadLetters(ctx context.Context, queue string) (int64, error) {
	qk := keys.KeysFor(queue)
	dlqKey := qk.DLQ()
	dlqIndexKey := qk.DLQIndex()

	var totalRetried int64
	for {
		taskIDs, err := s.d.rdb.ZRange(ctx, dlqKey, 0, 99).Result()
		if err != nil {
			return totalRetried, err
		}
		if len(taskIDs) == 0 {
			break
		}

		members, err := s.d.rdb.HMGet(ctx, dlqIndexKey, taskIDs...).Result()
		if err != nil {
			return totalRetried, err
		}

		for i, m := range members {
			taskID := taskIDs[i]
			if m == nil {
				_ = s.DeleteDeadLetter(ctx, queue, taskID)
				continue
			}
			str, ok := m.(string)
			if !ok {
				_ = s.DeleteDeadLetter(ctx, queue, taskID)
				continue
			}
			task := &taskmodel.Task{}
			if err := s.d.codec.Unmarshal(codec.UnsafeStringToBytes(str), task); err != nil {
				_ = s.DeleteDeadLetter(ctx, queue, taskID)
				continue
			}

			task.Retry = 0
			task.LastError = ""

			if err := s.enq.Enqueue(ctx, task); err != nil {
				return totalRetried, err
			}

			if err := s.DeleteDeadLetter(ctx, queue, task.ID); err != nil {
				return totalRetried, err
			}
			totalRetried++
		}
	}
	return totalRetried, nil
}

// PurgeAllDeadLetters deletes the entire DLQ for a queue.
func (s *dlq) PurgeAllDeadLetters(ctx context.Context, queue string) (int64, error) {
	qk := keys.KeysFor(queue)
	dlqKey := qk.DLQ()
	dlqIndexKey := qk.DLQIndex()

	count, err := s.d.rdb.ZCard(ctx, dlqKey).Result()
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}

	err = s.d.rdb.Del(ctx, dlqKey, dlqIndexKey).Err()
	if err != nil {
		return 0, err
	}
	return count, nil
}
