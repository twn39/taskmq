package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// scheduled implements ScheduledTaskManager.
type scheduled struct {
	d deps
}

func (s *scheduled) ListScheduledTasks(ctx context.Context, queue string, limit int) ([]*ScheduledTask, error) {
	delayedKey := keys.KeysFor(queue).Delayed()
	zs, err := s.d.rdb.ZRangeWithScores(ctx, delayedKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	out := make([]*ScheduledTask, 0, len(zs))
	for _, z := range zs {
		task := &taskmodel.Task{}
		memberStr, ok := z.Member.(string)
		if !ok {
			continue
		}
		if err := s.d.codec.Unmarshal([]byte(memberStr), task); err != nil {
			continue
		}
		out = append(out, &ScheduledTask{
			Task:  task,
			RunAt: time.UnixMilli(int64(z.Score)),
		})
	}
	return out, nil
}

func (s *scheduled) RunScheduledTask(ctx context.Context, queue string, taskID string) error {
	qk := keys.KeysFor(queue)
	delayedKey := qk.Delayed()
	members, err := s.d.rdb.ZRange(ctx, delayedKey, 0, -1).Result()
	if err != nil {
		return err
	}

	var targetMember string
	for _, m := range members {
		task := &taskmodel.Task{}
		if err := s.d.codec.Unmarshal([]byte(m), task); err == nil {
			if task.ID == taskID {
				targetMember = m
				break
			}
		}
	}

	if targetMember == "" {
		return fmt.Errorf("taskmq: task not found in scheduled tasks: %s", taskID)
	}

	// Atomic promote under hard/MAXLEN: if stream full, member stays delayed.
	if err := s.d.lifecycle.ForcePromoteMember(ctx, s.d.rdb, delayedKey, qk.Stream(), targetMember); err != nil {
		if errors.Is(err, lifecycle.ErrMemberGone) {
			return fmt.Errorf("taskmq: task already processed or deleted: %s", taskID)
		}
		return err
	}
	return nil
}

func (s *scheduled) DeleteScheduledTask(ctx context.Context, queue string, taskID string) error {
	qk := keys.KeysFor(queue)
	delayedKey := qk.Delayed()
	members, err := s.d.rdb.ZRange(ctx, delayedKey, 0, -1).Result()
	if err != nil {
		return err
	}

	var targetMember string
	var targetTask *taskmodel.Task
	for _, m := range members {
		task := &taskmodel.Task{}
		if err := s.d.codec.Unmarshal([]byte(m), task); err == nil {
			if task.ID == taskID {
				targetMember = m
				targetTask = task
				break
			}
		}
	}

	if targetMember == "" {
		return fmt.Errorf("taskmq: task not found in scheduled tasks: %s", taskID)
	}

	res, err := s.d.rdb.ZRem(ctx, delayedKey, targetMember).Result()
	if err != nil {
		return err
	}
	if res == 0 {
		return fmt.Errorf("taskmq: task already processed or deleted: %s", taskID)
	}

	if targetTask.UniqueKey != "" {
		lockKey := qk.Unique(targetTask.UniqueKey)
		_ = s.d.rdb.Del(ctx, lockKey).Err()
	}
	return nil
}
