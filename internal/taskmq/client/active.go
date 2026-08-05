package client

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// active implements ActiveTaskManager; cancellation is delegated via TaskCanceler.
type active struct {
	d      deps
	cancel TaskCanceler
}

func (s *active) ListActiveTasks(ctx context.Context, queue string, limit int) ([]*ActiveTask, error) {
	streamKey := keys.KeysFor(queue).Stream()

	msgs, err := s.d.rdb.XRangeN(ctx, streamKey, "-", "+", int64(limit)).Result()
	if err != nil {
		return nil, err
	}

	if len(msgs) == 0 {
		return []*ActiveTask{}, nil
	}

	pelMap := make(map[string]*redis.XPendingExt)
	groups, err := s.d.rdb.XInfoGroups(ctx, streamKey).Result()
	if err == nil {
		for _, g := range groups {
			pends, err := s.d.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: streamKey,
				Group:  g.Name,
				Start:  "-",
				End:    "+",
				Count:  int64(limit * 2),
			}).Result()
			if err == nil {
				for i := range pends {
					pelMap[pends[i].ID] = &pends[i]
				}
			}
		}
	}

	tasks := make([]*ActiveTask, 0, len(msgs))
	for _, msg := range msgs {
		var serialized []byte
		if valStr, ok := msg.Values["task"].(string); ok {
			serialized = []byte(valStr)
		} else if valBytes, ok := msg.Values["task"].([]byte); ok {
			serialized = valBytes
		} else {
			continue
		}

		task := &taskmodel.Task{}
		if err := s.d.codec.Unmarshal(serialized, task); err != nil {
			continue
		}

		status := "Pending"
		var consumer string
		var deliveries int64

		if p, isPending := pelMap[msg.ID]; isPending {
			status = "Processing"
			consumer = p.Consumer
			deliveries = p.RetryCount
		}

		tasks = append(tasks, &ActiveTask{
			Task:       task,
			StreamID:   msg.ID,
			Status:     status,
			Consumer:   consumer,
			Deliveries: deliveries,
			EnqueuedAt: parseStreamTime(msg.ID),
		})
	}

	return tasks, nil
}

func (s *active) DeleteActiveTask(ctx context.Context, queue string, streamID string) error {
	qk := keys.KeysFor(queue)
	streamKey := qk.Stream()

	msgs, err := s.d.rdb.XRangeN(ctx, streamKey, streamID, streamID, 1).Result()
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return fmt.Errorf("taskmq: active task not found in stream: %s", streamID)
	}

	var serialized []byte
	if valStr, ok := msgs[0].Values["task"].(string); ok {
		serialized = []byte(valStr)
	} else if valBytes, ok := msgs[0].Values["task"].([]byte); ok {
		serialized = valBytes
	}

	task := &taskmodel.Task{}
	if uerr := s.d.codec.Unmarshal(serialized, task); uerr == nil {
		if task.ID != "" && s.cancel != nil {
			_ = s.cancel.CancelTask(ctx, queue, task.ID)
		}
		if task.UniqueKey != "" {
			lockKey := qk.Unique(task.UniqueKey)
			_ = s.d.rdb.Del(ctx, lockKey).Err()
		}
	}

	groups, err := s.d.rdb.XInfoGroups(ctx, streamKey).Result()
	if err == nil {
		for _, g := range groups {
			_ = s.d.rdb.XAck(ctx, streamKey, g.Name, streamID).Err()
		}
	}

	return s.d.rdb.XDel(ctx, streamKey, streamID).Err()
}

func parseStreamTime(streamID string) time.Time {
	parts := strings.Split(streamID, "-")
	if len(parts) > 0 {
		if ms, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
			return time.UnixMilli(ms)
		}
	}
	return time.Time{}
}
