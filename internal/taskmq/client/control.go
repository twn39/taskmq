package client

import (
	"context"
	"fmt"
	"time"

	"github.com/twn39/taskmq/internal/taskmq/keys"
)

// control implements QueueController and TaskCanceler.
type control struct {
	d deps
}

func (s *control) CancelTask(ctx context.Context, queue, taskID string) error {
	qk := keys.KeysFor(queue)
	cancelledKey := qk.Cancelled(taskID)
	ttl := 24 * time.Hour
	if s.d.lifecycle != nil {
		if v := s.d.lifecycle.Config().CancelledTTL; v > 0 {
			ttl = v
		}
	}
	if err := s.d.rdb.Set(ctx, cancelledKey, "1", ttl).Err(); err != nil {
		return fmt.Errorf("taskmq: failed to set cancel marker: %w", err)
	}

	return s.d.rdb.Publish(ctx, qk.CancelChannel(), taskID).Err()
}

func (s *control) Pause(ctx context.Context, queue string) error {
	qk := keys.KeysFor(queue)
	if err := s.d.rdb.Set(ctx, qk.Paused(), "1", 0).Err(); err != nil {
		return fmt.Errorf("taskmq: failed to set pause marker: %w", err)
	}
	return s.d.rdb.Publish(ctx, qk.Control(), "pause").Err()
}

func (s *control) Resume(ctx context.Context, queue string) error {
	qk := keys.KeysFor(queue)
	if err := s.d.rdb.Del(ctx, qk.Paused()).Err(); err != nil {
		return fmt.Errorf("taskmq: failed to delete pause marker: %w", err)
	}
	return s.d.rdb.Publish(ctx, qk.Control(), "resume").Err()
}

func (s *control) IsPaused(ctx context.Context, queue string) (bool, error) {
	val, err := s.d.rdb.Exists(ctx, keys.KeysFor(queue).Paused()).Result()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}
