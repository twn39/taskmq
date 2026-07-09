package runner

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

// RetentionJanitor performs SafeTrim, DLQ age purge, cancelled-delayed cleanup,
// and idle consumer hygiene. It is separate from PEL recovery.
type RetentionJanitor struct {
	rdb       *redis.Client
	logger    *zap.Logger
	queue     string
	group     string
	codec     codec.Codec
	lifecycle *lifecycle.Lifecycle
}

func NewRetentionJanitor(
	rdb *redis.Client,
	logger *zap.Logger,
	queue string,
	group string,
	codec codec.Codec,
	lifecycle *lifecycle.Lifecycle,
) *RetentionJanitor {
	return &RetentionJanitor{
		rdb:       rdb,
		logger:    logger,
		queue:     queue,
		group:     group,
		codec:     codec,
		lifecycle: lifecycle,
	}
}

// Run launches the retention loop until context cancellation.
func (j *RetentionJanitor) Run(ctx context.Context) error {
	if j.lifecycle == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	cfg := j.lifecycle.Config()
	if !cfg.SafeTrimEnabled && cfg.DLQMaxAge <= 0 && !cfg.PurgeCancelledDelayed && cfg.IdleConsumerTimeout <= 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	interval := cfg.SafeTrimInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run once shortly after start so cold starts and tests see effect quickly.
	j.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			j.tick(ctx)
		}
	}
}

func (j *RetentionJanitor) tick(ctx context.Context) {
	cfg := j.lifecycle.Config()
	if cfg.SafeTrimEnabled {
		if n, err := j.safeTrim(ctx); err != nil {
			if ctx.Err() == nil {
				j.logger.Error("RetentionJanitor safe trim failed",
					zap.String("queue", j.queue),
					zap.Error(err),
				)
			}
		} else if n > 0 {
			j.lifecycle.Metrics().SafeTrimDeletedTotal.Add(n)
			j.logger.Debug("RetentionJanitor safe-trimmed stream entries",
				zap.String("queue", j.queue),
				zap.Int64("deleted", n),
			)
		}
	}
	if cfg.DLQMaxAge > 0 {
		if n, err := j.purgeDLQByAge(ctx, cfg.DLQMaxAge); err != nil {
			if ctx.Err() == nil {
				j.logger.Error("RetentionJanitor DLQ age purge failed",
					zap.String("queue", j.queue),
					zap.Error(err),
				)
			}
		} else if n > 0 {
			j.lifecycle.Metrics().DLQEvictedTotal.Add(n)
		}
	}
	if cfg.PurgeCancelledDelayed {
		if n, err := j.purgeCancelledDelayed(ctx); err != nil {
			if ctx.Err() == nil {
				j.logger.Error("RetentionJanitor cancelled delayed purge failed",
					zap.String("queue", j.queue),
					zap.Error(err),
				)
			}
		} else if n > 0 {
			j.lifecycle.Metrics().CancelledDelayedPurged.Add(n)
		}
	}
	if cfg.IdleConsumerTimeout > 0 {
		if n, err := j.purgeIdleConsumers(ctx, cfg.IdleConsumerTimeout); err != nil {
			if ctx.Err() == nil {
				j.logger.Error("RetentionJanitor idle consumer cleanup failed",
					zap.String("queue", j.queue),
					zap.Error(err),
				)
			}
		} else if n > 0 {
			j.lifecycle.Metrics().IdleConsumersRemovedTotal.Add(n)
		}
	}
}

// safeTrim trims stream entries strictly older than the oldest pending ID
// (or last-delivered when PEL is empty). Uses approximate MINID + LIMIT.
func (j *RetentionJanitor) safeTrim(ctx context.Context) (int64, error) {
	stream := keys.KeysFor(j.queue).Stream()
	minID, ok, err := j.safeTrimThreshold(ctx, stream)
	if err != nil {
		return 0, err
	}
	if !ok || minID == "" || minID == "0-0" {
		return 0, nil
	}
	limit := j.lifecycle.Config().SafeTrimBatchLimit
	if limit <= 0 {
		limit = 1000
	}
	// MINID ~ minID keeps entries with ID >= minID; removes older residual entries.
	return j.rdb.XTrimMinIDApprox(ctx, stream, minID, limit).Result()
}

func (j *RetentionJanitor) safeTrimThreshold(ctx context.Context, stream string) (string, bool, error) {
	pending, err := j.rdb.XPending(ctx, stream, j.group).Result()
	if err != nil {
		if isIgnorableStreamErr(err) {
			return "", false, nil
		}
		return "", false, err
	}
	if pending.Count > 0 && pending.Lower != "" {
		return pending.Lower, true, nil
	}

	groups, err := j.rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		if isIgnorableStreamErr(err) {
			return "", false, nil
		}
		return "", false, err
	}
	for _, g := range groups {
		if g.Name == j.group {
			if g.LastDeliveredID != "" && g.LastDeliveredID != "0-0" {
				return g.LastDeliveredID, true, nil
			}
			break
		}
	}
	return "", false, nil
}

func (j *RetentionJanitor) purgeDLQByAge(ctx context.Context, maxAge time.Duration) (int64, error) {
	keys := keys.KeysFor(j.queue)
	dlqKey := keys.DLQ()
	indexKey := keys.DLQIndex()
	cutoff := time.Now().Add(-maxAge).UnixMilli()

	const batch = 100
	var total int64
	for {
		ids, err := j.rdb.ZRangeByScore(ctx, dlqKey, &redis.ZRangeBy{
			Min:   "-inf",
			Max:   strconv.FormatInt(cutoff, 10),
			Count: batch,
		}).Result()
		if err != nil {
			return total, err
		}
		if len(ids) == 0 {
			return total, nil
		}
		pipe := j.rdb.Pipeline()
		for _, id := range ids {
			pipe.ZRem(ctx, dlqKey, id)
			pipe.HDel(ctx, indexKey, id)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return total, err
		}
		total += int64(len(ids))
		if len(ids) < batch {
			return total, nil
		}
	}
}

func (j *RetentionJanitor) purgeCancelledDelayed(ctx context.Context) (int64, error) {
	delayedKey := keys.KeysFor(j.queue).Delayed()
	const batch int64 = 100
	var cursor int64
	var purged int64

	for {
		members, err := j.rdb.ZRange(ctx, delayedKey, cursor, cursor+batch-1).Result()
		if err != nil {
			return purged, err
		}
		if len(members) == 0 {
			return purged, nil
		}
		for _, member := range members {
			task := &taskmodel.Task{}
			if err := j.codec.Unmarshal(codec.UnsafeStringToBytes(member), task); err != nil {
				continue
			}
			if task.ID == "" {
				continue
			}
			q := task.Queue
			if q == "" {
				q = j.queue
			}
			cancelledKey := keys.KeysFor(q).Cancelled(task.ID)
			exists, err := j.rdb.Exists(ctx, cancelledKey).Result()
			if err != nil || exists == 0 {
				continue
			}
			if err := j.rdb.ZRem(ctx, delayedKey, member).Err(); err != nil {
				return purged, err
			}
			purged++
		}
		if int64(len(members)) < batch {
			return purged, nil
		}
		cursor += batch
		// Cap work per tick for large delayed sets.
		if cursor > 2000 {
			return purged, nil
		}
	}
}

func (j *RetentionJanitor) purgeIdleConsumers(ctx context.Context, timeout time.Duration) (int64, error) {
	stream := keys.KeysFor(j.queue).Stream()
	consumers, err := j.rdb.XInfoConsumers(ctx, stream, j.group).Result()
	if err != nil {
		if isIgnorableStreamErr(err) {
			return 0, nil
		}
		return 0, err
	}
	var removed int64
	for _, c := range consumers {
		// go-redis exposes Idle as time.Duration.
		if c.Pending == 0 && c.Idle >= timeout {
			if _, err := j.rdb.XGroupDelConsumer(ctx, stream, j.group, c.Name).Result(); err != nil {
				j.logger.Debug("failed to delete idle consumer",
					zap.String("consumer", c.Name),
					zap.Error(err),
				)
				continue
			}
			removed++
		}
	}
	return removed, nil
}

func isIgnorableStreamErr(err error) bool {
	if err == nil || err == redis.Nil {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "NOGROUP") ||
		strings.Contains(msg, "no such key") ||
		strings.Contains(msg, "NOPERM")
}

// SafeTrim is an exported alias for safeTrim (used by tests and ops tooling).
func (j *RetentionJanitor) SafeTrim(ctx context.Context) (int64, error) {
	return j.safeTrim(ctx)
}

// PurgeDLQByAge is an exported alias for purgeDLQByAge.
func (j *RetentionJanitor) PurgeDLQByAge(ctx context.Context, maxAge time.Duration) (int64, error) {
	return j.purgeDLQByAge(ctx, maxAge)
}

// PurgeCancelledDelayed is an exported alias for purgeCancelledDelayed.
func (j *RetentionJanitor) PurgeCancelledDelayed(ctx context.Context) (int64, error) {
	return j.purgeCancelledDelayed(ctx)
}

// PurgeIdleConsumers is an exported alias for purgeIdleConsumers.
func (j *RetentionJanitor) PurgeIdleConsumers(ctx context.Context, timeout time.Duration) (int64, error) {
	return j.purgeIdleConsumers(ctx, timeout)
}
