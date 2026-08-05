package runner

import (
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"go.uber.org/zap"
)

// DefaultCronManagerFactory constructs the standard cron self-healing manager.
// lc may be nil (delayed capacity checks disabled).
func DefaultCronManagerFactory(rdb redis.UniversalClient, logger *zap.Logger, queue string, c codec.Codec, healingInterval time.Duration, healingLockTTL time.Duration, scanBatchSize int, scanMaxCount int, lc *lifecycle.Lifecycle) CronManager {
	return NewCronManager(rdb, logger, queue, c, healingInterval, healingLockTTL, scanBatchSize, scanMaxCount, lc)
}

// DefaultSchedulerFactory constructs the standard delayed scheduler.
// streamHardLimit is EnqueueHardLimit; streamMaxLen is StreamMaxLen (both 0 = off).
func DefaultSchedulerFactory(rdb redis.UniversalClient, logger *zap.Logger, queue string, cronManager CronManager, c codec.Codec, pollInterval time.Duration, streamHardLimit, streamMaxLen int64) Runner {
	return NewDelayedScheduler(rdb, logger, queue, cronManager, c, pollInterval, streamHardLimit, streamMaxLen)
}

// DefaultJanitorFactory constructs the standard PEL recovery janitor with stalled events.
func DefaultJanitorFactory(rdb redis.UniversalClient, logger *zap.Logger, queue string, group string, consumer string, concurrency int, checkInterval time.Duration, minIdleTime time.Duration) PELRecoveryJanitor {
	return NewPELRecoveryJanitor(rdb, logger, queue, group, consumer, concurrency, checkInterval, minIdleTime, nil)
}

// NewPELRecoveryJanitorWithCodec constructs a PEL janitor that decodes task id/name for stalled events.
func NewPELRecoveryJanitorWithCodec(
	rdb redis.UniversalClient,
	logger *zap.Logger,
	queue string,
	group string,
	consumer string,
	concurrency int,
	checkInterval time.Duration,
	minIdleTime time.Duration,
	c codec.Codec,
	eventsMaxLen int64,
) PELRecoveryJanitor {
	opts := []PELJanitorOption{WithPELCodec(c)}
	if eventsMaxLen > 0 {
		opts = append(opts, WithPELEventsMaxLen(eventsMaxLen))
	}
	return NewPELRecoveryJanitor(rdb, logger, queue, group, consumer, concurrency, checkInterval, minIdleTime, nil, opts...)
}

// DefaultRetentionJanitorFactory constructs the retention janitor from lifecycle policy.
func DefaultRetentionJanitorFactory(rdb redis.UniversalClient, logger *zap.Logger, queue string, group string, c codec.Codec, lc *lifecycle.Lifecycle) Runner {
	return NewRetentionJanitor(rdb, logger, queue, group, c, lc)
}
