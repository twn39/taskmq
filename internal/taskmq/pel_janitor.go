package taskmq

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type pelRecoveryJanitor struct {
	rdb           *redis.Client
	logger        *zap.Logger
	queue         string
	group         string
	consumer      string
	startCursor   string
	concurrency   int
	checkInterval time.Duration
	minIdleTime   time.Duration
	processFn     func(ctx context.Context, msg redis.XMessage)
	processFnMu   sync.RWMutex
}

func newPELRecoveryJanitor(
	rdb *redis.Client,
	logger *zap.Logger,
	queue string,
	group string,
	consumer string,
	concurrency int,
	checkInterval time.Duration,
	minIdleTime time.Duration,
	processFn func(ctx context.Context, msg redis.XMessage),
) PELRecoveryJanitor {
	return &pelRecoveryJanitor{
		rdb:           rdb,
		logger:        logger,
		queue:         queue,
		group:         group,
		consumer:      consumer,
		startCursor:   "0-0",
		concurrency:   concurrency,
		checkInterval: checkInterval,
		minIdleTime:   minIdleTime,
		processFn:     processFn,
	}
}

func (j *pelRecoveryJanitor) RegisterProcessor(fn func(ctx context.Context, msg redis.XMessage)) {
	j.processFnMu.Lock()
	defer j.processFnMu.Unlock()
	j.processFn = fn
}

// Run launches the PEL auto-claim and recovery loop
func (j *pelRecoveryJanitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(j.checkInterval)
	defer ticker.Stop()

	minIdleTime := j.minIdleTime
	streamKey := StreamKey(j.queue)

	// Semaphore to limit concurrent processing of reclaimed tasks
	sem := make(chan struct{}, j.concurrency)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Claim stalled messages via XAutoClaim with cursor-based pagination
			claimed, nextCursor, err := j.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamKey,
				Group:    j.group,
				Consumer: j.consumer,
				MinIdle:  minIdleTime,
				Start:    j.startCursor,
				Count:    10,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					continue
				}
				j.logger.Error("Janitor failed to auto-claim stalled messages", zap.Error(err))
				continue
			}

			// Update pagination cursor to next start ID
			if nextCursor != "" {
				j.startCursor = nextCursor
			}

			if len(claimed) > 0 {
				j.logger.Warn("Janitor reclaimed stalled active tasks from PEL", zap.Int("count", len(claimed)))
				for _, msg := range claimed {
					// Acquire semaphore slot or wait if limit reached
					select {
					case sem <- struct{}{}:
						go func(m redis.XMessage) {
							defer func() { <-sem }()

							j.processFnMu.RLock()
							processFn := j.processFn
							j.processFnMu.RUnlock()

							if processFn != nil {
								processFn(ctx, m)
							}
						}(msg)
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
	}
}
