package taskmq

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type pelRecoveryJanitor struct {
	rdb         *redis.Client
	logger      *zap.Logger
	queue       string
	group       string
	consumer    string
	startCursor string
	concurrency int
	processFn   func(ctx context.Context, msg redis.XMessage)
}

func newPELRecoveryJanitor(
	rdb *redis.Client,
	logger *zap.Logger,
	queue string,
	group string,
	consumer string,
	concurrency int,
	processFn func(ctx context.Context, msg redis.XMessage),
) Runner {
	return &pelRecoveryJanitor{
		rdb:         rdb,
		logger:      logger,
		queue:       queue,
		group:       group,
		consumer:    consumer,
		startCursor: "0-0",
		concurrency: concurrency,
		processFn:   processFn,
	}
}

// Run launches the PEL auto-claim and recovery loop
func (j *pelRecoveryJanitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	minIdleTime := 5 * time.Second
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
							j.processFn(ctx, m)
						}(msg)
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
	}
}
