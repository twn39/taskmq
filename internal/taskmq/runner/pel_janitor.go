package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/events"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

type pelRecoveryJanitor struct {
	rdb           redis.UniversalClient
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
	events        *events.Publisher
	codec         codec.Codec
}

// PELJanitorOption configures optional PEL recovery behaviour.
type PELJanitorOption func(*pelRecoveryJanitor)

// WithPELCodec enables best-effort task id/name extraction for stalled events.
func WithPELCodec(c codec.Codec) PELJanitorOption {
	return func(j *pelRecoveryJanitor) { j.codec = c }
}

// WithPELEventsPublisher overrides the default events publisher (maxLen from NewPublisher).
func WithPELEventsPublisher(p *events.Publisher) PELJanitorOption {
	return func(j *pelRecoveryJanitor) { j.events = p }
}

// WithPELEventsMaxLen sets the stalled-events stream MAXLEN (0 = DefaultMaxLen).
func WithPELEventsMaxLen(maxLen int64) PELJanitorOption {
	return func(j *pelRecoveryJanitor) {
		if j.rdb != nil {
			j.events = events.NewPublisher(j.rdb, maxLen)
		}
	}
}

func NewPELRecoveryJanitor(
	rdb redis.UniversalClient,
	logger *zap.Logger,
	queue string,
	group string,
	consumer string,
	concurrency int,
	checkInterval time.Duration,
	minIdleTime time.Duration,
	processFn func(ctx context.Context, msg redis.XMessage),
	opts ...PELJanitorOption,
) PELRecoveryJanitor {
	j := &pelRecoveryJanitor{
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
		events:        events.NewPublisher(rdb, 0),
	}
	for _, o := range opts {
		o(j)
	}
	return j
}

func (j *pelRecoveryJanitor) RegisterProcessor(fn func(ctx context.Context, msg redis.XMessage)) {
	j.processFnMu.Lock()
	defer j.processFnMu.Unlock()
	j.processFn = fn
}

// emitStalled publishes a best-effort TypeStalled event for dashboards/feeds.
// Failures never block reclaim or reprocess.
func (j *pelRecoveryJanitor) emitStalled(ctx context.Context, msg redis.XMessage, deliveryCount int64) {
	if j == nil || j.events == nil {
		return
	}
	taskID, name := j.taskMetaFromMessage(msg)
	if taskID == "" {
		taskID = msg.ID
	}
	j.events.Emit(ctx, j.queue, events.TypeStalled, taskID, name,
		fmt.Sprintf("reclaimed delivery_count=%d stream_id=%s", deliveryCount, msg.ID))
}

func (j *pelRecoveryJanitor) taskMetaFromMessage(msg redis.XMessage) (id, name string) {
	if j.codec == nil || msg.Values == nil {
		return "", ""
	}
	payload, ok := pelValueAsBytes(msg.Values["task"])
	if !ok {
		payload, ok = pelValueAsBytes(msg.Values["payload"])
	}
	if !ok {
		return "", ""
	}
	var t taskmodel.Task
	if err := j.codec.Unmarshal(payload, &t); err != nil {
		return "", ""
	}
	return t.ID, t.Name
}

func pelValueAsBytes(v interface{}) ([]byte, bool) {
	switch t := v.(type) {
	case []byte:
		return t, true
	case string:
		return codec.UnsafeStringToBytes(t), true
	default:
		return nil, false
	}
}

// Run launches the PEL auto-claim and recovery loop
func (j *pelRecoveryJanitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(j.checkInterval)
	defer ticker.Stop()

	minIdleTime := j.minIdleTime
	streamKey := keys.KeysFor(j.queue).Stream()

	// Semaphore to limit concurrent processing of reclaimed tasks
	sem := make(chan struct{}, j.concurrency)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Determine dynamic batch count based on worker concurrency (floor 10, ceiling 100)
			claimCount := int64(j.concurrency)
			if claimCount < 10 {
				claimCount = 10
			} else if claimCount > 100 {
				claimCount = 100
			}

			// Claim stalled messages via XAutoClaim with cursor-based pagination
			claimed, nextCursor, err := j.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamKey,
				Group:    j.group,
				Consumer: j.consumer,
				MinIdle:  minIdleTime,
				Start:    j.startCursor,
				Count:    claimCount,
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

				// Batch query XPendingExt for delivery counts (RetryCount)
				var pends []redis.XPendingExt
				var pendErr error
				if len(claimed) == 1 {
					pends, pendErr = j.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
						Stream:   streamKey,
						Group:    j.group,
						Start:    claimed[0].ID,
						End:      claimed[0].ID,
						Count:    1,
						Consumer: j.consumer,
					}).Result()
				} else {
					pends, pendErr = j.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
						Stream:   streamKey,
						Group:    j.group,
						Start:    claimed[0].ID,
						End:      claimed[len(claimed)-1].ID,
						Count:    int64(len(claimed)),
						Consumer: j.consumer,
					}).Result()
				}

				pendingMap := make(map[string]int64)
				if pendErr == nil {
					for _, p := range pends {
						pendingMap[p.ID] = p.RetryCount
					}
				} else {
					j.logger.Error("Janitor failed to batch query pending message delivery counts", zap.Error(pendErr))
				}

				for _, msg := range claimed {
					deliveryCount, ok := pendingMap[msg.ID]
					if !ok {
						deliveryCount = 1 // Fallback
					}

					if msg.Values == nil {
						msg.Values = make(map[string]interface{})
					}
					msg.Values["__delivery_count"] = deliveryCount

					// Best-effort observability: emit stalled before reprocess.
					j.emitStalled(ctx, msg, deliveryCount)

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
