package taskmq

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type delayedScheduler struct {
	rdb          *redis.Client
	logger       *zap.Logger
	queue        string
	cronManager  CronManager
	codec        Codec
	pollInterval time.Duration
}

func newDelayedScheduler(rdb *redis.Client, logger *zap.Logger, queue string, cronManager CronManager, codec Codec, pollInterval time.Duration) Runner {
	return &delayedScheduler{
		rdb:          rdb,
		logger:       logger,
		queue:        queue,
		cronManager:  cronManager,
		codec:        codec,
		pollInterval: pollInterval,
	}
}

// Run launches the delayed task scheduler loop
func (s *delayedScheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	keys := KeysFor(s.queue)
	delayedKey := keys.Delayed()
	streamKey := keys.Stream()

	// Lua script moves ready tasks from ZSET to Stream and removes them from ZSET, returning the moved elements
	luaScript := `
		local elements = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', 0, ARGV[3])
		if #elements > 0 then
			for i, member in ipairs(elements) do
				redis.call('XADD', KEYS[2], '*', 'task', member)
			end
			for i, member in ipairs(elements) do
				redis.call('ZREM', KEYS[1], member)
			end
		end
		return elements
	`

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			nowMs := time.Now().UnixMilli()

			res, err := s.rdb.Eval(ctx, luaScript, []string{delayedKey, streamKey}, 0, nowMs, 100).Result()
			if err != nil {
				s.logger.Error("Scheduler failed to poll delayed tasks", zap.Error(err))
				continue
			}

			elements, ok := res.([]interface{})
			if !ok {
				continue
			}

			if len(elements) > 0 {
				s.logger.Debug("Scheduler moved tasks from delayed to active", zap.Int("count", len(elements)))
				for _, el := range elements {
					memberStr, ok := el.(string)
					if !ok {
						continue
					}
					task := &Task{}
					err := s.codec.Unmarshal(unsafeStringToBytes(memberStr), task)
					if err == nil && task.CronSpec != "" {
						_ = s.cronManager.Reschedule(ctx, task)
					}
				}
			}
		}
	}
}
