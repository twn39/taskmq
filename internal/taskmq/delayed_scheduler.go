package taskmq

import (
	"context"
	"math/rand"
	"strconv"
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

// Run launches the delayed task scheduler loop with adaptive sleep and Pub/Sub wakeup.
//
// Design:
//   - A timer wakes the scheduler at the time of the earliest pending task.
//   - A Pub/Sub channel allows external callers (Enqueue, ScheduleRetry, etc.) to preempt
//     the timer immediately when a new task is enqueued.
//   - Between wakeups the scheduler sleeps adaptively (up to maxSleep), eliminating the
//     old busy-poll pattern completely.
func (s *delayedScheduler) Run(ctx context.Context) error {
	keys := KeysFor(s.queue)
	delayedKey := keys.Delayed()
	streamKey := keys.Stream()
	wakeupChannel := keys.DelayedWakeupChannel()

	// Lua script: atomically move ready tasks (score <= nowMs) from ZSET to Stream.
	// Returns the moved members so caller can trigger cron reschedule logic.
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

	const batchSize = 100
	const maxSleep = 10 * time.Second
	// minSleep prevents a tight busy-loop when calculateSleep() would return 0
	// (e.g. there are more overdue tasks than a single batch can handle).
	const minSleep = 1 * time.Millisecond

	// promoteReadyTasks moves all tasks whose run-time has arrived into the active stream.
	// It loops in batches until no more ready tasks remain, so a single wake-up is always
	// sufficient to drain any burst of overdue tasks.
	promoteReadyTasks := func() {
		for {
			nowMs := time.Now().UnixMilli()
			res, err := s.rdb.Eval(ctx, luaScript, []string{delayedKey, streamKey}, 0, nowMs, batchSize).Result()
			if err != nil {
				if ctx.Err() == nil {
					s.logger.Error("Scheduler failed to poll delayed tasks", zap.Error(err))
				}
				return
			}

			elements, ok := res.([]interface{})
			if !ok || len(elements) == 0 {
				return
			}

			s.logger.Debug("Scheduler moved tasks from delayed to active", zap.Int("count", len(elements)))
			for _, el := range elements {
				memberStr, ok := el.(string)
				if !ok {
					continue
				}
				task := &Task{}
				if err := s.codec.Unmarshal(unsafeStringToBytes(memberStr), task); err == nil && task.CronSpec != "" {
					_ = s.cronManager.Reschedule(ctx, task)
				}
			}

			// Full batch returned – there may be more ready tasks; loop again.
			if len(elements) < batchSize {
				return
			}
		}
	}

	// calculateSleep peeks at the earliest ZSET entry and returns how long to sleep.
	// It never returns 0 (uses minSleep instead) to prevent a busy-loop.
	calculateSleep := func() time.Duration {
		val, err := s.rdb.ZRangeWithScores(ctx, delayedKey, 0, 0).Result()
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Error("Scheduler failed to peek delayed ZSET", zap.Error(err))
			}
			return s.pollInterval
		}

		if len(val) == 0 {
			return maxSleep
		}

		earliestMs := int64(val[0].Score)
		nowMs := time.Now().UnixMilli()

		if earliestMs <= nowMs {
			// Already overdue but we couldn't fully drain it (e.g. > batchSize tasks).
			// Return minSleep so we loop back very quickly without spinning at 100% CPU.
			return minSleep
		}

		diff := time.Duration(earliestMs-nowMs) * time.Millisecond
		if diff > maxSleep {
			return maxSleep
		}
		return diff
	}

	// Subscribe before the initial promotion so we never miss a wakeup published
	// between startup and the first sleep.
	pubsub := s.rdb.Subscribe(ctx, wakeupChannel)
	defer pubsub.Close()
	ch := pubsub.Channel()

	// Perform initial promotion and set up the timer for the first sleep.
	promoteReadyTasks()
	sleepDuration := calculateSleep()
	nextWakeupMs := time.Now().UnixMilli() + sleepDuration.Milliseconds()

	timer := time.NewTimer(sleepDuration)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	// resetTimer safely stops and resets the timer without a drain race.
	resetTimer := func(d time.Duration) {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(d)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timer.C:
			// Timer fired at the expected task run time.
			promoteReadyTasks()
			sleepDuration = calculateSleep()
			nextWakeupMs = time.Now().UnixMilli() + sleepDuration.Milliseconds()
			timer.Reset(sleepDuration)

		case msg, ok := <-ch:
			if !ok {
				// go-redis closes the channel on disconnect; reconnect and continue
				// rather than returning an error that would kill the goroutine.
				s.logger.Warn("Delayed scheduler: PubSub channel closed, refreshing subscription")
				pubsub.Close()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				pubsub = s.rdb.Subscribe(ctx, wakeupChannel)
				ch = pubsub.Channel()
				continue
			}

			newTimeMs, err := strconv.ParseInt(msg.Payload, 10, 64)
			if err != nil {
				continue
			}

			// Preemption: only act if the newly-enqueued task runs sooner than our
			// current scheduled wakeup. Tasks farther in the future don't need us
			// to act now – the existing timer will handle them.
			if newTimeMs >= nextWakeupMs {
				continue
			}

			// Small jitter (0-50ms) to reduce thundering-herd in clustered deployments.
			jitter := time.Duration(rand.Intn(50)) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(jitter):
			}

			promoteReadyTasks()
			sleepDuration = calculateSleep()
			nextWakeupMs = time.Now().UnixMilli() + sleepDuration.Milliseconds()
			resetTimer(sleepDuration)
		}
	}
}


