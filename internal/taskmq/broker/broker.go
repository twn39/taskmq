package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/task"
)

// TaskBroker abstracts all atomic queue storage transitions.
type TaskBroker interface {
	MoveToDLQ(ctx context.Context, t *task.Task, streamKey, msgID, group string, dlqQueueName string) error
	ScheduleRetry(ctx context.Context, t *task.Task, streamKey, msgID, group string, runAt time.Time) error
	DeferRateLimitedTask(ctx context.Context, msgID string, t *task.Task, group string, runAt time.Time) error
	ReleaseUniqueLock(ctx context.Context, t *task.Task) error
	RenewUniqueLock(ctx context.Context, t *task.Task, ttl time.Duration) error
	CompleteTask(ctx context.Context, t *task.Task, streamKey, msgID, group string) error
}

type redisBroker struct {
	rdb       *redis.Client
	codec     codec.Codec
	lifecycle *lifecycle.Lifecycle
}

// NewRedisBroker creates a TaskBroker. Optional lifecycle controls DLQ capacity.
func NewRedisBroker(rdb *redis.Client, c codec.Codec, lc ...*lifecycle.Lifecycle) TaskBroker {
	var life *lifecycle.Lifecycle
	if len(lc) > 0 {
		life = lc[0]
	}
	return &redisBroker{
		rdb:       rdb,
		codec:     c,
		lifecycle: life,
	}
}

const luaHandleFailure = `
	local targetKey = KEYS[1]
	local streamKey = KEYS[2]
	local lockKey = KEYS[3]
	local dlqIndexKey = KEYS[4]
	local action = ARGV[1]
	local msgId = ARGV[2]
	local groupName = ARGV[3]
	local score = tonumber(ARGV[4])
	local serializedTask = ARGV[5]
	local expectedLockVal = ARGV[6]
	local taskId = ARGV[7]
	local maxCount = tonumber(ARGV[8]) or 0
	-- ARGV[9]: delayed max count (retry path only); ARGV[10]: overflow 0=reject 1=drop_farthest
	-- Retry is a system requeue: callers pass overflow=1 so capacity never rejects after XACK.
	local delayedMax = tonumber(ARGV[9]) or 0
	local delayedOverflow = tonumber(ARGV[10]) or 0

	-- Ack and delete the processed message from stream
	redis.call("XACK", streamKey, groupName, msgId)
	redis.call("XDEL", streamKey, msgId)

	-- Add to DLQ or Retry Delayed ZSet
	if action == "dlq" then
		redis.call("ZADD", targetKey, score, taskId)

		-- Set the secondary index in DLQ Hash
		if dlqIndexKey ~= nil and dlqIndexKey ~= "" and taskId ~= nil and taskId ~= "" then
			redis.call("HSET", dlqIndexKey, taskId, serializedTask)
		end

		-- Capacity protection: keep only latest maxCount DLQ items (0 = unlimited).
		local evicted = 0
		if maxCount > 0 and dlqIndexKey ~= nil and dlqIndexKey ~= "" then
			local toRemove = redis.call("ZRANGE", targetKey, 0, -(maxCount + 1))
			for _, id in ipairs(toRemove) do
				redis.call("HDEL", dlqIndexKey, id)
				evicted = evicted + 1
			end
			redis.call("ZREMRANGEBYRANK", targetKey, 0, -(maxCount + 1))
		end

		if lockKey ~= "" and expectedLockVal ~= "" then
			if redis.call("GET", lockKey) == expectedLockVal then
				redis.call("DEL", lockKey)
			end
		end
		return evicted
	else
		-- Retry → delayed ZSET with optional capacity (drop_farthest for system path).
		if delayedMax > 0 then
			local n = redis.call("ZCARD", targetKey)
			if n >= delayedMax then
				if delayedOverflow == 1 then
					local toDrop = n - delayedMax + 1
					if toDrop > 0 then
						redis.call("ZREMRANGEBYRANK", targetKey, -toDrop, -1)
					end
				end
				-- reject policy is intentionally ignored for system retries (would lose task).
			end
		end
		redis.call("ZADD", targetKey, score, serializedTask)
	end
	return 0
`

const luaDeferRateLimitedTask = `
	local delayedKey = KEYS[1]
	local streamKey = KEYS[2]
	local groupName = ARGV[1]
	local msgId = ARGV[2]
	local score = tonumber(ARGV[3])
	local serializedTask = ARGV[4]
	local delayedMax = tonumber(ARGV[5]) or 0
	-- System requeue: always drop_farthest when over capacity (never reject after XACK).
	local delayedOverflow = tonumber(ARGV[6]) or 1

	redis.call("XACK", streamKey, groupName, msgId)
	redis.call("XDEL", streamKey, msgId)
	if delayedMax > 0 then
		local n = redis.call("ZCARD", delayedKey)
		if n >= delayedMax then
			if delayedOverflow == 1 then
				local toDrop = n - delayedMax + 1
				if toDrop > 0 then
					redis.call("ZREMRANGEBYRANK", delayedKey, -toDrop, -1)
				end
			end
		end
	end
	redis.call("ZADD", delayedKey, score, serializedTask)
	return 1
`

const luaUnlock = `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`

const luaCompleteTask = `
	local streamKey = KEYS[1]
	local lockKey = KEYS[2]
	local msgId = ARGV[1]
	local groupName = ARGV[2]
	local expectedLockVal = ARGV[3]

	redis.call("XACK", streamKey, groupName, msgId)
	redis.call("XDEL", streamKey, msgId)

	if lockKey ~= "" and expectedLockVal ~= "" then
		if redis.call("GET", lockKey) == expectedLockVal then
			redis.call("DEL", lockKey)
		end
	end
	return 1
`

const luaRenewUniqueLock = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`

var (
	handleFailureCmd        = redis.NewScript(luaHandleFailure)
	deferRateLimitedTaskCmd = redis.NewScript(luaDeferRateLimitedTask)
	unlockCmd               = redis.NewScript(luaUnlock)
	completeTaskCmd         = redis.NewScript(luaCompleteTask)
	renewUniqueLockCmd      = redis.NewScript(luaRenewUniqueLock)
)

func (b *redisBroker) dlqMaxCount() int64 {
	// nil-safe Config() returns DefaultLifecycleConfig (DLQMaxCount=1000).
	return b.lifecycle.Config().DLQMaxCount
}

// MoveToDLQ moves a task from the stream to the dead-letter queue.
func (b *redisBroker) MoveToDLQ(ctx context.Context, t *task.Task, streamKey, msgID, group string, dlqQueueName string) error {
	serialized, err := b.codec.Marshal(t)
	if err != nil {
		return err
	}
	qk := keys.KeysFor(dlqQueueName)
	dlqKey := qk.DLQ()
	dlqIndexKey := qk.DLQIndex()
	nowMs := time.Now().UnixMilli()

	var uniqueLockKey string
	var uniqueLockVal string
	if t.UniqueKey != "" {
		uniqueLockKey = keys.KeysFor(t.Queue).Unique(t.UniqueKey)
		uniqueLockVal = t.ID
	}

	maxCount := b.dlqMaxCount()
	res, err := handleFailureCmd.Run(ctx, b.rdb, []string{dlqKey, streamKey, uniqueLockKey, dlqIndexKey},
		"dlq", msgID, group, nowMs, serialized, uniqueLockVal, t.ID, maxCount).Result()
	if err != nil {
		return err
	}
	if b.lifecycle != nil {
		if n, ok := res.(int64); ok && n > 0 {
			b.lifecycle.Metrics().DLQEvictedTotal.Add(n)
		}
	}
	return nil
}

func (b *redisBroker) delayedMaxCount() int64 {
	// nil-safe Config() returns DefaultLifecycleConfig (DelayedMaxCount=0 = off).
	return b.lifecycle.Config().DelayedMaxCount
}

// ScheduleRetry moves a task to the delayed set for retry.
// When DelayedMaxCount is set, overflow uses drop_farthest (system path — never reject after XACK).
func (b *redisBroker) ScheduleRetry(ctx context.Context, t *task.Task, streamKey, msgID, group string, runAt time.Time) error {
	serialized, err := b.codec.Marshal(t)
	if err != nil {
		return err
	}
	qk := keys.KeysFor(t.Queue)
	delayedKey := qk.Delayed()
	delayedMax := b.delayedMaxCount()
	const systemDropFarthest = 1
	_, err = handleFailureCmd.Run(ctx, b.rdb, []string{delayedKey, streamKey, "", ""},
		"retry", msgID, group, runAt.UnixMilli(), serialized, "", "", 0, delayedMax, systemDropFarthest).Result()
	if err == nil {
		// Notify the delayed scheduler so it wakes up instead of waiting up to maxSleep (10s).
		_ = b.rdb.Publish(ctx, qk.DelayedWakeupChannel(), fmt.Sprintf("%d", runAt.UnixMilli())).Err()
	}
	return err
}

// DeferRateLimitedTask requeues a rate-limited task into the delayed set.
// When DelayedMaxCount is set, overflow uses drop_farthest (system path — never reject after XACK).
func (b *redisBroker) DeferRateLimitedTask(ctx context.Context, msgID string, t *task.Task, group string, runAt time.Time) error {
	serialized, err := b.codec.Marshal(t)
	if err != nil {
		return err
	}
	qk := keys.KeysFor(t.Queue)
	delayedKey := qk.Delayed()
	streamKey := qk.Stream()
	delayedMax := b.delayedMaxCount()
	const systemDropFarthest = 1
	_, err = deferRateLimitedTaskCmd.Run(ctx, b.rdb, []string{delayedKey, streamKey},
		group, msgID, runAt.UnixMilli(), serialized, delayedMax, systemDropFarthest).Result()
	if err == nil {
		// Notify the delayed scheduler so it wakes up instead of waiting up to maxSleep (10s).
		_ = b.rdb.Publish(ctx, qk.DelayedWakeupChannel(), fmt.Sprintf("%d", runAt.UnixMilli())).Err()
	}
	return err
}

// ReleaseUniqueLock deletes the unique lock if owned by the task.
func (b *redisBroker) ReleaseUniqueLock(ctx context.Context, t *task.Task) error {
	if t.UniqueKey == "" {
		return nil
	}
	uniqueKey := keys.KeysFor(t.Queue).Unique(t.UniqueKey)
	return unlockCmd.Run(ctx, b.rdb, []string{uniqueKey}, t.ID).Err()
}

// RenewUniqueLock extends the unique lock TTL if owned by the task.
func (b *redisBroker) RenewUniqueLock(ctx context.Context, t *task.Task, ttl time.Duration) error {
	if t.UniqueKey == "" {
		return nil
	}
	uniqueKey := keys.KeysFor(t.Queue).Unique(t.UniqueKey)
	_, err := renewUniqueLockCmd.Run(ctx, b.rdb, []string{uniqueKey}, t.ID, int(ttl.Milliseconds())).Result()
	return err
}

// CompleteTask ACKs/deletes the stream message and releases the unique lock when owned.
func (b *redisBroker) CompleteTask(ctx context.Context, t *task.Task, streamKey, msgID, group string) error {
	var uniqueLockKey string
	var uniqueLockVal string
	if t.UniqueKey != "" {
		uniqueLockKey = keys.KeysFor(t.Queue).Unique(t.UniqueKey)
		uniqueLockVal = t.ID
	}
	_, err := completeTaskCmd.Run(ctx, b.rdb, []string{streamKey, uniqueLockKey}, msgID, group, uniqueLockVal).Result()
	return err
}
