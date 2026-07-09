package taskmq

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TaskBroker abstracts all atomic queue storage transitions.
type TaskBroker interface {
	MoveToDLQ(ctx context.Context, task *Task, streamKey, msgID, group string, dlqQueueName string) error
	ScheduleRetry(ctx context.Context, task *Task, streamKey, msgID, group string, runAt time.Time) error
	DeferRateLimitedTask(ctx context.Context, msgID string, task *Task, group string, runAt time.Time) error
	ReleaseUniqueLock(ctx context.Context, task *Task) error
	RenewUniqueLock(ctx context.Context, task *Task, ttl time.Duration) error
	CompleteTask(ctx context.Context, task *Task, streamKey, msgID, group string) error
}

type redisBroker struct {
	rdb       *redis.Client
	codec     Codec
	lifecycle *Lifecycle
}

// NewRedisBroker creates a TaskBroker. Optional lifecycle controls DLQ capacity.
func NewRedisBroker(rdb *redis.Client, codec Codec, lifecycle ...*Lifecycle) TaskBroker {
	var lc *Lifecycle
	if len(lifecycle) > 0 {
		lc = lifecycle[0]
	}
	return &redisBroker{
		rdb:       rdb,
		codec:     codec,
		lifecycle: lc,
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

	redis.call("XACK", streamKey, groupName, msgId)
	redis.call("XDEL", streamKey, msgId)
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
	if b.lifecycle == nil {
		return 1000 // preserve historical default
	}
	return b.lifecycle.Config().DLQMaxCount
}

func (b *redisBroker) MoveToDLQ(ctx context.Context, task *Task, streamKey, msgID, group string, dlqQueueName string) error {
	serialized, err := b.codec.Marshal(task)
	if err != nil {
		return err
	}
	keys := KeysFor(dlqQueueName)
	dlqKey := keys.DLQ()
	dlqIndexKey := keys.DLQIndex()
	nowMs := time.Now().UnixMilli()

	var uniqueLockKey string
	var uniqueLockVal string
	if task.UniqueKey != "" {
		uniqueLockKey = KeysFor(task.Queue).Unique(task.UniqueKey)
		uniqueLockVal = task.ID
	}

	maxCount := b.dlqMaxCount()
	res, err := handleFailureCmd.Run(ctx, b.rdb, []string{dlqKey, streamKey, uniqueLockKey, dlqIndexKey},
		"dlq", msgID, group, nowMs, serialized, uniqueLockVal, task.ID, maxCount).Result()
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

func (b *redisBroker) ScheduleRetry(ctx context.Context, task *Task, streamKey, msgID, group string, runAt time.Time) error {
	serialized, err := b.codec.Marshal(task)
	if err != nil {
		return err
	}
	keys := KeysFor(task.Queue)
	delayedKey := keys.Delayed()
	_, err = handleFailureCmd.Run(ctx, b.rdb, []string{delayedKey, streamKey, "", ""},
		"retry", msgID, group, runAt.UnixMilli(), serialized, "", "", 0).Result()
	if err == nil {
		// Notify the delayed scheduler so it wakes up instead of waiting up to maxSleep (10s).
		_ = b.rdb.Publish(ctx, keys.DelayedWakeupChannel(), fmt.Sprintf("%d", runAt.UnixMilli())).Err()
	}
	return err
}

func (b *redisBroker) DeferRateLimitedTask(ctx context.Context, msgID string, task *Task, group string, runAt time.Time) error {
	serialized, err := b.codec.Marshal(task)
	if err != nil {
		return err
	}
	keys := KeysFor(task.Queue)
	delayedKey := keys.Delayed()
	streamKey := keys.Stream()
	_, err = deferRateLimitedTaskCmd.Run(ctx, b.rdb, []string{delayedKey, streamKey}, group, msgID, runAt.UnixMilli(), serialized).Result()
	if err == nil {
		// Notify the delayed scheduler so it wakes up instead of waiting up to maxSleep (10s).
		_ = b.rdb.Publish(ctx, keys.DelayedWakeupChannel(), fmt.Sprintf("%d", runAt.UnixMilli())).Err()
	}
	return err
}

func (b *redisBroker) ReleaseUniqueLock(ctx context.Context, task *Task) error {
	if task.UniqueKey == "" {
		return nil
	}
	uniqueKey := KeysFor(task.Queue).Unique(task.UniqueKey)
	return unlockCmd.Run(ctx, b.rdb, []string{uniqueKey}, task.ID).Err()
}

func (b *redisBroker) RenewUniqueLock(ctx context.Context, task *Task, ttl time.Duration) error {
	if task.UniqueKey == "" {
		return nil
	}
	uniqueKey := KeysFor(task.Queue).Unique(task.UniqueKey)
	_, err := renewUniqueLockCmd.Run(ctx, b.rdb, []string{uniqueKey}, task.ID, int(ttl.Milliseconds())).Result()
	return err
}

func (b *redisBroker) CompleteTask(ctx context.Context, task *Task, streamKey, msgID, group string) error {
	var uniqueLockKey string
	var uniqueLockVal string
	if task.UniqueKey != "" {
		uniqueLockKey = KeysFor(task.Queue).Unique(task.UniqueKey)
		uniqueLockVal = task.ID
	}
	_, err := completeTaskCmd.Run(ctx, b.rdb, []string{streamKey, uniqueLockKey}, msgID, group, uniqueLockVal).Result()
	return err
}
