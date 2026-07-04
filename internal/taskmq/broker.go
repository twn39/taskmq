package taskmq

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// TaskBroker abstracts all atomic queue storage transitions.
type TaskBroker interface {
	MoveToDLQ(ctx context.Context, task *Task, streamKey, msgID, group string, dlqQueueName string) error
	ScheduleRetry(ctx context.Context, task *Task, streamKey, msgID, group string, runAt time.Time) error
	DeferRateLimitedTask(ctx context.Context, msgID string, task *Task, group string, runAt time.Time) error
	ReleaseUniqueLock(ctx context.Context, task *Task) error
	CompleteTask(ctx context.Context, task *Task, streamKey, msgID, group string) error
}

type redisBroker struct {
	rdb   *redis.Client
	codec Codec
}

func NewRedisBroker(rdb *redis.Client, codec Codec) TaskBroker {
	return &redisBroker{
		rdb:   rdb,
		codec: codec,
	}
}

const luaHandleFailure = `
	local targetKey = KEYS[1]
	local streamKey = KEYS[2]
	local lockKey = KEYS[3]
	local action = ARGV[1]
	local msgId = ARGV[2]
	local groupName = ARGV[3]
	local score = tonumber(ARGV[4])
	local serializedTask = ARGV[5]
	local expectedLockVal = ARGV[6]

	-- Ack and delete the processed message from stream
	redis.call("XACK", streamKey, groupName, msgId)
	redis.call("XDEL", streamKey, msgId)

	-- Add to DLQ or Retry Delayed ZSet
	redis.call("ZADD", targetKey, score, serializedTask)

	-- Clean up uniqueness lock if moving to DLQ and lock matches
	if action == "dlq" then
		-- Apply capacity protection: keep only latest 1000 DLQ items
		redis.call("ZREMRANGEBYRANK", targetKey, 0, -1001)

		if lockKey ~= "" and expectedLockVal ~= "" then
			if redis.call("GET", lockKey) == expectedLockVal then
				redis.call("DEL", lockKey)
			end
		end
	end
	return 1
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

var (
	handleFailureCmd        = redis.NewScript(luaHandleFailure)
	deferRateLimitedTaskCmd = redis.NewScript(luaDeferRateLimitedTask)
	unlockCmd               = redis.NewScript(luaUnlock)
	completeTaskCmd         = redis.NewScript(luaCompleteTask)
)

func (b *redisBroker) MoveToDLQ(ctx context.Context, task *Task, streamKey, msgID, group string, dlqQueueName string) error {
	serialized, err := b.codec.Marshal(task)
	if err != nil {
		return err
	}
	dlqKey := DLQKey(dlqQueueName)
	nowMs := time.Now().UnixMilli()

	var uniqueLockKey string
	var uniqueLockVal string
	if task.UniqueKey != "" {
		uniqueLockKey = UniqueKey(task.Queue, task.UniqueKey)
		uniqueLockVal = task.ID
	}

	_, err = handleFailureCmd.Run(ctx, b.rdb, []string{dlqKey, streamKey, uniqueLockKey}, "dlq", msgID, group, nowMs, serialized, uniqueLockVal).Result()
	return err
}

func (b *redisBroker) ScheduleRetry(ctx context.Context, task *Task, streamKey, msgID, group string, runAt time.Time) error {
	serialized, err := b.codec.Marshal(task)
	if err != nil {
		return err
	}
	delayedKey := DelayedKey(task.Queue)
	_, err = handleFailureCmd.Run(ctx, b.rdb, []string{delayedKey, streamKey, ""}, "retry", msgID, group, runAt.UnixMilli(), serialized, "").Result()
	return err
}

func (b *redisBroker) DeferRateLimitedTask(ctx context.Context, msgID string, task *Task, group string, runAt time.Time) error {
	serialized, err := b.codec.Marshal(task)
	if err != nil {
		return err
	}
	delayedKey := DelayedKey(task.Queue)
	streamKey := StreamKey(task.Queue)
	_, err = deferRateLimitedTaskCmd.Run(ctx, b.rdb, []string{delayedKey, streamKey}, group, msgID, runAt.UnixMilli(), serialized).Result()
	return err
}

func (b *redisBroker) ReleaseUniqueLock(ctx context.Context, task *Task) error {
	if task.UniqueKey == "" {
		return nil
	}
	uniqueKey := UniqueKey(task.Queue, task.UniqueKey)
	return unlockCmd.Run(ctx, b.rdb, []string{uniqueKey}, task.ID).Err()
}

func (b *redisBroker) CompleteTask(ctx context.Context, task *Task, streamKey, msgID, group string) error {
	var uniqueLockKey string
	var uniqueLockVal string
	if task.UniqueKey != "" {
		uniqueLockKey = UniqueKey(task.Queue, task.UniqueKey)
		uniqueLockVal = task.ID
	}
	_, err := completeTaskCmd.Run(ctx, b.rdb, []string{streamKey, uniqueLockKey}, msgID, group, uniqueLockVal).Result()
	return err
}
