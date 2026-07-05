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
	RenewUniqueLock(ctx context.Context, task *Task, ttl time.Duration) error
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
	local dlqIndexKey = KEYS[4]
	local action = ARGV[1]
	local msgId = ARGV[2]
	local groupName = ARGV[3]
	local score = tonumber(ARGV[4])
	local serializedTask = ARGV[5]
	local expectedLockVal = ARGV[6]
	local taskId = ARGV[7]

	-- Ack and delete the processed message from stream
	redis.call("XACK", streamKey, groupName, msgId)
	redis.call("XDEL", streamKey, msgId)

	-- Add to DLQ or Retry Delayed ZSet
	redis.call("ZADD", targetKey, score, serializedTask)

	-- Clean up uniqueness lock if moving to DLQ and lock matches
	if action == "dlq" then
		-- Set the secondary index in DLQ Hash
		if dlqIndexKey ~= nil and dlqIndexKey ~= "" and taskId ~= nil and taskId ~= "" then
			redis.call("HSET", dlqIndexKey, taskId, serializedTask)
		end

		-- Apply capacity protection: keep only latest 1000 DLQ items.
		-- Before removing from Sorted Set, clean them up from the Hash index.
		if dlqIndexKey ~= nil and dlqIndexKey ~= "" then
			local toRemove = redis.call("ZRANGE", targetKey, 0, -1001)
			for _, member in ipairs(toRemove) do
				local id = nil
				if string.sub(member, 1, 1) == "{" then
					id = string.match(member, '"id"%s*:%s*"([^"]+)"')
				else
					if #member >= 2 then
						local b1 = string.byte(member, 1)
						local b2 = string.byte(member, 2)
						local len = b1 * 256 + b2
						if #member >= 2 + len then
							id = string.sub(member, 3, 2 + len)
						end
					end
				end
				if id then
					redis.call("HDEL", dlqIndexKey, id)
				end
			end
		end

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

	_, err = handleFailureCmd.Run(ctx, b.rdb, []string{dlqKey, streamKey, uniqueLockKey, dlqIndexKey}, "dlq", msgID, group, nowMs, serialized, uniqueLockVal, task.ID).Result()
	return err
}

func (b *redisBroker) ScheduleRetry(ctx context.Context, task *Task, streamKey, msgID, group string, runAt time.Time) error {
	serialized, err := b.codec.Marshal(task)
	if err != nil {
		return err
	}
	delayedKey := KeysFor(task.Queue).Delayed()
	_, err = handleFailureCmd.Run(ctx, b.rdb, []string{delayedKey, streamKey, "", ""}, "retry", msgID, group, runAt.UnixMilli(), serialized, "", "").Result()
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
