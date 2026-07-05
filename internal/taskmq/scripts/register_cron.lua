local configsKey = KEYS[1]
local delayedKey = KEYS[2]
local jobName = ARGV[1]
local spec = ARGV[2]
local serializedTask = ARGV[3]
local firstRunScore = tonumber(ARGV[4])

local existing = redis.call('HGET', configsKey, jobName)
if existing == serializedTask then
	return 0
end

if existing then
	redis.call('ZREM', delayedKey, existing)
end

redis.call('HSET', configsKey, jobName, serializedTask)
redis.call('ZADD', delayedKey, firstRunScore, serializedTask)
return 1
