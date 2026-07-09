local configsKey = KEYS[1]
local delayedKey = KEYS[2]
local jobName = ARGV[1]
local spec = ARGV[2]
local serializedTask = ARGV[3]
local firstRunScore = tonumber(ARGV[4])
local maxCount = tonumber(ARGV[5]) or 0
local overflow = tonumber(ARGV[6]) or 0

local existing = redis.call('HGET', configsKey, jobName)
if existing == serializedTask then
	return 0
end

-- Capacity only blocks brand-new job names. Replacing an existing job always proceeds
-- (after ZREM of the prior delayed member) so we never leave config without a schedule.
if not existing and maxCount > 0 then
	local n = redis.call('ZCARD', delayedKey)
	if n >= maxCount then
		if overflow == 1 then
			local toDrop = n - maxCount + 1
			if toDrop > 0 then
				redis.call('ZREMRANGEBYRANK', delayedKey, -toDrop, -1)
			end
		else
			return -3
		end
	end
end

if existing then
	redis.call('ZREM', delayedKey, existing)
end

redis.call('HSET', configsKey, jobName, serializedTask)
redis.call('ZADD', delayedKey, firstRunScore, serializedTask)
return 1
