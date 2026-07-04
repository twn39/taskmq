local lockKey = KEYS[1]
local delayedKey = KEYS[2]
local lockVal = ARGV[1]
local ttlMs = tonumber(ARGV[2])
local serialized = ARGV[3]
local score = tonumber(ARGV[4])

local currentLockVal = redis.call("GET", lockKey)
if currentLockVal and currentLockVal ~= lockVal then
	return -1
end
redis.call("SET", lockKey, lockVal, "PX", ttlMs)
redis.call("ZADD", delayedKey, score, serialized)
return 1
