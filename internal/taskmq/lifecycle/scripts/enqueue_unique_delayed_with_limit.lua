-- Unique + delayed capacity.
-- KEYS[1]=lock KEYS[2]=delayed
-- ARGV[1]=lockVal ARGV[2]=ttlMs ARGV[3]=serialized ARGV[4]=score ARGV[5]=maxCount ARGV[6]=overflow
-- Returns: 1=ok, -1=duplicate, -3=delayed full (reject policy).

local lockKey = KEYS[1]
local delayedKey = KEYS[2]
local lockVal = ARGV[1]
local ttlMs = tonumber(ARGV[2])
local serialized = ARGV[3]
local score = tonumber(ARGV[4])
local maxCount = tonumber(ARGV[5]) or 0
local overflow = tonumber(ARGV[6]) or 0

local currentLockVal = redis.call("GET", lockKey)
if currentLockVal and currentLockVal ~= lockVal then
	return -1
end
if maxCount > 0 then
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
redis.call("SET", lockKey, lockVal, "PX", ttlMs)
redis.call("ZADD", delayedKey, score, serialized)
return 1
