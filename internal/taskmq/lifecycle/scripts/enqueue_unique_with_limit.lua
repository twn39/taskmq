-- Unique enqueue with hard limit / MAXLEN.
-- KEYS[1]=lock KEYS[2]=stream
-- ARGV[1]=lockVal ARGV[2]=ttlMs ARGV[3]=serialized ARGV[4]=hard ARGV[5]=maxlen
-- Returns: 1=ok, -1=duplicate, -2=queue full.

local lockKey = KEYS[1]
local streamKey = KEYS[2]
local lockVal = ARGV[1]
local ttlMs = tonumber(ARGV[2])
local serialized = ARGV[3]
local hard = tonumber(ARGV[4]) or 0
local maxlen = tonumber(ARGV[5]) or 0

local currentLockVal = redis.call("GET", lockKey)
if currentLockVal and currentLockVal ~= lockVal then
	return -1
end
if hard > 0 then
  local n = redis.call('XLEN', streamKey)
  if n >= hard then
    return -2
  end
end
redis.call("SET", lockKey, lockVal, "PX", ttlMs)
if maxlen > 0 then
  redis.call("XADD", streamKey, "MAXLEN", "~", maxlen, "*", "task", serialized)
else
  redis.call("XADD", streamKey, "*", "task", serialized)
end
return 1
