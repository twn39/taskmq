-- Enforces delayed max count then ZADD.
-- KEYS[1]=delayed
-- ARGV[1]=score ARGV[2]=serialized ARGV[3]=maxCount ARGV[4]=overflow (0=reject,1=drop_farthest)
-- Returns: 1=ok, -3=delayed full (reject policy).

local delayedKey = KEYS[1]
local score = tonumber(ARGV[1])
local serialized = ARGV[2]
local maxCount = tonumber(ARGV[3]) or 0
local overflow = tonumber(ARGV[4]) or 0

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
redis.call('ZADD', delayedKey, score, serialized)
return 1
