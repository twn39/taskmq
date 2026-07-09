-- Atomically moves one delayed member onto the stream with hard/MAXLEN checks.
-- KEYS[1]=delayed KEYS[2]=stream
-- ARGV[1]=member ARGV[2]=hardLimit (0=off) ARGV[3]=streamMaxLen (0=off)
-- Returns: 1=ok, 0=member missing, -2=queue full (member left in delayed).

local delayed = KEYS[1]
local stream = KEYS[2]
local member = ARGV[1]
local hard = tonumber(ARGV[2]) or 0
local maxlen = tonumber(ARGV[3]) or 0
if hard > 0 then
  local n = redis.call('XLEN', stream)
  if n >= hard then
    return -2
  end
end
local removed = redis.call('ZREM', delayed, member)
if removed == 0 then
  return 0
end
if maxlen > 0 then
  redis.call('XADD', stream, 'MAXLEN', '~', maxlen, '*', 'task', member)
else
  redis.call('XADD', stream, '*', 'task', member)
end
return 1
