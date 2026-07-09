-- Atomically enforces hard limit then XADD (optional MAXLEN).
-- KEYS[1]=stream
-- ARGV[1]=hardLimit (0=off), ARGV[2]=streamMaxLen (0=off), ARGV[3]=serialized
-- Returns: stream ID string on success, -2 if queue full.

local stream = KEYS[1]
local hard = tonumber(ARGV[1]) or 0
local maxlen = tonumber(ARGV[2]) or 0
local payload = ARGV[3]
if hard > 0 then
  local n = redis.call('XLEN', stream)
  if n >= hard then
    return -2
  end
end
if maxlen > 0 then
  return redis.call('XADD', stream, 'MAXLEN', '~', maxlen, '*', 'task', payload)
end
return redis.call('XADD', stream, '*', 'task', payload)
