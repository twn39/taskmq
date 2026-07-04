local dlqKey = KEYS[1]
local dlqIndexKey = KEYS[2]
local taskID = ARGV[1]

local serialized = redis.call("HGET", dlqIndexKey, taskID)
if serialized then
	redis.call("ZREM", dlqKey, serialized)
	redis.call("HDEL", dlqIndexKey, taskID)
	return 1
end
return 0
