local dlqKey = KEYS[1]
local dlqIndexKey = KEYS[2]
local taskID = ARGV[1]

local exists = redis.call("HEXISTS", dlqIndexKey, taskID)
if exists == 1 then
	redis.call("ZREM", dlqKey, taskID)
	redis.call("HDEL", dlqIndexKey, taskID)
	return 1
end
return 0
