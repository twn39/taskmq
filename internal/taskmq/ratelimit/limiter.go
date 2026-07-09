package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// GCRALimiter implements Generic Cell Rate Algorithm rate limiting via Redis.
type GCRALimiter struct {
	rdb *redis.Client
}

// NewGCRALimiter creates a GCRA limiter backed by Redis.
func NewGCRALimiter(rdb *redis.Client) *GCRALimiter {
	return &GCRALimiter{rdb: rdb}
}

// Lua scripts
const gcraCheckScript = `
local rate_limit_key = KEYS[1]
local burst = tonumber(ARGV[1])
local emission_interval = tonumber(ARGV[2]) -- in seconds

local now = redis.call("TIME")
local now_sec = tonumber(now[1]) + (tonumber(now[2]) / 1000000)

local tat = tonumber(redis.call("GET", rate_limit_key) or now_sec)
tat = math.max(tat, now_sec)

local burst_offset = emission_interval * burst
local allow_at = tat - burst_offset

if now_sec < allow_at then
    -- Return remaining wait time in milliseconds
    return math.ceil((allow_at - now_sec) * 1000)
else
    return 0
end
`

const gcraTryConsumeScript = `
local rate_limit_key = KEYS[1]
local burst = tonumber(ARGV[1])
local emission_interval = tonumber(ARGV[2]) -- in seconds

local now = redis.call("TIME")
local now_sec = tonumber(now[1]) + (tonumber(now[2]) / 1000000)

local tat = tonumber(redis.call("GET", rate_limit_key) or now_sec)
tat = math.max(tat, now_sec)

local burst_offset = emission_interval * burst
local allow_at = tat - burst_offset

if now_sec < allow_at then
    return math.ceil((allow_at - now_sec) * 1000)
end

local new_tat = tat + emission_interval
local key_expiration = math.ceil(new_tat - now_sec)
redis.call("SET", rate_limit_key, tostring(new_tat), "EX", key_expiration)
return 0
`

// Check checks if the key is rate limited. Returns wait time duration (0 if allowed)
func (l *GCRALimiter) Check(ctx context.Context, key string, max int64, duration time.Duration) (time.Duration, error) {
	if max <= 0 || duration <= 0 {
		return 0, nil
	}

	emissionInterval := float64(duration.Seconds()) / float64(max)
	burst := max - 1
	if burst < 0 {
		burst = 0
	}

	res, err := l.rdb.Eval(ctx, gcraCheckScript, []string{key}, burst, emissionInterval).Result()
	if err != nil {
		return 0, err
	}

	waitMs, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected script result type: %T", res)
	}

	if waitMs > 0 {
		return time.Duration(waitMs) * time.Millisecond, nil
	}

	return 0, nil
}

// TryConsume atomically checks and consumes a token for the key. Returns remaining wait time if rate limited.
func (l *GCRALimiter) TryConsume(ctx context.Context, key string, max int64, duration time.Duration) (time.Duration, error) {
	if max <= 0 || duration <= 0 {
		return 0, nil
	}

	emissionInterval := float64(duration.Seconds()) / float64(max)
	burst := max - 1
	if burst < 0 {
		burst = 0
	}

	res, err := l.rdb.Eval(ctx, gcraTryConsumeScript, []string{key}, burst, emissionInterval).Result()
	if err != nil {
		return 0, err
	}

	waitMs, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected script result type: %T", res)
	}

	if waitMs > 0 {
		return time.Duration(waitMs) * time.Millisecond, nil
	}

	return 0, nil
}
