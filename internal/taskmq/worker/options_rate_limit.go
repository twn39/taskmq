package worker

import (
	"fmt"
	"time"
)

func WithRateLimit(max int64, duration time.Duration) poolOption {
	return poolOption(func(o *WorkerPoolOptions) error {
		if max <= 0 || duration <= 0 {
			return fmt.Errorf("invalid rate limit parameters: max=%d, duration=%v", max, duration)
		}
		o.rateLimitMax = max
		o.rateLimitDuration = duration
		return nil
	})
}

func WithRateLimitKeyField(field string) poolOption {
	return poolOption(func(o *WorkerPoolOptions) error {
		o.rateLimitKeyField = field
		return nil
	})
}
