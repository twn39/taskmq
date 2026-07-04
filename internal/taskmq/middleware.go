package taskmq

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

// MiddlewareHandlerFunc represents a signature that consumes ConsumeContext.
type MiddlewareHandlerFunc func(c *ConsumeContext) error

// RecoveryMiddleware catches any panic from the handler and returns it as an error.
func RecoveryMiddleware(logger *zap.Logger) CoreHandlerFunc {
	return func(c *ConsumeContext) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("task panicked: %v", r)
				logger.Error("Task panic caught in recovery middleware",
					zap.String("task_id", c.Task.ID),
					zap.String("task_name", c.Task.Name),
					zap.Any("panic", r),
				)
			}
		}()
		return c.Next()
	}
}

// RateLimitMiddleware applies dynamic GCRA rate limiting before task execution.
func RateLimitMiddleware(limiter *GCRALimiter, broker TaskBroker, provider func(queue string) (int64, time.Duration, string), logger *zap.Logger) CoreHandlerFunc {
	return func(c *ConsumeContext) error {
		max, duration, keyField := provider(c.Queue)
		if max <= 0 || duration <= 0 {
			return c.Next()
		}

		var groupKeyVal string
		if keyField != "" {
			groupKeyVal = extractGroupKey(c.Task.Payload, keyField)
		}
		limitKey := RateLimitKey(c.Queue, groupKeyVal)

		// Use TryConsume to atomically consume a token and get wait time if limited
		wait, err := limiter.TryConsume(c.Context, limitKey, max, duration)
		if err != nil {
			logger.Error("Failed to apply rate limiting", zap.Error(err))
			return c.Next()
		}

		if wait > 0 {
			logger.Debug("Rate limit exceeded, deferring task",
				zap.String("queue", c.Queue),
				zap.String("group_key", groupKeyVal),
				zap.Duration("wait", wait),
			)
			runAt := time.Now().Add(wait)
			if err := broker.DeferRateLimitedTask(c.Context, c.MessageID, c.Task, c.Group, runAt); err != nil {
				logger.Error("Failed to defer rate limited task", zap.Error(err))
				return err
			}
			c.Abort()
			return nil
		}

		return c.Next()
	}
}

// RetryAndDLQMiddleware manages task error retries and routes to dead letter queues.
func RetryAndDLQMiddleware(broker TaskBroker, retryPolicy RetryPolicy, dlqPolicy DeadLetterPolicy, logger *zap.Logger) CoreHandlerFunc {
	return func(c *ConsumeContext) error {
		err := c.Next()
		if err == nil {
			return nil
		}

		c.Task.Retry++
		streamKey := StreamKey(c.Queue)

		if !retryPolicy.ShouldRetry(c.Task, err) {
			dlqPolicy.BeforeDeadLetter(c.Context, c.Task, err)
			dlqName := dlqPolicy.DLQQueueName(c.Task)

			logger.Error("Task permanently failed, moving to DLQ",
				zap.String("task_id", c.Task.ID),
				zap.String("dlq", dlqName),
				zap.Error(err),
			)

			if dlqErr := broker.MoveToDLQ(c.Context, c.Task, streamKey, c.MessageID, c.Group, dlqName); dlqErr != nil {
				logger.Error("Failed to move task to DLQ atomically", zap.Error(dlqErr))
			}
			return err
		}

		backoff := retryPolicy.NextBackoff(c.Task)
		logger.Info("Scheduling task retry with backoff",
			zap.String("task_id", c.Task.ID),
			zap.String("task_name", c.Task.Name),
			zap.Int("retry", c.Task.Retry),
			zap.Duration("backoff", backoff),
		)

		runAt := time.Now().Add(backoff)
		if rErr := broker.ScheduleRetry(c.Context, c.Task, streamKey, c.MessageID, c.Group, runAt); rErr != nil {
			logger.Error("Failed to schedule task retry atomically", zap.Error(rErr))
		}
		return err
	}
}
