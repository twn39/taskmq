package taskmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
func RateLimitMiddleware(limiter *GCRALimiter, broker TaskBroker, provider func(queue string) (int64, time.Duration, string), extractor func([]byte) string, codec Codec, logger *zap.Logger) CoreHandlerFunc {
	return func(c *ConsumeContext) error {
		max, duration, keyField := provider(c.Queue)
		if max <= 0 || duration <= 0 {
			return c.Next()
		}

		var groupKeyVal string
		if c.Task.GroupKey != "" {
			groupKeyVal = c.Task.GroupKey
		} else if extractor != nil {
			groupKeyVal = extractor(c.Task.Payload)
		} else if keyField != "" {
			if inspector, ok := codec.(PayloadInspector); ok {
				if val, err := inspector.InspectField(c.Task.Payload, keyField); err == nil {
					groupKeyVal = val
				}
			} else {
				if val, err := inspectJSONField(c.Task.Payload, keyField); err == nil {
					groupKeyVal = val
				}
			}
		}
		c.RateLimitGroup = groupKeyVal
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

		if errors.Is(err, ErrNoHandler) || !retryPolicy.ShouldRetry(c.Task, err) {
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

// UniqueLockWatchdogMiddleware handles background renewal of unique locks during execution
// and implements the specific UniqueScope lifecycle behaviors.
func UniqueLockWatchdogMiddleware(broker TaskBroker, logger *zap.Logger) CoreHandlerFunc {
	return func(c *ConsumeContext) error {
		task := c.Task
		if task.UniqueKey == "" {
			return c.Next()
		}

		// UniqueUntilStart: Release the lock immediately as execution starts
		if task.UniqueScope == UniqueUntilStart {
			if err := broker.ReleaseUniqueLock(c.Context, task); err != nil {
				logger.Error("Failed to release unique lock for UniqueUntilStart",
					zap.String("task_id", task.ID),
					zap.Error(err),
				)
			}
			return c.Next()
		}

		// Initialize Watchdog ticker for automatic lock renewal
		ttl := time.Duration(task.UniqueTTLMs) * time.Millisecond
		if ttl <= 0 {
			ttl = 1 * time.Hour // Default to 1 hour
		}

		renewInterval := ttl / 3
		if renewInterval < 100*time.Millisecond {
			renewInterval = 100 * time.Millisecond // Safety floor
		}

		watchdogCtx, cancelWatchdog := context.WithCancel(c.Context)
		defer cancelWatchdog()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(renewInterval)
			defer ticker.Stop()

			for {
				select {
				case <-watchdogCtx.Done():
					return
				case <-ticker.C:
					var success bool
					for attempt := 1; attempt <= 3; attempt++ {
						err := broker.RenewUniqueLock(watchdogCtx, task, ttl)
						if err == nil {
							success = true
							logger.Debug("Watchdog successfully renewed unique lock",
								zap.String("task_id", task.ID),
								zap.String("key", task.UniqueKey),
							)
							break
						}

						// Retries with short pause
						logger.Warn("Watchdog failed to renew lock, retrying...",
							zap.String("task_id", task.ID),
							zap.Int("attempt", attempt),
							zap.Error(err),
						)
						select {
						case <-watchdogCtx.Done():
							return
						case <-time.After(50 * time.Millisecond):
						}
					}

					if !success {
						logger.Error("Watchdog failed to renew unique lock after all retries",
							zap.String("task_id", task.ID),
						)
					}
				}
			}
		}()

		// Execute next handler
		err := c.Next()

		// Stop watchdog and wait for its completion
		cancelWatchdog()
		wg.Wait()

		// UniqueUntilSuccess: If error occurs, release unique lock during retry delay
		if err != nil && task.UniqueScope == UniqueUntilSuccess {
			if rErr := broker.ReleaseUniqueLock(context.Background(), task); rErr != nil {
				logger.Error("Failed to release unique lock on failure for UniqueUntilSuccess",
					zap.String("task_id", task.ID),
					zap.Error(rErr),
				)
			}
		}

		return err
	}
}
