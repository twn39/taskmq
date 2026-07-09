package worker

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

// MessageProcessor runs the per-message pipeline: decode, gate, middleware, complete.
type MessageProcessor struct {
	rdb              *redis.Client
	logger           *zap.Logger
	group            string
	codec            codec.Codec
	broker           broker.TaskBroker
	deadLetterPolicy policy.DeadLetterPolicy
	cancel           *Cancelations
	handlers         map[string]HandlerFunc
	middlewareChain  []CoreHandlerFunc
}

// Process handles a single Redis stream message end-to-end.
func (p *MessageProcessor) Process(ctx context.Context, streamKey string, msg redis.XMessage) {
	payload, ok := msg.Values["task"].([]byte)
	if !ok {
		if payloadStr, ok := msg.Values["task"].(string); ok {
			payload = codec.UnsafeStringToBytes(payloadStr)
		} else {
			// Fallback to legacy "payload" key
			payload, ok = msg.Values["payload"].([]byte)
			if !ok {
				if payloadStr, ok := msg.Values["payload"].(string); ok {
					payload = codec.UnsafeStringToBytes(payloadStr)
				} else {
					p.logger.Error("Message payload or task must be bytes or string")
					return
				}
			}
		}
	}

	var task taskmodel.Task
	err := p.codec.Unmarshal(payload, &task)
	if err != nil {
		p.logger.Error("Failed to deserialize task, discarding corrupted message", zap.Error(err))
		_ = p.rdb.XAck(ctx, streamKey, p.group, msg.ID).Err()
		_ = p.rdb.XDel(ctx, streamKey, msg.ID).Err()
		return
	}

	// Override task.Retry if delivery count is passed (representing crashes / reclaims)
	if devCountVal, ok := msg.Values["__delivery_count"]; ok {
		var devCount int64
		switch v := devCountVal.(type) {
		case int64:
			devCount = v
		case int:
			devCount = int64(v)
		case float64:
			devCount = int64(v)
		}
		if devCount > 0 {
			task.Retry = int(devCount) - 1
		}
	}

	// Check if the task has exceeded MaxRetry due to crash recovery
	if task.Retry > task.MaxRetry {
		p.logger.Warn("Task has exceeded MaxRetry due to crash recovery, routing directly to DLQ",
			zap.String("task_id", task.ID),
			zap.Int("retry", task.Retry),
			zap.Int("max_retry", task.MaxRetry),
		)
		dlqName := p.deadLetterPolicy.DLQQueueName(&task)
		p.deadLetterPolicy.BeforeDeadLetter(ctx, &task, fmt.Errorf("task exceeded max retry limits (%d/%d) due to worker crashes", task.Retry, task.MaxRetry))
		errMove := p.broker.MoveToDLQ(ctx, &task, streamKey, msg.ID, p.group, dlqName)
		if errMove != nil {
			p.logger.Error("Failed to move task to DLQ in crash recovery check", zap.Error(errMove))
		}
		return
	}

	// Check if task is already cancelled before execution (pre-execution check for backlog tasks)
	cancelledKey := keys.KeysFor(task.Queue).Cancelled(task.ID)
	isCancelled, err := p.rdb.Exists(ctx, cancelledKey).Result()
	if err == nil && isCancelled > 0 {
		p.logger.Warn("Task was cancelled before execution, discarding atomically", zap.String("task_id", task.ID))
		errComplete := p.broker.CompleteTask(ctx, &task, streamKey, msg.ID, p.group)
		if errComplete != nil {
			p.logger.Error("Failed to complete task in cancel check", zap.Error(errComplete))
		}
		return
	}

	// Create cancellable context and register it in cancel registry
	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if p.cancel != nil {
		p.cancel.Add(task.ID, cancel)
		defer p.cancel.Delete(task.ID)
	}

	c := AcquireConsumeContext(cancellableCtx, &task, msg.ID, task.Queue, p.group, p.middlewareChain)
	defer ReleaseConsumeContext(c)

	execErr := c.Next()

	// If no error occurred during processing chain and it completed fully (not aborted)
	if execErr == nil && !c.IsAborted() {
		err = p.broker.CompleteTask(ctx, &task, streamKey, msg.ID, p.group)
		if err != nil {
			p.logger.Error("Failed to complete task",
				zap.String("task_id", task.ID),
				zap.String("stream_id", msg.ID),
				zap.Error(err),
			)
			return
		}
	}
}
