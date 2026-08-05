package worker

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/broker"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/events"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/meta"
	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

// MessageProcessor runs the per-message pipeline: decode, gate, middleware, complete.
type MessageProcessor struct {
	rdb              redis.UniversalClient
	logger           *zap.Logger
	group            string
	codec            codec.Codec
	broker           broker.TaskBroker
	deadLetterPolicy policy.DeadLetterPolicy
	cancel           *Cancelations
	handlers         map[string]HandlerFunc
	middlewareChain  []CoreHandlerFunc
	meta             *meta.Store
	events           *events.Publisher
}

// Process handles a single Redis stream message end-to-end.
// Stages: decode → delivery count → max-retry DLQ gate → cancel gate → execute → settle.
func (p *MessageProcessor) Process(ctx context.Context, streamKey string, msg redis.XMessage) {
	payload, ok := extractMessagePayload(msg.Values)
	if !ok {
		p.logger.Error("Message payload or task must be bytes or string")
		return
	}

	task, err := p.decodeTask(payload)
	if err != nil {
		p.logger.Error("Failed to deserialize task, discarding corrupted message", zap.Error(err))
		_ = p.rdb.XAck(ctx, streamKey, p.group, msg.ID).Err()
		_ = p.rdb.XDel(ctx, streamKey, msg.ID).Err()
		return
	}

	applyDeliveryCount(&task, msg.Values)

	if p.routeExceededMaxRetry(ctx, streamKey, msg.ID, &task) {
		return
	}
	if p.discardIfCancelled(ctx, streamKey, msg.ID, &task) {
		return
	}

	if p.meta != nil {
		_ = p.meta.SetState(ctx, task.Queue, task.ID, meta.StateActive, "", msg.ID)
	}
	if p.events != nil {
		p.events.Emit(ctx, task.Queue, events.TypeActive, task.ID, task.Name, "")
	}
	p.executeAndSettle(ctx, streamKey, msg.ID, &task)
}

// extractMessagePayload reads the task body from stream message values.
// Prefers "task", falls back to legacy "payload". Accepts []byte or string.
func extractMessagePayload(values map[string]interface{}) (payload []byte, ok bool) {
	if payload, ok = valueAsBytes(values["task"]); ok {
		return payload, true
	}
	return valueAsBytes(values["payload"])
}

func valueAsBytes(v interface{}) ([]byte, bool) {
	switch t := v.(type) {
	case []byte:
		return t, true
	case string:
		return codec.UnsafeStringToBytes(t), true
	default:
		return nil, false
	}
}

func (p *MessageProcessor) decodeTask(payload []byte) (taskmodel.Task, error) {
	var task taskmodel.Task
	err := p.codec.Unmarshal(payload, &task)
	return task, err
}

// applyDeliveryCount maps PEL reclaim delivery count into task.Retry.
// When __delivery_count is present and > 0, Retry becomes delivery_count-1
// so crash recovery counts against MaxRetry.
func applyDeliveryCount(task *taskmodel.Task, values map[string]interface{}) {
	devCountVal, ok := values["__delivery_count"]
	if !ok {
		return
	}
	var devCount int64
	switch v := devCountVal.(type) {
	case int64:
		devCount = v
	case int:
		devCount = int64(v)
	case float64:
		devCount = int64(v)
	case string:
		// Redis Streams often surface field values as strings.
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return
		}
		devCount = n
	case []byte:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return
		}
		devCount = n
	default:
		return
	}
	if devCount > 0 {
		task.Retry = int(devCount) - 1
	}
}

// routeExceededMaxRetry moves crash-recovered tasks that exceeded MaxRetry to DLQ.
// Returns true if the message was fully handled (caller must not continue).
func (p *MessageProcessor) routeExceededMaxRetry(ctx context.Context, streamKey, msgID string, task *taskmodel.Task) bool {
	if task.Retry <= task.MaxRetry {
		return false
	}
	p.logger.Warn("Task has exceeded MaxRetry due to crash recovery, routing directly to DLQ",
		zap.String("task_id", task.ID),
		zap.Int("retry", task.Retry),
		zap.Int("max_retry", task.MaxRetry),
	)
	dlqName := p.deadLetterPolicy.DLQQueueName(task)
	p.deadLetterPolicy.BeforeDeadLetter(ctx, task, fmt.Errorf("task exceeded max retry limits (%d/%d) due to worker crashes", task.Retry, task.MaxRetry))
	if errMove := p.broker.MoveToDLQ(ctx, task, streamKey, msgID, p.group, dlqName); errMove != nil {
		p.logger.Error("Failed to move task to DLQ in crash recovery check", zap.Error(errMove))
	}
	return true
}

// discardIfCancelled completes and drops tasks cancelled before execution.
// Returns true if the message was fully handled.
func (p *MessageProcessor) discardIfCancelled(ctx context.Context, streamKey, msgID string, task *taskmodel.Task) bool {
	cancelledKey := keys.KeysFor(task.Queue).Cancelled(task.ID)
	isCancelled, err := p.rdb.Exists(ctx, cancelledKey).Result()
	if err != nil || isCancelled == 0 {
		return false
	}
	p.logger.Warn("Task was cancelled before execution, discarding atomically", zap.String("task_id", task.ID))
	if errComplete := p.broker.CompleteTask(ctx, task, streamKey, msgID, p.group); errComplete != nil {
		p.logger.Error("Failed to complete task in cancel check", zap.Error(errComplete))
	}
	if p.meta != nil {
		_ = p.meta.SetState(ctx, task.Queue, task.ID, meta.StateCancelled, "cancelled before execution", msgID)
	}
	if p.events != nil {
		p.events.Emit(ctx, task.Queue, events.TypeCancelled, task.ID, task.Name, "cancelled before execution")
	}
	return true
}

// executeAndSettle runs the middleware/handler chain and completes on success.
//
// Terminal outcomes (see docs/TERMINAL_OUTCOMES.md):
//   - success → CompleteTask here
//   - retry / DLQ → RetryAndDLQMiddleware returns Handled(err); no CompleteTask
//   - rate-limit defer → Abort + nil; no CompleteTask
//   - cancel / max-retry gates → handled before this method
func (p *MessageProcessor) executeAndSettle(ctx context.Context, streamKey, msgID string, task *taskmodel.Task) {
	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if p.cancel != nil {
		p.cancel.Add(task.ID, cancel)
		defer p.cancel.Delete(task.ID)
	}

	c := AcquireConsumeContext(cancellableCtx, task, msgID, task.Queue, p.group, p.middlewareChain)
	defer ReleaseConsumeContext(c)
	if p.meta != nil {
		metaStore := p.meta
		ev := p.events
		c.progress = func(ctx context.Context, percent int, data string) error {
			if err := metaStore.SetProgress(ctx, task.Queue, task.ID, percent, data); err != nil {
				return err
			}
			if ev != nil {
				_ = ev.Publish(ctx, events.Event{
					Type:     events.TypeProgress,
					Queue:    task.Queue,
					TaskID:   task.ID,
					Name:     task.Name,
					Progress: percent,
					Data:     data,
				})
			}
			return nil
		}
	}

	execErr := c.Next()
	// Handled / aborted / any error: message already settled or must stay in PEL.
	if execErr != nil || c.IsAborted() {
		if execErr != nil && !IsHandled(execErr) && !c.IsAborted() {
			p.logger.Error("Task pipeline finished without settling outcome",
				zap.String("task_id", task.ID),
				zap.String("stream_id", msgID),
				zap.Error(execErr),
			)
		}
		return
	}

	if len(c.Result) > 0 {
		task.Result = c.Result
	}
	if err := p.broker.CompleteTask(ctx, task, streamKey, msgID, p.group); err != nil {
		p.logger.Error("Failed to complete task",
			zap.String("task_id", task.ID),
			zap.String("stream_id", msgID),
			zap.Error(err),
		)
	}
}
