package taskmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type HandlerFunc func(ctx context.Context, task *Task) error

type Worker interface {
	Register(taskName string, handler HandlerFunc)
	Start(ctx context.Context) error
	Stop()
}

type workerPool struct {
	rdb           *redis.Client
	logger        *zap.Logger
	queue         string
	group         string
	consumer      string
	concurrency   int
	handlers      map[string]HandlerFunc
	scheduler     *delayedScheduler
	janitor       *pelRecoveryJanitor
	cronManager   *cronManager
	codec         Codec
	syncExecution bool
	execPoolSize  int
	execChan      chan taskExecutionRequest
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

type WorkerOptions struct {
	Group             string
	Consumer          string
	Concurrency       int
	Codec             Codec
	SyncExecution     bool
	ExecutionPoolSize int
}

type taskExecutionRequest struct {
	ctx     context.Context
	task    *Task
	handler HandlerFunc
	resChan chan<- error
}

func NewWorkerPool(rdb *redis.Client, logger *zap.Logger, queue string, opts ...WorkerOptions) Worker {
	pool := &workerPool{
		rdb:           rdb,
		logger:        logger,
		queue:         queue,
		group:         "taskmq-group",
		consumer:      "taskmq-consumer-1",
		concurrency:   5,
		codec:         JSONCodec{},
		syncExecution: false,
		execPoolSize:  5,
		handlers:      make(map[string]HandlerFunc),
	}

	if len(opts) > 0 {
		opt := opts[0]
		if opt.Group != "" {
			pool.group = opt.Group
		}
		if opt.Consumer != "" {
			pool.consumer = opt.Consumer
		}
		if opt.Concurrency > 0 {
			pool.concurrency = opt.Concurrency
			pool.execPoolSize = opt.Concurrency
		}
		if opt.Codec != nil {
			pool.codec = opt.Codec
		}
		pool.syncExecution = opt.SyncExecution
		if opt.ExecutionPoolSize > 0 {
			pool.execPoolSize = opt.ExecutionPoolSize
		}
	}

	pool.cronManager = newCronManager(rdb, logger, queue, pool.codec)
	pool.scheduler = newDelayedScheduler(rdb, logger, queue, pool.cronManager, pool.codec)
	pool.janitor = newPELRecoveryJanitor(rdb, logger, queue, pool.group, pool.consumer, pool.concurrency, func(ctx context.Context, msg redis.XMessage) {
		pool.processMessage(ctx, StreamKey(queue), msg)
	})

	return pool
}

// Register registers a handler function for a specific task name
func (w *workerPool) Register(taskName string, handler HandlerFunc) {
	w.handlers[taskName] = handler
}

// Start starts the worker pool consumers, scheduler, and janitor loops
func (w *workerPool) Start(ctx context.Context) error {
	streamKey := StreamKey(w.queue)

	// Create Consumer Group. Ignore BUSYGROUP error if it already exists.
	err := w.rdb.XGroupCreateMkStream(ctx, streamKey, w.group, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	w.logger.Info("Starting TaskMQ worker pool",
		zap.String("queue", w.queue),
		zap.String("group", w.group),
		zap.Int("concurrency", w.concurrency),
	)

	// Create a long-running context for the worker loop, independent of the short startup ctx
	w.ctx, w.cancel = context.WithCancel(context.Background())

	// Start execution pool if not in synchronous execution mode
	if !w.syncExecution {
		w.execChan = make(chan taskExecutionRequest, w.execPoolSize*2)
		w.startExecutionPool(w.ctx)
	}

	// 1. Start workers
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.worker(streamKey)
	}

	// 2. Start Scheduler loop (ZSET -> Stream)
	w.wg.Add(1)
	go w.scheduler.Start(w.ctx, &w.wg)

	// 3. Start Janitor loop (PEL Recovery)
	w.wg.Add(1)
	go w.janitor.Start(w.ctx, &w.wg)

	// 4. Start Cron Self-healing loop
	w.wg.Add(1)
	go w.cronManager.Start(w.ctx, &w.wg)

	return nil
}

// Stop stops the worker pool gracefully
func (w *workerPool) Stop() {
	w.logger.Info("Stopping TaskMQ worker pool gracefully")
	if w.cancel != nil {
		w.cancel()
	}
	if w.execChan != nil {
		close(w.execChan)
	}
	w.wg.Wait()
	w.logger.Info("TaskMQ worker pool stopped")
}

func (w *workerPool) startExecutionPool(ctx context.Context) {
	w.logger.Info("Starting TaskMQ execution pool", zap.Int("size", w.execPoolSize))
	for i := 0; i < w.execPoolSize; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for req := range w.execChan {
				req.resChan <- w.runHandlerWithRecovery(req.ctx, req.task, req.handler)
			}
		}()
	}
}

func (w *workerPool) runHandlerWithRecovery(ctx context.Context, task *Task, handler HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("task handler panicked: %v", r)
			w.logger.Error("Panic recovered in task handler execution",
				zap.String("task_id", task.ID),
				zap.String("task_name", task.Name),
				zap.Any("panic", r),
			)
		}
	}()
	return handler(ctx, task)
}

func (w *workerPool) worker(streamKey string) {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			// Read messages from the stream using the long-running context w.ctx
			streams, err := w.rdb.XReadGroup(w.ctx, &redis.XReadGroupArgs{
				Group:    w.group,
				Consumer: w.consumer,
				Streams:  []string{streamKey, ">"},
				Count:    1,
				Block:    time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					// Timeout block, no new messages
					continue
				}
				// Silently ignore standard network I/O timeouts
				if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
					continue
				}
				if w.ctx.Err() != nil {
					return
				}
				w.logger.Error("Worker error reading stream", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					w.processMessage(w.ctx, streamKey, msg)
				}
			}
		}
	}
}

func (w *workerPool) processMessage(ctx context.Context, streamKey string, msg redis.XMessage) {
	taskData, ok := msg.Values["task"].(string)
	if !ok {
		w.logger.Error("Invalid message payload format, missing 'task' field")
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	task := &Task{}
	err := w.codec.Unmarshal([]byte(taskData), task)
	if err != nil {
		w.logger.Error("Failed to deserialize task", zap.Error(err))
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	handler, exists := w.handlers[task.Name]
	if !exists {
		w.logger.Warn("No handler registered for task", zap.String("task_name", task.Name))
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	// 1. Enforce timeout context
	timeout := time.Duration(task.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second // Default fallback timeout
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if w.syncExecution {
		err = w.runHandlerWithRecovery(timeoutCtx, task, handler)
		if err != nil {
			w.logger.Error("Task handler failed",
				zap.String("task_id", task.ID),
				zap.String("task_name", task.Name),
				zap.Error(err),
			)
			w.handleFailure(ctx, streamKey, msg, task, err)
			return
		}
	} else {
		// Channel must be buffered to prevent goroutine leak if we return early due to timeout
		errChan := make(chan error, 1)

		// Try submitting task to the execution pool with timeout
		select {
		case w.execChan <- taskExecutionRequest{
			ctx:     timeoutCtx,
			task:    task,
			handler: handler,
			resChan: errChan,
		}:
		case <-timeoutCtx.Done():
			err = timeoutCtx.Err()
			w.logger.Error("Task timed out waiting in execution queue",
				zap.String("task_id", task.ID),
				zap.String("task_name", task.Name),
				zap.Error(err),
			)
			w.handleFailure(ctx, streamKey, msg, task, err)
			return
		}

		// Wait for execution result or timeout
		select {
		case err = <-errChan:
			if err != nil {
				w.logger.Error("Task handler failed",
					zap.String("task_id", task.ID),
					zap.String("task_name", task.Name),
					zap.Error(err),
				)
				w.handleFailure(ctx, streamKey, msg, task, err)
				return
			}
		case <-timeoutCtx.Done():
			err = timeoutCtx.Err() // context.DeadlineExceeded
			w.logger.Error("Task handler timed out",
				zap.String("task_id", task.ID),
				zap.String("task_name", task.Name),
				zap.Duration("timeout", timeout),
				zap.Error(err),
			)
			w.handleFailure(ctx, streamKey, msg, task, err)
			return
		}
	}

	// Acknowledge successfully processed task
	err = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
	if err != nil {
		w.logger.Error("Failed to acknowledge message ID",
			zap.String("message_id", msg.ID),
			zap.Error(err),
		)
	} else {
		w.releaseUniqueLock(ctx, task)
	}
}

const luaHandleFailure = `
	local op = ARGV[1]
	local msgID = ARGV[2]
	local group = ARGV[3]
	local score = tonumber(ARGV[4])
	local serializedTask = ARGV[5]

	-- Safety Check: Perform ZADD (reschedule/archive) BEFORE XACK.
	-- Since Redis Lua has no rollback, if ZADD fails, the script aborts
	-- and the message remains in the stream's PEL to prevent task loss.
	redis.call('ZADD', KEYS[1], score, serializedTask)

	if op == 'dlq' then
		-- Limit DLQ size to latest 1000 items
		redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -1001)
		
		-- Release unique lock if provided
		local uniqueKey = KEYS[3]
		local uniqueKeyVal = ARGV[6]
		if uniqueKey ~= '' and uniqueKeyVal ~= '' then
			if redis.call('GET', uniqueKey) == uniqueKeyVal then
				redis.call('DEL', uniqueKey)
			end
		end
	end

	-- ACK the message in the stream to remove it from PEL
	return redis.call('XACK', KEYS[2], group, msgID)
`

func (w *workerPool) handleFailure(ctx context.Context, streamKey string, msg redis.XMessage, task *Task, err error) {
	task.Retry++

	if task.Retry >= task.MaxRetry {
		task.LastError = err.Error()
		w.logger.Error("Task exhausted all retries, moving to DLQ",
			zap.String("task_id", task.ID),
			zap.String("task_name", task.Name),
			zap.Int("retry", task.Retry),
			zap.String("error", task.LastError),
		)

		// Serialize and store in DLQ ZSET
		serialized, _ := w.codec.Marshal(task)
		dlqKey := DLQKey(task.Queue)
		nowMs := time.Now().UnixMilli()

		var uniqueLockKey string
		var uniqueLockVal string
		if task.UniqueKey != "" {
			uniqueLockKey = UniqueKey(task.Queue, task.UniqueKey)
			uniqueLockVal = task.ID
		}

		// Execute atomic DLQ move, lock release, and ACK
		_, zerr := w.rdb.Eval(ctx, luaHandleFailure, []string{dlqKey, streamKey, uniqueLockKey}, "dlq", msg.ID, w.group, nowMs, string(serialized), uniqueLockVal).Result()
		if zerr != nil {
			w.logger.Error("Failed to move task to DLQ atomically", zap.Error(zerr))
		}
		return
	}

	// Exponential backoff: 2^retry * 100ms (faster for tests, scalable for prod)
	backoff := time.Millisecond * time.Duration(100*(1<<uint(task.Retry)))

	w.logger.Info("Scheduling task retry with exponential backoff",
		zap.String("task_id", task.ID),
		zap.String("task_name", task.Name),
		zap.Int("retry", task.Retry),
		zap.Duration("backoff", backoff),
	)

	// Serialize updated task state
	serialized, err := w.codec.Marshal(task)
	if err != nil {
		w.logger.Error("Failed to serialize retry task", zap.Error(err))
		return
	}

	delayedKey := DelayedKey(task.Queue)
	at := time.Now().Add(backoff)

	// Execute atomic retry schedule and ACK
	_, zerr := w.rdb.Eval(ctx, luaHandleFailure, []string{delayedKey, streamKey, ""}, "retry", msg.ID, w.group, at.UnixMilli(), string(serialized), "").Result()
	if zerr != nil {
		w.logger.Error("Failed to schedule task retry atomically", zap.Error(zerr))
	}
}

func (w *workerPool) releaseUniqueLock(ctx context.Context, task *Task) {
	if task.UniqueKey == "" {
		return
	}
	uniqueKey := UniqueKey(task.Queue, task.UniqueKey)
	luaUnlock := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
	err := w.rdb.Eval(ctx, luaUnlock, []string{uniqueKey}, task.ID).Err()
	if err != nil {
		w.logger.Error("Failed to release task uniqueness lock",
			zap.String("task_id", task.ID),
			zap.String("unique_key", task.UniqueKey),
			zap.Error(err),
		)
	}
}

