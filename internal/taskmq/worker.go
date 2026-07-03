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
	Stop(ctxs ...context.Context)
}

type workerPool struct {
	rdb            *redis.Client
	logger         *zap.Logger
	queue          string
	group          string
	consumer       string
	concurrency    int
	handlers       map[string]HandlerFunc
	scheduler      Runner
	janitor        Runner
	cronManager    CronManager
	codec          Codec
	syncExecution  bool
	execPoolSize   int
	sem            chan struct{}
	parentCtx      context.Context
	ctx            context.Context
	cancel         context.CancelFunc
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	wg             sync.WaitGroup
}

type WorkerOptions struct {
	Group               string
	Consumer            string
	Concurrency         int
	Codec               Codec
	SyncExecution       bool
	ExecutionPoolSize   int
	CronHealingInterval      time.Duration
	CronHealingLockTTL       time.Duration
	CronHealingScanBatchSize int
	CronHealingScanMaxCount  int

	// Dependency Injections
	CronManager         CronManager
	Scheduler           Runner
	Janitor             Runner

	// Scheduler & Janitor Tick Intervals (DIP / Configurable tickers)
	SchedulerPollInterval time.Duration
	JanitorInterval       time.Duration
	JanitorMinIdleTime    time.Duration

	// Parent context for the worker pool execution lifecycle
	Context context.Context
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
		parentCtx:     context.Background(),
	}

	cronHealingInterval := 1 * time.Minute
	cronHealingLockTTL := 50 * time.Second
	cronHealingScanBatchSize := 100
	cronHealingScanMaxCount := 1000
	schedulerPollInterval := 500 * time.Millisecond
	janitorInterval := 3 * time.Second
	janitorMinIdleTime := 5 * time.Second

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
		if opt.CronHealingInterval > 0 {
			cronHealingInterval = opt.CronHealingInterval
		}
		if opt.CronHealingLockTTL > 0 {
			cronHealingLockTTL = opt.CronHealingLockTTL
		}
		if opt.CronHealingScanBatchSize > 0 {
			cronHealingScanBatchSize = opt.CronHealingScanBatchSize
		}
		if opt.CronHealingScanMaxCount > 0 {
			cronHealingScanMaxCount = opt.CronHealingScanMaxCount
		}
		if opt.Context != nil {
			pool.parentCtx = opt.Context
		}
		if opt.CronManager != nil {
			pool.cronManager = opt.CronManager
		}
		if opt.Scheduler != nil {
			pool.scheduler = opt.Scheduler
		}
		if opt.Janitor != nil {
			pool.janitor = opt.Janitor
		}
		if opt.SchedulerPollInterval > 0 {
			schedulerPollInterval = opt.SchedulerPollInterval
		}
		if opt.JanitorInterval > 0 {
			janitorInterval = opt.JanitorInterval
		}
		if opt.JanitorMinIdleTime > 0 {
			janitorMinIdleTime = opt.JanitorMinIdleTime
		}
	}

	pool.sem = make(chan struct{}, pool.execPoolSize)

	if pool.cronManager == nil {
		pool.cronManager = newCronManager(rdb, logger, queue, pool.codec, cronHealingInterval, cronHealingLockTTL, cronHealingScanBatchSize, cronHealingScanMaxCount)
	}
	if pool.scheduler == nil {
		pool.scheduler = newDelayedScheduler(rdb, logger, queue, pool.cronManager, pool.codec, schedulerPollInterval)
	}
	if pool.janitor == nil {
		pool.janitor = newPELRecoveryJanitor(rdb, logger, queue, pool.group, pool.consumer, pool.concurrency, janitorInterval, janitorMinIdleTime, nil)
	}

	if j, ok := pool.janitor.(PELRecoveryJanitor); ok {
		j.RegisterProcessor(func(ctx context.Context, msg redis.XMessage) {
			pool.processMessage(ctx, StreamKey(queue), msg)
		})
	}

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

	// Create contexts for the active tasks and consumers/background loops
	w.ctx, w.cancel = context.WithCancel(w.parentCtx)
	w.consumerCtx, w.consumerCancel = context.WithCancel(w.ctx)

	// 1. Start workers
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.worker(streamKey)
	}

	// 2. Start Scheduler loop (ZSET -> Stream)
	w.wg.Add(1)
	go w.runBackgroundLoop(w.scheduler, "scheduler")

	// 3. Start Janitor loop (PEL Recovery)
	w.wg.Add(1)
	go w.runBackgroundLoop(w.janitor, "janitor")

	// 4. Start Cron Self-healing loop
	w.wg.Add(1)
	go w.runBackgroundLoop(w.cronManager, "cron_manager")

	return nil
}

// Stop stops the worker pool gracefully
func (w *workerPool) Stop(ctxs ...context.Context) {
	w.logger.Info("Stopping TaskMQ worker pool gracefully")
	if w.consumerCancel != nil {
		w.consumerCancel()
	}

	shutdownTimeout := 10 * time.Second
	var parentCtx context.Context
	if len(ctxs) > 0 && ctxs[0] != nil {
		parentCtx = ctxs[0]
		if deadline, ok := parentCtx.Deadline(); ok {
			shutdownTimeout = time.Until(deadline)
			// Add a small safety buffer (e.g. 500ms) so we force-cancel before Fx times out the hook
			if shutdownTimeout > 500*time.Millisecond {
				shutdownTimeout -= 500 * time.Millisecond
			} else {
				shutdownTimeout = 100 * time.Millisecond
			}
		}
	} else {
		parentCtx = context.Background()
	}

	// Wait for running tasks with a shutdown timeout
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("All tasks finished, worker pool stopped")
	case <-time.After(shutdownTimeout):
		w.logger.Warn("Shutdown timeout reached, force cancelling running tasks")
		if w.cancel != nil {
			w.cancel() // cancels w.ctx, which cancels active task contexts
		}
		<-done
	case <-parentCtx.Done():
		w.logger.Warn("Shutdown context canceled, force cancelling running tasks")
		if w.cancel != nil {
			w.cancel()
		}
		<-done
	}
	w.logger.Info("TaskMQ worker pool stopped")
}

func (w *workerPool) runBackgroundLoop(runner Runner, name string) {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("Panic recovered in background loop",
				zap.String("loop_name", name),
				zap.Any("panic", r),
			)
		}
	}()
	if err := runner.Run(w.consumerCtx); err != nil && err != context.Canceled {
		w.logger.Error("Background loop returned error",
			zap.String("loop_name", name),
			zap.Error(err),
		)
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
		case <-w.consumerCtx.Done():
			return
		default:
			// 1. Acquire execution slot token before pulling task from Redis
			select {
			case <-w.consumerCtx.Done():
				return
			case w.sem <- struct{}{}:
			}

			// Read messages from the stream using the long-running context w.consumerCtx
			streams, err := w.rdb.XReadGroup(w.consumerCtx, &redis.XReadGroupArgs{
				Group:    w.group,
				Consumer: w.consumer,
				Streams:  []string{streamKey, ">"},
				Count:    1,
				Block:    time.Second,
			}).Result()

			if err != nil {
				w.releaseToken() // Release token since we didn't get any message
				if err == redis.Nil {
					// Timeout block, no new messages
					continue
				}
				// Silently ignore standard network I/O timeouts
				if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
					continue
				}
				if w.consumerCtx.Err() != nil {
					return
				}
				w.logger.Error("Worker error reading stream", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			msgPulled := false
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					msgPulled = true
					if w.syncExecution {
						w.processMessage(w.ctx, streamKey, msg)
						w.releaseToken()
					} else {
						// Spawn a goroutine for async processing so the consumer thread
						// doesn't block and can immediately pull the next task.
						w.wg.Add(1)
						go func(m redis.XMessage) {
							defer w.wg.Done()
							defer w.releaseToken()
							w.processMessage(w.ctx, streamKey, m)
						}(msg)
					}
				}
			}

			if !msgPulled {
				w.releaseToken() // Release token if no messages were actually pulled
			}
		}
	}
}

func (w *workerPool) releaseToken() {
	select {
	case <-w.sem:
	default:
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

	err = w.runHandlerWithRecovery(timeoutCtx, task, handler)
	if err != nil {
		// Check if the parent context (w.ctx) was cancelled, which means the worker pool is stopping
		if w.ctx.Err() != nil {
			w.logger.Warn("Task aborted due to worker pool shutdown, leaving in PEL for recovery",
				zap.String("task_id", task.ID),
				zap.String("task_name", task.Name),
			)
			return
		}

		w.logger.Error("Task handler failed",
			zap.String("task_id", task.ID),
			zap.String("task_name", task.Name),
			zap.Error(err),
		)
		w.handleFailure(ctx, streamKey, msg, task, err)
		return
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

