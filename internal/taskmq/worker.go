package taskmq

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
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

type QueuePriority struct {
	Name              string
	Weight            int
	RateLimitMax      int64
	RateLimitDuration time.Duration
	RateLimitKeyField string
}

type workerPool struct {
	rdb              *redis.Client
	logger           *zap.Logger
	queue            string
	queues           []QueuePriority
	priorityStrategy string
	group            string
	consumer         string
	concurrency      int
	handlers         map[string]HandlerFunc
	scheduler        Runner
	janitor          Runner
	cronManager      CronManager
	codec            Codec
	syncExecution    bool
	execPoolSize     int
	sem              chan struct{}
	parentCtx        context.Context
	ctx              context.Context
	cancel           context.CancelFunc
	consumerCtx      context.Context
	consumerCancel   context.CancelFunc
	wg               sync.WaitGroup

	// Multi-queue support mapping
	schedulers   map[string]Runner
	janitors     map[string]Runner
	cronManagers map[string]CronManager

	// Rate Limiting
	limiter           *GCRALimiter
	rateLimitMax      int64
	rateLimitDuration time.Duration
	rateLimitKeyField string
}

type WorkerOptions struct {
	Group                    string
	Consumer                 string
	Concurrency              int
	Codec                    Codec
	SyncExecution            bool
	ExecutionPoolSize        int
	CronHealingInterval      time.Duration
	CronHealingLockTTL       time.Duration
	CronHealingScanBatchSize int
	CronHealingScanMaxCount  int

	// Dependency Injections
	CronManager CronManager
	Scheduler   Runner
	Janitor     Runner

	// Scheduler & Janitor Tick Intervals (DIP / Configurable tickers)
	SchedulerPollInterval time.Duration
	JanitorInterval       time.Duration
	JanitorMinIdleTime    time.Duration

	// Parent context for the worker pool execution lifecycle
	Context context.Context

	// Priority Queues Settings
	PriorityQueues   []QueuePriority
	PriorityStrategy string // "strict" or "weighted"

	// Rate Limiting Settings
	RateLimitMax      int64
	RateLimitDuration time.Duration
	RateLimitKeyField string
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
		schedulers:    make(map[string]Runner),
		janitors:      make(map[string]Runner),
		cronManagers:  make(map[string]CronManager),
		limiter:       NewGCRALimiter(rdb),
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
		if opt.Context != nil {
			pool.parentCtx = opt.Context
		}

		pool.queues = opt.PriorityQueues
		pool.priorityStrategy = opt.PriorityStrategy
		if pool.priorityStrategy == "" {
			pool.priorityStrategy = "weighted"
		}

		if len(pool.queues) > 0 {
			// Multi-queue priority mode
			cronHealingInterval := opt.CronHealingInterval
			if cronHealingInterval <= 0 {
				cronHealingInterval = 1 * time.Minute
			}
			cronHealingLockTTL := opt.CronHealingLockTTL
			if cronHealingLockTTL <= 0 {
				cronHealingLockTTL = 50 * time.Second
			}
			cronHealingScanBatchSize := opt.CronHealingScanBatchSize
			if cronHealingScanBatchSize <= 0 {
				cronHealingScanBatchSize = 100
			}
			cronHealingScanMaxCount := opt.CronHealingScanMaxCount
			if cronHealingScanMaxCount <= 0 {
				cronHealingScanMaxCount = 1000
			}
			schedulerPollInterval := opt.SchedulerPollInterval
			if schedulerPollInterval <= 0 {
				schedulerPollInterval = 500 * time.Millisecond
			}
			janitorInterval := opt.JanitorInterval
			if janitorInterval <= 0 {
				janitorInterval = 3 * time.Second
			}
			janitorMinIdleTime := opt.JanitorMinIdleTime
			if janitorMinIdleTime <= 0 {
				janitorMinIdleTime = 5 * time.Second
			}

			for _, q := range pool.queues {
				qCron := newCronManager(rdb, logger, q.Name, pool.codec, cronHealingInterval, cronHealingLockTTL, cronHealingScanBatchSize, cronHealingScanMaxCount)
				qSched := newDelayedScheduler(rdb, logger, q.Name, qCron, pool.codec, schedulerPollInterval)
				qJan := newPELRecoveryJanitor(rdb, logger, q.Name, pool.group, pool.consumer, pool.concurrency, janitorInterval, janitorMinIdleTime, nil)

				pool.cronManagers[q.Name] = qCron
				pool.schedulers[q.Name] = qSched
				pool.janitors[q.Name] = qJan
			}

			if len(pool.queues) > 0 {
				firstQ := pool.queues[0].Name
				pool.cronManager = pool.cronManagers[firstQ]
				pool.scheduler = pool.schedulers[firstQ]
				pool.janitor = pool.janitors[firstQ]
			}
		} else {
			// Single-queue mode
			if opt.CronManager != nil {
				pool.cronManager = opt.CronManager
				pool.cronManagers[queue] = opt.CronManager
			}
			if opt.Scheduler != nil {
				pool.scheduler = opt.Scheduler
				pool.schedulers[queue] = opt.Scheduler
			}
			if opt.Janitor != nil {
				pool.janitor = opt.Janitor
				pool.janitors[queue] = opt.Janitor
			}

			pool.rateLimitMax = opt.RateLimitMax
			pool.rateLimitDuration = opt.RateLimitDuration
			pool.rateLimitKeyField = opt.RateLimitKeyField

			if pool.cronManager == nil || pool.scheduler == nil || pool.janitor == nil {
				panic("NewWorkerPool: CronManager, Scheduler, and Janitor must be provided in WorkerOptions")
			}
		}
	} else {
		panic("NewWorkerPool: WorkerOptions must be provided")
	}

	pool.sem = make(chan struct{}, pool.execPoolSize)

	// Register processors for all janitors
	for qName, jan := range pool.janitors {
		if j, ok := jan.(PELRecoveryJanitor); ok {
			name := qName
			j.RegisterProcessor(func(ctx context.Context, msg redis.XMessage) {
				pool.processMessage(ctx, StreamKey(name), msg)
			})
		}
	}

	return pool
}

// Register registers a handler function for a specific task name
func (w *workerPool) Register(taskName string, handler HandlerFunc) {
	w.handlers[taskName] = handler
}

// Start starts the worker pool consumers, scheduler, and janitor loops
func (w *workerPool) Start(ctx context.Context) error {
	// 1. Create Consumer Groups for all queues
	if len(w.queues) > 0 {
		for _, q := range w.queues {
			streamKey := StreamKey(q.Name)
			err := w.rdb.XGroupCreateMkStream(ctx, streamKey, w.group, "$").Err()
			if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
				return fmt.Errorf("failed to create consumer group for queue %s: %w", q.Name, err)
			}
		}
	} else {
		streamKey := StreamKey(w.queue)
		err := w.rdb.XGroupCreateMkStream(ctx, streamKey, w.group, "$").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("failed to create consumer group: %w", err)
		}
	}

	w.logger.Info("Starting TaskMQ worker pool",
		zap.String("queue", w.queue),
		zap.Any("queues", w.queues),
		zap.String("group", w.group),
		zap.Int("concurrency", w.concurrency),
	)

	// Create contexts for the active tasks and consumers/background loops
	w.ctx, w.cancel = context.WithCancel(w.parentCtx)
	w.consumerCtx, w.consumerCancel = context.WithCancel(w.ctx)

	// 2. Start workers
	if len(w.queues) > 0 {
		for i := 0; i < w.concurrency; i++ {
			w.wg.Add(1)
			go w.worker("")
		}
	} else {
		streamKey := StreamKey(w.queue)
		for i := 0; i < w.concurrency; i++ {
			w.wg.Add(1)
			go w.worker(streamKey)
		}
	}

	// 3. Start Scheduler, Janitor, and CronManager loops for all queues
	for qName, sched := range w.schedulers {
		w.wg.Add(1)
		go w.runBackgroundLoop(sched, "scheduler-"+qName)
	}

	for qName, jan := range w.janitors {
		w.wg.Add(1)
		go w.runBackgroundLoop(jan, "janitor-"+qName)
	}

	for qName, cronMgr := range w.cronManagers {
		w.wg.Add(1)
		go w.runBackgroundLoop(cronMgr, "cron_manager-"+qName)
	}

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
			w.cancel()
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

func shuffleQueues(queues []QueuePriority) []string {
	temp := make([]QueuePriority, len(queues))
	copy(temp, queues)

	var result []string
	for len(temp) > 0 {
		totalWeight := 0
		for _, q := range temp {
			wt := q.Weight
			if wt <= 0 {
				wt = 1
			}
			totalWeight += wt
		}

		if totalWeight == 0 {
			for _, q := range temp {
				result = append(result, q.Name)
			}
			break
		}

		r := rand.Intn(totalWeight)
		currentSum := 0
		idx := -1
		for i, q := range temp {
			wt := q.Weight
			if wt <= 0 {
				wt = 1
			}
			currentSum += wt
			if r < currentSum {
				idx = i
				break
			}
		}

		if idx != -1 {
			result = append(result, temp[idx].Name)
			temp = append(temp[:idx], temp[idx+1:]...)
		} else {
			result = append(result, temp[0].Name)
			temp = temp[1:]
		}
	}
	return result
}

func sortQueues(queues []QueuePriority) []string {
	temp := make([]QueuePriority, len(queues))
	copy(temp, queues)

	sort.Slice(temp, func(i, j int) bool {
		return temp[i].Weight > temp[j].Weight
	})

	result := make([]string, len(temp))
	for i, q := range temp {
		result[i] = q.Name
	}
	return result
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

			var streams []redis.XStream
			var err error

			if len(w.queues) > 0 {
				var orderedQueues []string
				if w.priorityStrategy == "strict" {
					orderedQueues = sortQueues(w.queues)
				} else {
					orderedQueues = shuffleQueues(w.queues)
				}

				// Check rate limits and determine which queues are currently allowed
				var nonLimitedQueues []string
				var minWait time.Duration
				for _, qName := range orderedQueues {
					rlMax, rlDuration, rlKeyField := w.getQueueRateLimit(qName)
					if rlMax > 0 && rlDuration > 0 && rlKeyField == "" {
						limitKey := RateLimitKey(qName, "")
						wait, checkErr := w.limiter.Check(w.consumerCtx, limitKey, rlMax, rlDuration)
						if checkErr == nil && wait > 0 {
							w.logger.Debug("Queue rate-limited, skipping in polling", zap.String("queue", qName), zap.Duration("wait", wait))
							if wait < minWait || minWait == 0 {
								minWait = wait
							}
							continue
						}
					}
					nonLimitedQueues = append(nonLimitedQueues, qName)
				}

				if len(nonLimitedQueues) == 0 {
					// All queues are currently rate limited! Sleep and retry
					w.releaseToken()
					if minWait == 0 {
						minWait = time.Second
					}
					select {
					case <-w.consumerCtx.Done():
						return
					case <-time.After(minWait):
					}
					continue
				}

				// 1. Try non-blocking check on each non-limited queue in priority order first to prevent priority inversion under load
				found := false
				for _, qName := range nonLimitedQueues {
					sKey := StreamKey(qName)
					res, readErr := w.rdb.XReadGroup(w.consumerCtx, &redis.XReadGroupArgs{
						Group:    w.group,
						Consumer: w.consumer,
						Streams:  []string{sKey, ">"},
						Count:    1,
						Block:    -1, // True non-blocking check (omits BLOCK parameter)
					}).Result()

					if readErr != nil && readErr != redis.Nil {
						w.logger.Error("Non-blocking priority check error", zap.String("queue", qName), zap.Error(readErr))
					} else {
						w.logger.Debug("Non-blocking priority check", zap.String("queue", qName), zap.Int("len_res", len(res)))
					}

					if readErr == nil && len(res) > 0 && len(res[0].Messages) > 0 {
						streams = res
						found = true
						break
					}
				}

				// 2. If no messages found in any non-limited queue, do a blocking read on non-limited queues
				if !found {
					streamsArg := make([]string, 2*len(nonLimitedQueues))
					for i, qName := range nonLimitedQueues {
						streamsArg[i] = StreamKey(qName)
					}
					for i := 0; i < len(nonLimitedQueues); i++ {
						streamsArg[len(nonLimitedQueues)+i] = ">"
					}

					streams, err = w.rdb.XReadGroup(w.consumerCtx, &redis.XReadGroupArgs{
						Group:    w.group,
						Consumer: w.consumer,
						Streams:  streamsArg,
						Count:    1,
						Block:    time.Second,
					}).Result()
				}
			} else {
				rlMax, rlDuration, rlKeyField := w.getQueueRateLimit(w.queue)
				if rlMax > 0 && rlDuration > 0 && rlKeyField == "" {
					limitKey := RateLimitKey(w.queue, "")
					wait, checkErr := w.limiter.Check(w.consumerCtx, limitKey, rlMax, rlDuration)
					if checkErr == nil && wait > 0 {
						w.releaseToken()
						select {
						case <-w.consumerCtx.Done():
							return
						case <-time.After(wait):
						}
						continue
					}
				}

				streams, err = w.rdb.XReadGroup(w.consumerCtx, &redis.XReadGroupArgs{
					Group:    w.group,
					Consumer: w.consumer,
					Streams:  []string{streamKey, ">"},
					Count:    1,
					Block:    time.Second,
				}).Result()
			}

			if err != nil {
				w.releaseToken() // Release token since we didn't get any message
				if w.consumerCtx.Err() != nil {
					return
				}
				if err == redis.Nil {
					continue
				}
				if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
					continue
				}
				w.logger.Error("Worker error reading stream", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			msgPulled := false
			firstMsg := true
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					msgPulled = true
					if !firstMsg {
						select {
						case <-w.consumerCtx.Done():
							return
						case w.sem <- struct{}{}:
						}
					}
					firstMsg = false

					if w.syncExecution {
						w.processMessage(w.ctx, stream.Stream, msg)
						w.releaseToken()
					} else {
						w.wg.Add(1)
						go func(s string, m redis.XMessage) {
							defer w.wg.Done()
							defer w.releaseToken()
							w.processMessage(w.ctx, s, m)
						}(stream.Stream, msg)
					}
				}
			}

			if !msgPulled {
				w.releaseToken()
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

	// Rate Limit Check & Consume (Phase 2)
	rlMax, rlDuration, rlKeyField := w.getQueueRateLimit(task.Queue)
	if rlMax > 0 && rlDuration > 0 {
		groupKeyVal := ""
		if rlKeyField != "" {
			groupKeyVal = extractGroupKey(task.Payload, rlKeyField)
		}
		limitKey := RateLimitKey(task.Queue, groupKeyVal)
		wait, rlErr := w.limiter.TryConsume(ctx, limitKey, rlMax, rlDuration)
		if rlErr != nil {
			w.logger.Error("Failed to apply rate limiting", zap.Error(rlErr))
		} else if wait > 0 {
			w.logger.Debug("Rate limit exceeded, deferring task", zap.String("queue", task.Queue), zap.String("group_key", groupKeyVal), zap.Duration("wait", wait))
			_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
			_ = w.rdb.XDel(ctx, streamKey, msg.ID).Err()
			_ = w.enqueueDelayed(ctx, task, wait)
			return
		}
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

func RateLimitKey(queue string, groupKey string) string {
	if groupKey != "" {
		return fmt.Sprintf("taskmq:{%s}:rate_limit:%s", queue, groupKey)
	}
	return fmt.Sprintf("taskmq:{%s}:rate_limit", queue)
}

func (w *workerPool) getQueueRateLimit(qName string) (int64, time.Duration, string) {
	if len(w.queues) > 0 {
		for _, q := range w.queues {
			if q.Name == qName {
				return q.RateLimitMax, q.RateLimitDuration, q.RateLimitKeyField
			}
		}
	}
	if qName == w.queue {
		return w.rateLimitMax, w.rateLimitDuration, w.rateLimitKeyField
	}
	return 0, 0, ""
}

func (w *workerPool) enqueueDelayed(ctx context.Context, task *Task, delay time.Duration) error {
	serialized, err := w.codec.Marshal(task)
	if err != nil {
		return err
	}
	delayedKey := DelayedKey(task.Queue)
	at := time.Now().Add(delay)
	return w.rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: string(serialized),
	}).Err()
}

func extractGroupKey(payload []byte, field string) string {
	if len(payload) == 0 || field == "" {
		return ""
	}
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return ""
	}
	if val, ok := data[field]; ok {
		return fmt.Sprintf("%v", val)
	}
	return ""
}
