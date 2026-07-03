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
		limiter:       NewGCRALimiter(rdb),
	}

	var opt WorkerOptions
	if len(opts) > 0 {
		opt = opts[0]
	} else {
		panic("NewWorkerPool: WorkerOptions must be provided")
	}
	opt.ApplyDefaults(rdb, logger, queue, pool.codec)

	pool.group = opt.Group
	pool.consumer = opt.Consumer
	pool.concurrency = opt.Concurrency
	pool.execPoolSize = opt.Concurrency
	pool.codec = opt.Codec
	pool.syncExecution = opt.SyncExecution
	if opt.ExecutionPoolSize > 0 {
		pool.execPoolSize = opt.ExecutionPoolSize
	}
	if opt.Context != nil {
		pool.parentCtx = opt.Context
	}

	pool.cronManager = opt.CronManager
	pool.scheduler = opt.Scheduler
	pool.janitor = opt.Janitor

	pool.rateLimitMax = opt.RateLimitMax
	pool.rateLimitDuration = opt.RateLimitDuration
	pool.rateLimitKeyField = opt.RateLimitKeyField

	pool.sem = make(chan struct{}, pool.execPoolSize)

	// Register processors for janitor
	if j, ok := pool.janitor.(PELRecoveryJanitor); ok {
		j.RegisterProcessor(func(ctx context.Context, msg redis.XMessage) {
			pool.ProcessMessage(ctx, msg)
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

	// 2. Start workers
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.worker(streamKey)
	}

	// 3. Start Scheduler, Janitor, and CronManager loops
	if w.scheduler != nil {
		w.wg.Add(1)
		go w.runBackgroundLoop(w.scheduler, "scheduler-"+w.queue)
	}
	if w.janitor != nil {
		w.wg.Add(1)
		go w.runBackgroundLoop(w.janitor, "janitor-"+w.queue)
	}
	if w.cronManager != nil {
		w.wg.Add(1)
		go w.runBackgroundLoop(w.cronManager, "cronManager-"+w.queue)
	}

	return nil
}

// Stop stops the worker pool gracefully
func (w *workerPool) Stop(ctxs ...context.Context) {
	w.logger.Info("Stopping TaskMQ worker pool gracefully")
	if w.consumerCancel != nil {
		w.consumerCancel()
	}

	var ctx context.Context
	if len(ctxs) > 0 {
		ctx = ctxs[0]
	} else {
		ctx = context.Background()
	}

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		w.logger.Warn("Graceful shutdown timeout, cancelling remaining context and forcing exit")
		if w.cancel != nil {
			w.cancel()
		}
		<-done
	case <-done:
		w.logger.Info("TaskMQ worker pool stopped")
	}
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

	err := runner.Run(w.ctx)
	if err != nil && err != context.Canceled {
		w.logger.Error("Background loop failed",
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

			var streams []redis.XStream
			var err error

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
						w.ProcessMessage(w.ctx, msg)
						w.releaseToken()
					} else {
						w.wg.Add(1)
						go func(m redis.XMessage) {
							defer w.wg.Done()
							defer w.releaseToken()
							w.ProcessMessage(w.ctx, m)
						}(msg)
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

func (w *workerPool) ProcessMessage(ctx context.Context, msg redis.XMessage) {
	streamKey := StreamKey(w.queue)
	taskData, ok := msg.Values["task"].(string)
	if !ok {
		w.logger.Error("Invalid message payload format, missing 'task' field")
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	task := &Task{}
	if err := w.codec.Unmarshal([]byte(taskData), task); err != nil {
		w.logger.Error("Failed to deserialize task", zap.Error(err))
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	handler, exists := w.handlers[task.Name]
	if !exists {
		w.logger.Warn("No handler registered for task, leaving in PEL for potential future handler", zap.String("task_name", task.Name))
		return
	}

	// Dynamic GCRA Rate Limiting
	rlMax, rlDuration, rlKeyField := w.getQueueRateLimit(task.Queue)
	if rlMax > 0 && rlDuration > 0 {
		var groupKeyVal string
		if rlKeyField != "" {
			groupKeyVal = extractGroupKey(task.Payload, rlKeyField)
		}
		limitKey := RateLimitKey(task.Queue, groupKeyVal)

		wait, rlErr := w.limiter.Check(ctx, limitKey, rlMax, rlDuration)
		if rlErr != nil {
			w.logger.Error("Failed to apply rate limiting", zap.Error(rlErr))
		} else if wait > 0 {
			w.logger.Debug("Rate limit exceeded, deferring task", zap.String("queue", task.Queue), zap.String("group_key", groupKeyVal), zap.Duration("wait", wait))
			if err := w.deferRateLimitedTask(ctx, msg.ID, task, wait); err != nil {
				w.logger.Error("Failed to defer rate limited task atomically",
					zap.String("task_id", task.ID),
					zap.String("task_name", task.Name),
					zap.Error(err),
				)
			}
			return
		}
	}

	w.logger.Info("Executing task",
		zap.String("task_id", task.ID),
		zap.String("task_name", task.Name),
		zap.Int("retry", task.Retry),
	)

	err := w.runHandlerWithRecovery(ctx, task, handler)
	if err != nil {
		w.logger.Error("Task execution failed",
			zap.String("task_id", task.ID),
			zap.String("task_name", task.Name),
			zap.Error(err),
		)
		w.handleFailure(ctx, msg, task, err)
		return
	}

	// ACK the message in the stream to remove it from PEL
	err = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
	if err != nil {
		w.logger.Error("Failed to ACK task stream message",
			zap.String("task_id", task.ID),
			zap.String("stream_id", msg.ID),
			zap.Error(err),
		)
		return
	}

	// Uniqueness locks release (if lock is active)
	if task.UniqueKey != "" {
		w.releaseUniqueLock(ctx, task)
	}
}

const luaDeferRateLimitedTask = `
	local delayedKey = KEYS[1]
	local streamKey = KEYS[2]
	local group = ARGV[1]
	local msgID = ARGV[2]
	local score = tonumber(ARGV[3])
	local serializedTask = ARGV[4]

	-- Safety Check: Perform ZADD (reschedule/archive) BEFORE XACK.
	redis.call('ZADD', delayedKey, score, serializedTask)

	-- ACK the message in the stream to remove it from PEL
	return redis.call('XACK', streamKey, group, msgID)
`

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

func (w *workerPool) handleFailure(ctx context.Context, msg redis.XMessage, task *Task, err error) {
	task.Retry++
	streamKey := StreamKey(w.queue)

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

func (w *workerPool) deferRateLimitedTask(ctx context.Context, msgID string, task *Task, delay time.Duration) error {
	serialized, err := w.codec.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to serialize rate-limited task: %w", err)
	}
	delayedKey := DelayedKey(task.Queue)
	streamKey := StreamKey(w.queue)
	at := time.Now().Add(delay)

	_, err = w.rdb.Eval(ctx, luaDeferRateLimitedTask, []string{delayedKey, streamKey}, w.group, msgID, at.UnixMilli(), string(serialized)).Result()
	return err
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
