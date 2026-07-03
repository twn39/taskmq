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

	// Decoupled abstractions
	broker           TaskBroker
	retryPolicy      RetryPolicy
	deadLetterPolicy DeadLetterPolicy
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
	CronManager      CronManager
	Scheduler        Runner
	Janitor          Runner
	Broker           TaskBroker       // Decoupled Task Broker
	RetryPolicy      RetryPolicy      // Decoupled Retry Policy
	DeadLetterPolicy DeadLetterPolicy // Decoupled Dead Letter Policy

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

	// Decoupled components
	if opt.Broker != nil {
		pool.broker = opt.Broker
	} else {
		pool.broker = NewRedisBroker(rdb, pool.codec)
	}

	if opt.RetryPolicy != nil {
		pool.retryPolicy = opt.RetryPolicy
	} else {
		pool.retryPolicy = NewExponentialBackoff(100*time.Millisecond, 1*time.Hour, true)
	}

	if opt.DeadLetterPolicy != nil {
		pool.deadLetterPolicy = opt.DeadLetterPolicy
	} else {
		pool.deadLetterPolicy = NewStandardDeadLetterPolicy("", nil)
	}

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
		go w.runBackgroundLoop(w.cronManager, "cron-"+w.queue)
	}

	return nil
}

// Stop stops the worker pool gracefully
func (w *workerPool) Stop(ctxs ...context.Context) {
	w.logger.Info("Stopping TaskMQ worker pool", zap.String("queue", w.queue))

	// Step 1: Terminate active consumer loops so they don't read new messages
	if w.consumerCancel != nil {
		w.consumerCancel()
	}

	// Step 2: Cancel root context so all active processing and background loops stop
	if w.cancel != nil {
		w.cancel()
	}

	// Wait for all workers and background processes to terminate
	waitChan := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(waitChan)
	}()

	var timeoutCtx context.Context
	var cancel context.CancelFunc
	if len(ctxs) > 0 {
		timeoutCtx = ctxs[0]
	} else {
		timeoutCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}

	select {
	case <-waitChan:
		w.logger.Info("All TaskMQ workers and background loops stopped gracefully")
	case <-timeoutCtx.Done():
		w.logger.Warn("Graceful shutdown timed out; some tasks may still be active in background")
	}
}

func (w *workerPool) runBackgroundLoop(runner Runner, name string) {
	defer w.wg.Done()
	w.logger.Info("Starting background runner loop", zap.String("runner", name))
	if err := runner.Run(w.ctx); err != nil && err != context.Canceled {
		w.logger.Error("Background runner stopped with error", zap.String("runner", name), zap.Error(err))
	}
}

func (w *workerPool) worker(streamKey string) {
	defer w.wg.Done()

	for {
		select {
		case <-w.consumerCtx.Done():
			return
		default:
			// Block-reading messages from group. Use block duration 100ms.
			streams, err := w.rdb.XReadGroup(w.consumerCtx, &redis.XReadGroupArgs{
				Group:    w.group,
				Consumer: w.consumer,
				Streams:  []string{streamKey, ">"},
				Count:    1,
				Block:    100 * time.Millisecond,
			}).Result()

			if err != nil {
				if err == redis.Nil || err == context.Canceled || w.consumerCtx.Err() != nil {
					continue
				}
				w.logger.Error("Failed to read messages from stream group", zap.Error(err))
				time.Sleep(200 * time.Millisecond) // exponential backoff fallback
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					if w.syncExecution {
						w.ProcessMessage(w.ctx, msg)
					} else {
						// Concurrency control via semaphore
						select {
						case w.sem <- struct{}{}:
							w.wg.Add(1)
							go func(m redis.XMessage) {
								defer func() {
									<-w.sem
									w.wg.Done()
								}()
								w.ProcessMessage(w.ctx, m)
							}(msg)
						case <-w.consumerCtx.Done():
							return
						}
					}
				}
			}
		}
	}
}

// ProcessMessage unmarshals and executes a task from a stream message
func (w *workerPool) ProcessMessage(ctx context.Context, msg redis.XMessage) {
	streamKey := StreamKey(w.queue)

	taskRaw, ok := msg.Values["task"]
	if !ok {
		w.logger.Error("Message values missing 'task' field; acknowledging corrupted message", zap.String("msg_id", msg.ID))
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	taskStr, ok := taskRaw.(string)
	if !ok {
		w.logger.Error("Message 'task' field is not a string; acknowledging corrupted message", zap.String("msg_id", msg.ID))
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	task := &Task{}
	if err := w.codec.Unmarshal([]byte(taskStr), task); err != nil {
		w.logger.Error("Failed to unmarshal task; acknowledging corrupted message", zap.String("msg_id", msg.ID), zap.Error(err))
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
			runAt := time.Now().Add(wait)
			if err := w.broker.DeferRateLimitedTask(ctx, msg.ID, task, runAt); err != nil {
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
		_ = w.broker.ReleaseUniqueLock(ctx, task)
	}
}

func (w *workerPool) handleFailure(ctx context.Context, msg redis.XMessage, task *Task, err error) {
	task.Retry++
	streamKey := StreamKey(w.queue)

	if !w.retryPolicy.ShouldRetry(task, err) {
		// Run dead-letter hooks/custom routing
		w.deadLetterPolicy.BeforeDeadLetter(ctx, task, err)
		dlqName := w.deadLetterPolicy.DLQQueueName(task)

		w.logger.Error("Task permanently failed, moving to DLQ",
			zap.String("task_id", task.ID),
			zap.String("dlq", dlqName),
			zap.Error(err),
		)

		if err := w.broker.MoveToDLQ(ctx, task, streamKey, msg.ID, w.group, dlqName); err != nil {
			w.logger.Error("Failed to move task to DLQ atomically", zap.Error(err))
		}
		return
	}

	backoff := w.retryPolicy.NextBackoff(task)

	w.logger.Info("Scheduling task retry with backoff",
		zap.String("task_id", task.ID),
		zap.String("task_name", task.Name),
		zap.Int("retry", task.Retry),
		zap.Duration("backoff", backoff),
	)

	runAt := time.Now().Add(backoff)
	if err := w.broker.ScheduleRetry(ctx, task, streamKey, msg.ID, w.group, runAt); err != nil {
		w.logger.Error("Failed to schedule task retry atomically", zap.Error(err))
	}
}

func (w *workerPool) runHandlerWithRecovery(ctx context.Context, task *Task, handler HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("task panicked: %v", r)
		}
	}()
	return handler(ctx, task)
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
