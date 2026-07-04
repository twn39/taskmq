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

type CoreHandlerFunc func(c *ConsumeContext) error

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
		return fmt.Errorf("failed to create stream or group: %w", err)
	}

	w.ctx, w.cancel = context.WithCancel(w.parentCtx)
	w.consumerCtx, w.consumerCancel = context.WithCancel(w.ctx)

	// Start Scheduler & Janitor loop
	if w.scheduler != nil {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			if err := w.scheduler.Run(w.ctx); err != nil {
				w.logger.Error("Scheduler loop stopped with error", zap.Error(err))
			}
		}()
	}

	if w.janitor != nil {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			if err := w.janitor.Run(w.ctx); err != nil {
				w.logger.Error("Janitor loop stopped with error", zap.Error(err))
			}
		}()
	}

	// Start Cron Manager healing loop
	if w.cronManager != nil {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			if err := w.cronManager.Run(w.ctx); err != nil {
				w.logger.Error("Cron Manager healing loop stopped with error", zap.Error(err))
			}
		}()
	}

	// Start concurrent workers to consume queue
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.runBackgroundLoop(i)
	}

	w.logger.Info("Worker pool started successfully", zap.String("queue", w.queue), zap.Int("concurrency", w.concurrency))
	return nil
}

// Stop gracefully stops the consumer background loops, then waits for in-flight tasks
func (w *workerPool) Stop(ctxs ...context.Context) {
	fmt.Printf("DEBUG: Stop started\n")
	w.logger.Info("Stopping worker pool background loops...")

	// 1. Stop queue stream readers immediately
	if w.consumerCancel != nil {
		fmt.Printf("DEBUG: Stop calling consumerCancel\n")
		w.consumerCancel()
	}

	// 2. Shut down scheduler, janitor and cron healing loops
	if w.cancel != nil {
		fmt.Printf("DEBUG: Stop calling cancel (scheduler/janitor)\n")
		w.cancel()
	}

	// 3. Optional timeout block wait
	done := make(chan struct{})
	go func() {
		fmt.Printf("DEBUG: Stop goroutine waiting on w.wg.Wait()\n")
		w.wg.Wait()
		fmt.Printf("DEBUG: Stop goroutine w.wg.Wait() returned\n")
		close(done)
	}()

	var waitCtx context.Context
	if len(ctxs) > 0 {
		waitCtx = ctxs[0]
	} else {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}

	select {
	case <-done:
		fmt.Printf("DEBUG: Stop case <-done selected\n")
		w.logger.Info("Worker pool gracefully stopped.")
	case <-waitCtx.Done():
		fmt.Printf("DEBUG: Stop case <-waitCtx.Done() selected\n")
		w.logger.Warn("Worker pool shutdown timeout exceeded, forcing stop.")
	}
}

func (w *workerPool) runBackgroundLoop(workerID int) {
	defer w.wg.Done()
	consumerName := fmt.Sprintf("%s-%d", w.consumer, workerID)
	streamKey := StreamKey(w.queue)

	w.logger.Debug("Worker background loop started", zap.String("consumer", consumerName))

	for {
		select {
		case <-w.consumerCtx.Done():
			w.logger.Debug("Worker consumer context cancelled, exiting loop", zap.String("consumer", consumerName))
			return
		default:
			// Read messages from the stream
			streams, err := w.rdb.XReadGroup(w.consumerCtx, &redis.XReadGroupArgs{
				Group:    w.group,
				Consumer: consumerName,
				Streams:  []string{streamKey, ">"},
				Count:    1,
				Block:    -1,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				select {
				case <-w.consumerCtx.Done():
					return
				default:
					w.logger.Error("Failed to read group messages from stream", zap.Error(err))
					time.Sleep(1 * time.Second)
					continue
				}
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					if w.syncExecution {
						w.ProcessMessage(w.consumerCtx, msg)
					} else {
						// Wait for pool execution token
						select {
						case w.sem <- struct{}{}:
						case <-w.consumerCtx.Done():
							return
						}

						w.wg.Add(1)
						go func(m redis.XMessage) {
							defer func() {
								<-w.sem
								w.wg.Done()
							}()
							w.ProcessMessage(w.consumerCtx, m)
						}(msg)
					}
				}
			}
		}
	}
}

func (w *workerPool) ProcessMessage(ctx context.Context, msg redis.XMessage) {
	// Dynamically unpack task
	payload, ok := msg.Values["task"].([]byte)
	if !ok {
		if payloadStr, ok := msg.Values["task"].(string); ok {
			payload = unsafeStringToBytes(payloadStr)
		} else {
			// Fallback to legacy "payload" key
			payload, ok = msg.Values["payload"].([]byte)
			if !ok {
				if payloadStr, ok := msg.Values["payload"].(string); ok {
					payload = unsafeStringToBytes(payloadStr)
				} else {
					w.logger.Error("Message payload or task must be bytes or string")
					return
				}
			}
		}
	}

	var task Task
	err := w.codec.Unmarshal(payload, &task)
	if err != nil {
		w.logger.Error("Failed to deserialize task", zap.Error(err))
		return
	}

	handler, exists := w.handlers[task.Name]
	if !exists {
		w.logger.Error("No handler registered", zap.String("task_name", task.Name))
		return
	}

	// GCRA dynamic limits parsing
	rlMax, rlDuration, rlKeyField := w.getQueueRateLimit(task.Queue)

	// Built-in handlers/middleware chain assembly
	handlers := []CoreHandlerFunc{
		RetryAndDLQMiddleware(w.broker, w.retryPolicy, w.deadLetterPolicy, w.logger),
		RateLimitMiddleware(w.limiter, w.broker, rlMax, rlDuration, rlKeyField, w.logger),
		RecoveryMiddleware(w.logger),
		func(c *ConsumeContext) error {
			if c.Task.TimeoutMs > 0 {
				timeoutCtx, cancel := context.WithTimeout(c.Context, time.Duration(c.Task.TimeoutMs)*time.Millisecond)
				defer cancel()
				oldCtx := c.Context
				c.Context = timeoutCtx
				defer func() { c.Context = oldCtx }()
			}
			return handler(c.Context, c.Task)
		},
	}

	c := &ConsumeContext{
		Context:   ctx,
		Task:      &task,
		MessageID: msg.ID,
		Queue:     w.queue,
		Group:     w.group,
		handlers:  handlers,
		index:     -1,
	}

	execErr := c.Next()

	// If no error occurred during processing chain and it completed fully (not aborted)
	if execErr == nil && !c.IsAborted() {
		streamKey := StreamKey(w.queue)
		err = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		if err != nil {
			w.logger.Error("Failed to ACK task stream message",
				zap.String("task_id", task.ID),
				zap.String("stream_id", msg.ID),
				zap.Error(err),
			)
			return
		}

		if task.UniqueKey != "" {
			_ = w.broker.ReleaseUniqueLock(ctx, &task)
		}
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
