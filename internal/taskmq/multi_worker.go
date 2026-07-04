package taskmq

import (
	"math/rand"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type multiWorker struct {
	workers map[string]Worker
}

// NewMultiQueueWorker returns a MultiQueueWorker that wraps multiple sub-worker instances.
func NewMultiQueueWorker(workers map[string]Worker) MultiQueueWorker {
	return &multiWorker{workers: workers}
}

func (m *multiWorker) Queue(name string) Worker {
	return m.workers[name]
}

func (m *multiWorker) Register(taskName string, handler HandlerFunc) {
	if w, ok := m.workers["default"]; ok {
		w.Register(taskName, handler)
	}
}

func (m *multiWorker) Start(ctx context.Context) error {
	seen := make(map[Worker]bool)
	for _, w := range m.workers {
		if seen[w] {
			continue
		}
		seen[w] = true
		if err := w.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiWorker) Stop(ctxs ...context.Context) {
	var ctx context.Context
	if len(ctxs) > 0 {
		ctx = ctxs[0]
	} else {
		ctx = context.Background()
	}

	var wg sync.WaitGroup
	seen := make(map[Worker]bool)
	for _, w := range m.workers {
		if seen[w] {
			continue
		}
		seen[w] = true
		wg.Add(1)
		go func(worker Worker) {
			defer wg.Done()
			worker.Stop(ctx)
		}(w)
	}
	wg.Wait()
}

type priorityWorker struct {
	rdb              *redis.Client
	logger           *zap.Logger
	queues           []QueuePriority
	priorityStrategy string
	group            string
	consumer         string
	concurrency      int
	handlers         map[string]HandlerFunc
	schedulers       map[string]Runner
	janitors         map[string]Runner
	cronManagers     map[string]CronManager
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
	middlewareChain  []CoreHandlerFunc
}

func NewPriorityWorker(rdb *redis.Client, logger *zap.Logger, opts WorkerOptions) Worker {
	opts.ApplyDefaults(rdb, logger, "", opts.Codec)
	pw := &priorityWorker{
		rdb:              rdb,
		logger:           logger,
		queues:           opts.PriorityQueues,
		priorityStrategy: opts.PriorityStrategy,
		group:            opts.Group,
		consumer:         opts.Consumer,
		concurrency:      opts.Concurrency,
		codec:            opts.Codec,
		syncExecution:    opts.SyncExecution,
		execPoolSize:     opts.Concurrency,
		handlers:         make(map[string]HandlerFunc),
		parentCtx:        context.Background(),
		schedulers:       make(map[string]Runner),
		janitors:         make(map[string]Runner),
		cronManagers:     make(map[string]CronManager),
		limiter:          NewGCRALimiter(rdb),
	}

	if opts.ExecutionPoolSize > 0 {
		pw.execPoolSize = opts.ExecutionPoolSize
	}
	if opts.Context != nil {
		pw.parentCtx = opts.Context
	}
	if pw.priorityStrategy == "" {
		pw.priorityStrategy = "weighted"
	}

	if opts.Broker != nil {
		pw.broker = opts.Broker
	} else {
		pw.broker = NewRedisBroker(rdb, pw.codec)
	}

	if opts.RetryPolicy != nil {
		pw.retryPolicy = opts.RetryPolicy
	} else {
		pw.retryPolicy = NewExponentialBackoff(100*time.Millisecond, 1*time.Hour, true)
	}

	if opts.DeadLetterPolicy != nil {
		pw.deadLetterPolicy = opts.DeadLetterPolicy
	} else {
		pw.deadLetterPolicy = NewStandardDeadLetterPolicy("", nil)
	}

	pw.middlewareChain = []CoreHandlerFunc{
		RetryAndDLQMiddleware(pw.broker, pw.retryPolicy, pw.deadLetterPolicy, pw.logger),
		RateLimitMiddleware(pw.limiter, pw.broker, pw.getQueueRateLimit, pw.logger),
		RecoveryMiddleware(pw.logger),
		func(c *ConsumeContext) error {
			handler, exists := pw.handlers[c.Task.Name]
			if !exists {
				return fmt.Errorf("no handler registered: %s", c.Task.Name)
			}
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

	pw.sem = make(chan struct{}, pw.execPoolSize)

	// Multi-queue priority mode components initialization
	for _, q := range pw.queues {
		qCron := newCronManager(rdb, logger, q.Name, pw.codec, opts.CronHealingInterval, opts.CronHealingLockTTL, opts.CronHealingScanBatchSize, opts.CronHealingScanMaxCount)
		qSched := newDelayedScheduler(rdb, logger, q.Name, qCron, pw.codec, opts.SchedulerPollInterval)
		qJan := newPELRecoveryJanitor(rdb, logger, q.Name, pw.group, pw.consumer, pw.concurrency, opts.JanitorInterval, opts.JanitorMinIdleTime, nil)

		pw.cronManagers[q.Name] = qCron
		pw.schedulers[q.Name] = qSched
		pw.janitors[q.Name] = qJan
	}

	// Register processors for all janitors
	for qName, jan := range pw.janitors {
		if j, ok := jan.(PELRecoveryJanitor); ok {
			name := qName
			j.RegisterProcessor(func(ctx context.Context, msg redis.XMessage) {
				pw.processMessage(ctx, StreamKey(name), msg)
			})
		}
	}

	return pw
}

func (pw *priorityWorker) Register(taskName string, handler HandlerFunc) {
	pw.handlers[taskName] = handler
}

func (pw *priorityWorker) Start(ctx context.Context) error {
	for _, q := range pw.queues {
		streamKey := StreamKey(q.Name)
		err := pw.rdb.XGroupCreateMkStream(ctx, streamKey, pw.group, "$").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("failed to create consumer group for queue %s: %w", q.Name, err)
		}
	}

	pw.logger.Info("Starting TaskMQ priority worker",
		zap.String("group", pw.group),
		zap.Int("concurrency", pw.concurrency),
		zap.String("strategy", pw.priorityStrategy),
	)

	pw.ctx, pw.cancel = context.WithCancel(pw.parentCtx)
	pw.consumerCtx, pw.consumerCancel = context.WithCancel(pw.ctx)

	for i := 0; i < pw.concurrency; i++ {
		pw.wg.Add(1)
		go pw.worker()
	}

	for qName, sched := range pw.schedulers {
		pw.wg.Add(1)
		go pw.runBackgroundLoop(sched, "scheduler-"+qName)
	}
	for qName, jan := range pw.janitors {
		pw.wg.Add(1)
		go pw.runBackgroundLoop(jan, "janitor-"+qName)
	}
	for qName, cron := range pw.cronManagers {
		pw.wg.Add(1)
		go pw.runBackgroundLoop(cron, "cron-"+qName)
	}

	return nil
}

func (pw *priorityWorker) runBackgroundLoop(runner Runner, name string) {
	defer pw.wg.Done()
	if err := runner.Run(pw.ctx); err != nil {
		pw.logger.Error("Background priority runner stopped with error", zap.String("runner", name), zap.Error(err))
	}
}

func (pw *priorityWorker) Stop(ctxs ...context.Context) {
	pw.logger.Info("Stopping TaskMQ priority worker background loops...")

	if pw.consumerCancel != nil {
		pw.consumerCancel()
	}
	if pw.cancel != nil {
		pw.cancel()
	}

	done := make(chan struct{})
	go func() {
		pw.wg.Wait()
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
		pw.logger.Info("Priority worker gracefully stopped.")
	case <-waitCtx.Done():
		pw.logger.Warn("Priority worker shutdown timeout exceeded, forcing stop.")
	}
}

func (pw *priorityWorker) worker() {
	defer pw.wg.Done()
	consumerName := fmt.Sprintf("%s-%d", pw.consumer, rand.Intn(10000))

	for {
		select {
		case <-pw.consumerCtx.Done():
			return
		default:
			// Fetch the list of queue names according to priority strategy
			var queueNames []string
			if pw.priorityStrategy == "strict" {
				queueNames = sortQueues(pw.queues)
			} else {
				queueNames = shuffleQueues(pw.queues)
			}

			messageFetched := false

			for _, qName := range queueNames {
				streamKey := StreamKey(qName)

				streams, err := pw.rdb.XReadGroup(pw.consumerCtx, &redis.XReadGroupArgs{
					Group:    pw.group,
					Consumer: consumerName,
					Streams:  []string{streamKey, ">"},
					Count:    1,
					Block:    -1,
				}).Result()

				if err != nil {
					if err == redis.Nil {
						continue
					}
					select {
					case <-pw.consumerCtx.Done():
						return
					default:
						pw.logger.Error("Failed to read group messages in priority worker", zap.Error(err))
						time.Sleep(100 * time.Millisecond)
						continue
					}
				}

				for _, stream := range streams {
					for _, msg := range stream.Messages {
						messageFetched = true
						if pw.syncExecution {
							pw.processMessage(pw.consumerCtx, streamKey, msg)
						} else {
							select {
							case pw.sem <- struct{}{}:
							case <-pw.consumerCtx.Done():
								return
							}

							pw.wg.Add(1)
							go func(sk string, m redis.XMessage) {
								defer func() {
									<-pw.sem
									pw.wg.Done()
								}()
								pw.processMessage(pw.consumerCtx, sk, m)
							}(streamKey, msg)
						}
					}
				}

				if messageFetched {
					break
				}
			}

			if !messageFetched {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

func (pw *priorityWorker) processMessage(ctx context.Context, streamKey string, msg redis.XMessage) {
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
					pw.logger.Error("Message payload or task must be bytes or string")
					return
				}
			}
		}
	}

	var task Task
	err := pw.codec.Unmarshal(payload, &task)
	if err != nil {
		pw.logger.Error("Failed to deserialize task", zap.Error(err))
		return
	}

	_, exists := pw.handlers[task.Name]
	if !exists {
		pw.logger.Error("No handler registered", zap.String("task_name", task.Name))
		return
	}

	c := AcquireConsumeContext(ctx, &task, msg.ID, task.Queue, pw.group, pw.middlewareChain)
	defer ReleaseConsumeContext(c)

	execErr := c.Next()

	// If no error occurred during processing chain and it completed fully (not aborted)
	if execErr == nil && !c.IsAborted() {
		err = pw.rdb.XAck(ctx, streamKey, pw.group, msg.ID).Err()
		if err != nil {
			pw.logger.Error("Failed to ACK task stream message",
				zap.String("task_id", task.ID),
				zap.String("stream_id", msg.ID),
				zap.Error(err),
			)
			return
		}

		if task.UniqueKey != "" {
			_ = pw.broker.ReleaseUniqueLock(ctx, &task)
		}
	}
}

func (pw *priorityWorker) getQueueRateLimit(qName string) (int64, time.Duration, string) {
	for _, q := range pw.queues {
		if q.Name == qName {
			return q.RateLimitMax, q.RateLimitDuration, q.RateLimitKeyField
		}
	}
	return 0, 0, ""
}
