package taskmq

import (
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
}

func NewPriorityWorker(rdb *redis.Client, logger *zap.Logger, opts WorkerOptions) Worker {
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

	return nil
}

func (pw *priorityWorker) Stop(ctxs ...context.Context) {
	pw.logger.Info("Stopping TaskMQ priority worker gracefully")
	if pw.consumerCancel != nil {
		pw.consumerCancel()
	}

	var ctx context.Context
	if len(ctxs) > 0 {
		ctx = ctxs[0]
	} else {
		ctx = context.Background()
	}

	done := make(chan struct{})
	go func() {
		pw.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		pw.logger.Warn("Graceful shutdown timeout, cancelling remaining context and forcing exit")
		if pw.cancel != nil {
			pw.cancel()
		}
		<-done
	case <-done:
		pw.logger.Info("All tasks finished, priority worker stopped")
	}
}

func (pw *priorityWorker) runBackgroundLoop(runner Runner, name string) {
	defer pw.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			pw.logger.Error("Panic recovered in priority background loop",
				zap.String("loop_name", name),
				zap.Any("panic", r),
			)
		}
	}()

	err := runner.Run(pw.ctx)
	if err != nil && err != context.Canceled {
		pw.logger.Error("Priority background loop failed",
			zap.String("loop_name", name),
			zap.Error(err),
		)
	}
}

func (pw *priorityWorker) worker() {
	defer pw.wg.Done()

	for {
		select {
		case <-pw.consumerCtx.Done():
			return
		default:
			select {
			case <-pw.consumerCtx.Done():
				return
			case pw.sem <- struct{}{}:
			}

			var streams []redis.XStream
			var err error

			var orderedQueues []string
			if pw.priorityStrategy == "strict" {
				orderedQueues = sortQueues(pw.queues)
			} else {
				orderedQueues = shuffleQueues(pw.queues)
			}

			var nonLimitedQueues []string
			var minWait time.Duration
			for _, qName := range orderedQueues {
				rlMax, rlDuration, rlKeyField := pw.getQueueRateLimit(qName)
				if rlMax > 0 && rlDuration > 0 && rlKeyField == "" {
					limitKey := RateLimitKey(qName, "")
					wait, checkErr := pw.limiter.Check(pw.consumerCtx, limitKey, rlMax, rlDuration)
					if checkErr == nil && wait > 0 {
						pw.logger.Debug("Queue rate-limited, skipping in polling", zap.String("queue", qName), zap.Duration("wait", wait))
						if wait < minWait || minWait == 0 {
							minWait = wait
						}
						continue
					}
				}
				nonLimitedQueues = append(nonLimitedQueues, qName)
			}

			if len(nonLimitedQueues) == 0 {
				pw.releaseToken()
				if minWait == 0 {
					minWait = time.Second
				}
				select {
				case <-pw.consumerCtx.Done():
					return
				case <-time.After(minWait):
				}
				continue
			}

			found := false
			for _, qName := range nonLimitedQueues {
				sKey := StreamKey(qName)
				res, readErr := pw.rdb.XReadGroup(pw.consumerCtx, &redis.XReadGroupArgs{
					Group:    pw.group,
					Consumer: pw.consumer,
					Streams:  []string{sKey, ">"},
					Count:    1,
					Block:    -1,
				}).Result()

				if readErr == nil && len(res) > 0 && len(res[0].Messages) > 0 {
					streams = res
					found = true
					break
				}
			}

			if !found {
				streamsArg := make([]string, 2*len(nonLimitedQueues))
				for i, qName := range nonLimitedQueues {
					streamsArg[i] = StreamKey(qName)
				}
				for i := 0; i < len(nonLimitedQueues); i++ {
					streamsArg[len(nonLimitedQueues)+i] = ">"
				}

				streams, err = pw.rdb.XReadGroup(pw.consumerCtx, &redis.XReadGroupArgs{
					Group:    pw.group,
					Consumer: pw.consumer,
					Streams:  streamsArg,
					Count:    1,
					Block:    time.Second,
				}).Result()
			}

			if err != nil {
				pw.releaseToken()
				if pw.consumerCtx.Err() != nil {
					return
				}
				if err == redis.Nil {
					continue
				}
				if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
					continue
				}
				pw.logger.Error("Priority worker error reading stream", zap.Error(err))
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
						case <-pw.consumerCtx.Done():
							return
						case pw.sem <- struct{}{}:
						}
					}
					firstMsg = false

					if pw.syncExecution {
						pw.processMessage(pw.ctx, stream.Stream, msg)
						pw.releaseToken()
					} else {
						pw.wg.Add(1)
						go func(s string, m redis.XMessage) {
							defer pw.wg.Done()
							defer pw.releaseToken()
							pw.processMessage(pw.ctx, s, m)
						}(stream.Stream, msg)
					}
				}
			}

			if !msgPulled {
				pw.releaseToken()
			}
		}
	}
}

func (pw *priorityWorker) releaseToken() {
	select {
	case <-pw.sem:
	default:
	}
}

func (pw *priorityWorker) processMessage(ctx context.Context, streamKey string, msg redis.XMessage) {
	taskData, ok := msg.Values["task"].(string)
	if !ok {
		pw.logger.Error("Invalid message payload format, missing 'task' field")
		_ = pw.rdb.XAck(ctx, streamKey, pw.group, msg.ID).Err()
		return
	}

	task := &Task{}
	if err := pw.codec.Unmarshal([]byte(taskData), task); err != nil {
		pw.logger.Error("Failed to deserialize task", zap.Error(err))
		_ = pw.rdb.XAck(ctx, streamKey, pw.group, msg.ID).Err()
		return
	}

	handler, exists := pw.handlers[task.Name]
	if !exists {
		pw.logger.Warn("No handler registered for task, leaving in PEL for potential future handler", zap.String("task_name", task.Name))
		return
	}

	rlMax, rlDuration, rlKeyField := pw.getQueueRateLimit(task.Queue)
	if rlMax > 0 && rlDuration > 0 {
		var groupKeyVal string
		if rlKeyField != "" {
			groupKeyVal = extractGroupKey(task.Payload, rlKeyField)
		}
		limitKey := RateLimitKey(task.Queue, groupKeyVal)

		wait, rlErr := pw.limiter.Check(ctx, limitKey, rlMax, rlDuration)
		if rlErr != nil {
			pw.logger.Error("Failed to apply rate limiting", zap.Error(rlErr))
		} else if wait > 0 {
			pw.logger.Debug("Rate limit exceeded, deferring task", zap.String("queue", task.Queue), zap.String("group_key", groupKeyVal), zap.Duration("wait", wait))
			if err := pw.deferRateLimitedTask(ctx, streamKey, msg.ID, task, wait); err != nil {
				pw.logger.Error("Failed to defer rate limited task atomically",
					zap.String("task_id", task.ID),
					zap.String("task_name", task.Name),
					zap.Error(err),
				)
			}
			return
		}
	}

	pw.logger.Info("Priority executing task",
		zap.String("task_id", task.ID),
		zap.String("task_name", task.Name),
		zap.Int("retry", task.Retry),
	)

	err := pw.runHandlerWithRecovery(ctx, task, handler)
	if err != nil {
		pw.logger.Error("Task execution failed",
			zap.String("task_id", task.ID),
			zap.String("task_name", task.Name),
			zap.Error(err),
		)
		pw.handleFailure(ctx, streamKey, msg, task, err)
		return
	}

	// ACK the message in the stream to remove it from PEL
	err = pw.rdb.XAck(ctx, streamKey, pw.group, msg.ID).Err()
	if err != nil {
		pw.logger.Error("Failed to ACK task stream message",
			zap.String("task_id", task.ID),
			zap.String("stream_id", msg.ID),
			zap.Error(err),
		)
		return
	}

	// Uniqueness locks release (if lock is active)
	if task.UniqueKey != "" {
		pw.releaseUniqueLock(ctx, task)
	}
}

func (pw *priorityWorker) runHandlerWithRecovery(ctx context.Context, task *Task, handler HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("task handler panicked: %v", r)
			pw.logger.Error("Panic recovered in priority task handler execution",
				zap.String("task_id", task.ID),
				zap.String("task_name", task.Name),
				zap.Any("panic", r),
			)
		}
	}()
	return handler(ctx, task)
}

func (pw *priorityWorker) handleFailure(ctx context.Context, streamKey string, msg redis.XMessage, task *Task, err error) {
	task.Retry++

	if task.Retry >= task.MaxRetry {
		task.LastError = err.Error()
		pw.logger.Error("Task exhausted all retries, moving to DLQ",
			zap.String("task_id", task.ID),
			zap.String("task_name", task.Name),
			zap.Int("retry", task.Retry),
			zap.String("error", task.LastError),
		)

		serialized, _ := pw.codec.Marshal(task)
		dlqKey := DLQKey(task.Queue)
		nowMs := time.Now().UnixMilli()

		var uniqueLockKey string
		var uniqueLockVal string
		if task.UniqueKey != "" {
			uniqueLockKey = UniqueKey(task.Queue, task.UniqueKey)
			uniqueLockVal = task.ID
		}

		_, zerr := pw.rdb.Eval(ctx, luaHandleFailure, []string{dlqKey, streamKey, uniqueLockKey}, "dlq", msg.ID, pw.group, nowMs, string(serialized), uniqueLockVal).Result()
		if zerr != nil {
			pw.logger.Error("Failed to move task to DLQ atomically", zap.Error(zerr))
		}
		return
	}

	backoff := time.Millisecond * time.Duration(100*(1<<uint(task.Retry)))

	pw.logger.Info("Scheduling task retry with exponential backoff",
		zap.String("task_id", task.ID),
		zap.String("task_name", task.Name),
		zap.Int("retry", task.Retry),
		zap.Duration("backoff", backoff),
	)

	serialized, err := pw.codec.Marshal(task)
	if err != nil {
		pw.logger.Error("Failed to serialize retry task", zap.Error(err))
		return
	}

	delayedKey := DelayedKey(task.Queue)
	at := time.Now().Add(backoff)

	_, zerr := pw.rdb.Eval(ctx, luaHandleFailure, []string{delayedKey, streamKey, ""}, "retry", msg.ID, pw.group, at.UnixMilli(), string(serialized), "").Result()
	if zerr != nil {
		pw.logger.Error("Failed to schedule task retry atomically", zap.Error(zerr))
	}
}

func (pw *priorityWorker) deferRateLimitedTask(ctx context.Context, streamKey string, msgID string, task *Task, delay time.Duration) error {
	serialized, err := pw.codec.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to serialize rate-limited task: %w", err)
	}
	delayedKey := DelayedKey(task.Queue)
	at := time.Now().Add(delay)

	_, err = pw.rdb.Eval(ctx, luaDeferRateLimitedTask, []string{delayedKey, streamKey}, pw.group, msgID, at.UnixMilli(), string(serialized)).Result()
	return err
}

func (pw *priorityWorker) releaseUniqueLock(ctx context.Context, task *Task) {
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
	err := pw.rdb.Eval(ctx, luaUnlock, []string{uniqueKey}, task.ID).Err()
	if err != nil {
		pw.logger.Error("Failed to release task uniqueness lock",
			zap.String("task_id", task.ID),
			zap.String("unique_key", task.UniqueKey),
			zap.Error(err),
		)
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
