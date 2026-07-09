package worker

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/runner"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

type HandlerFunc func(ctx context.Context, task *taskmodel.Task) error

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
	*baseWorker
	queue            string
	scheduler        runner.Runner
	janitor          runner.Runner
	retentionJanitor runner.Runner
	cronManager      runner.CronManager
}

func NewWorkerPool(rdb *redis.Client, logger *zap.Logger, queue string, opts ...WorkerPoolOption) Worker {
	opt, err := applyWorkerPoolOptions(codec.JSONCodec{}, opts)
	if err != nil {
		panic(fmt.Errorf("invalid option: %w", err))
	}

	buildDefaultPoolComponents(rdb, logger, queue, &opt)

	base := &baseWorker{}
	base.initBase(rdb, logger, &opt.WorkerConfig)

	// Set pool-specific settings on baseWorker
	base.rateLimitMax = opt.rateLimitMax
	base.rateLimitDuration = opt.rateLimitDuration
	base.rateLimitKeyField = opt.rateLimitKeyField

	pool := &workerPool{
		baseWorker:       base,
		queue:            queue,
		cronManager:      opt.cron.manager,
		scheduler:        opt.scheduler.scheduler,
		janitor:          opt.janitor.janitor,
		retentionJanitor: opt.retentionJanitor,
	}

	pool.getQueueRateLimit = func(qName string) (int64, time.Duration, string) {
		if qName == pool.queue {
			return pool.rateLimitMax, pool.rateLimitDuration, pool.rateLimitKeyField
		}
		return 0, 0, ""
	}

	pool.buildMiddlewareChain()

	// Register processors for janitor
	if j, ok := pool.janitor.(runner.PELRecoveryJanitor); ok {
		j.RegisterProcessor(func(ctx context.Context, msg redis.XMessage) {
			pool.ProcessMessage(ctx, msg)
		})
	}

	return pool
}

// Start starts the worker pool consumers, scheduler, and janitor loops
func (w *workerPool) Start(ctx context.Context) error {
	streamKey := keys.KeysFor(w.queue).Stream()
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

	if w.retentionJanitor != nil {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			if err := w.retentionJanitor.Run(w.ctx); err != nil {
				w.logger.Error("Retention janitor loop stopped with error", zap.Error(err))
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

	// Start Cancellation Subscriber loop
	w.startCancelSubscriber(w.ctx, &w.wg, []string{w.queue})

	// Start Queue Control (Pause/Resume) Subscriber loop
	w.startControlSubscriber(w.ctx, &w.wg, []string{w.queue})

	// Start concurrent workers to consume queue
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.runBackgroundLoop(i)
	}

	w.logger.Info("Worker pool started successfully", zap.String("queue", w.queue), zap.Int("concurrency", w.concurrency))
	return nil
}

func (w *workerPool) runBackgroundLoop(workerID int) {
	defer w.wg.Done()
	consumerName := fmt.Sprintf("%s-%d", w.consumer, workerID)
	streamKey := keys.KeysFor(w.queue).Stream()

	w.logger.Debug("Worker background loop started", zap.String("consumer", consumerName))

	for {
		select {
		case <-w.consumerCtx.Done():
			w.logger.Debug("Worker consumer context cancelled, exiting loop", zap.String("consumer", consumerName))
			return
		default:
			// Check if queue is paused before fetching
			if w.isQueuePaused(w.queue) {
				w.logger.Debug("Queue is paused, consumer waiting for resume signal", zap.String("queue", w.queue), zap.String("consumer", consumerName))
				pauseCh := w.getOrInitPauseChan(w.queue)
				select {
				case <-w.consumerCtx.Done():
					return
				case <-pauseCh:
					w.logger.Debug("Queue resumed, worker waking up", zap.String("queue", w.queue), zap.String("consumer", consumerName))
				}
				continue
			}

			// Read messages from the stream
			streams, err := w.rdb.XReadGroup(w.consumerCtx, &redis.XReadGroupArgs{
				Group:    w.group,
				Consumer: consumerName,
				Streams:  []string{streamKey, ">"},
				Count:    1,
				Block:    1 * time.Second,
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
					// Re-check pause state: XReadGroup may have returned immediately
					// due to a new message arriving while the queue was being paused.
					// Wait for resume here so the message is not left stuck in the PEL.
					if w.isQueuePaused(w.queue) {
						w.logger.Debug("Queue was paused while XReadGroup was blocking; waiting for resume before processing",
							zap.String("queue", w.queue),
							zap.String("msg_id", msg.ID),
						)
						pauseCh := w.getOrInitPauseChan(w.queue)
						select {
						case <-w.consumerCtx.Done():
							return
						case <-pauseCh:
							w.logger.Debug("Queue resumed, processing buffered message", zap.String("queue", w.queue))
						}
					}

					if w.syncExecution {
						w.ProcessMessage(w.consumerCtx, msg)
					} else {
						// Wait for pool execution token
						if err := w.execPool.Acquire(w.consumerCtx); err != nil {
							return
						}

						w.wg.Add(1)
						go func(m redis.XMessage) {
							defer func() {
								w.execPool.Release()
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
	w.processMessage(ctx, keys.KeysFor(w.queue).Stream(), msg)
}

func RateLimitKey(queue string, groupKey string) string {
	if groupKey != "" {
		return fmt.Sprintf("taskmq:{%s}:rate_limit:%s", queue, groupKey)
	}
	return fmt.Sprintf("taskmq:{%s}:rate_limit", queue)
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
