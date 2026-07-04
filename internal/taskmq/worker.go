package taskmq

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
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
	*baseWorker
	queue       string
	scheduler   Runner
	janitor     Runner
	cronManager CronManager
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
	var opt WorkerOptions
	if len(opts) > 0 {
		opt = opts[0]
	} else {
		panic("NewWorkerPool: WorkerOptions must be provided")
	}

	opt.ApplyDefaults(rdb, logger, queue, JSONCodec{})

	base := &baseWorker{}
	base.initBase(rdb, logger, &opt, JSONCodec{})

	pool := &workerPool{
		baseWorker:  base,
		queue:       queue,
		cronManager: opt.CronManager,
		scheduler:   opt.Scheduler,
		janitor:     opt.Janitor,
	}

	pool.getQueueRateLimit = func(qName string) (int64, time.Duration, string) {
		if qName == pool.queue {
			return pool.rateLimitMax, pool.rateLimitDuration, pool.rateLimitKeyField
		}
		return 0, 0, ""
	}

	pool.buildMiddlewareChain()

	// Register processors for janitor
	if j, ok := pool.janitor.(PELRecoveryJanitor); ok {
		j.RegisterProcessor(func(ctx context.Context, msg redis.XMessage) {
			pool.ProcessMessage(ctx, msg)
		})
	}

	return pool
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
	w.processMessage(ctx, StreamKey(w.queue), msg)
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
