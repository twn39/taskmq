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
	*baseWorker
	queues           []QueuePriority
	priorityStrategy string
	schedulers       map[string]Runner
	janitors         map[string]Runner
	cronManagers     map[string]CronManager
}

func NewPriorityWorker(rdb *redis.Client, logger *zap.Logger, opts ...PriorityWorkerOption) Worker {
	opt := defaultPriorityWorkerOptions(JSONCodec{})
	for _, o := range opts {
		if err := o.ApplyPriorityWorker(&opt); err != nil {
			panic(fmt.Errorf("invalid option: %w", err))
		}
	}

	buildSharedPriorityDefaults(rdb, &opt)

	base := &baseWorker{}
	base.initBase(rdb, logger, &opt.BaseWorkerOptions)

	pw := &priorityWorker{
		baseWorker:       base,
		queues:           opt.priorityQueues,
		priorityStrategy: opt.priorityStrategy,
		schedulers:       make(map[string]Runner),
		janitors:         make(map[string]Runner),
		cronManagers:     make(map[string]CronManager),
	}

	if pw.priorityStrategy == "" {
		pw.priorityStrategy = "weighted"
	}

	pw.getQueueRateLimit = func(qName string) (int64, time.Duration, string) {
		for _, q := range pw.queues {
			if q.Name == qName {
				return q.RateLimitMax, q.RateLimitDuration, q.RateLimitKeyField
			}
		}
		return 0, 0, ""
	}

	pw.buildMiddlewareChain()

	// Multi-queue priority mode components initialization
	for _, q := range pw.queues {
		var qCron CronManager
		if opt.cron.factory != nil {
			qCron = opt.cron.factory(rdb, logger, q.Name, pw.codec, opt.cron.healingInterval, opt.cron.lockTTL, opt.cron.scanBatchSize, opt.cron.scanMaxCount)
		} else {
			qCron = newCronManager(rdb, logger, q.Name, pw.codec, opt.cron.healingInterval, opt.cron.lockTTL, opt.cron.scanBatchSize, opt.cron.scanMaxCount)
		}

		var qSched Runner
		if opt.scheduler.factory != nil {
			qSched = opt.scheduler.factory(rdb, logger, q.Name, qCron, pw.codec, opt.scheduler.pollInterval)
		} else {
			qSched = newDelayedScheduler(rdb, logger, q.Name, qCron, pw.codec, opt.scheduler.pollInterval)
		}

		var qJan PELRecoveryJanitor
		if opt.janitor.factory != nil {
			qJan = opt.janitor.factory(rdb, logger, q.Name, pw.group, pw.consumer, pw.concurrency, opt.janitor.interval, opt.janitor.minIdleTime)
		} else {
			qJan = newPELRecoveryJanitor(rdb, logger, q.Name, pw.group, pw.consumer, pw.concurrency, opt.janitor.interval, opt.janitor.minIdleTime, nil)
		}

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

	// Start Cancellation Subscriber loop
	queues := make([]string, len(pw.queues))
	for i, q := range pw.queues {
		queues[i] = q.Name
	}
	pw.startCancelSubscriber(pw.ctx, &pw.wg, queues)

	// Start Queue Control (Pause/Resume) Subscriber loop
	pw.startControlSubscriber(pw.ctx, &pw.wg, queues)

	for i := 0; i < pw.concurrency; i++ {
		pw.wg.Add(1)
		go pw.worker(i)
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

func (pw *priorityWorker) worker(workerIndex int) {
	defer pw.wg.Done()
	consumerName := fmt.Sprintf("%s-%d", pw.consumer, workerIndex)

	for {
		select {
		case <-pw.consumerCtx.Done():
			return
		default:
			// 1. Get ordered queue list according to priority strategy
			var queueNames []string
			if pw.priorityStrategy == "strict" {
				queueNames = sortQueues(pw.queues)
			} else {
				queueNames = shuffleQueues(pw.queues)
			}

			messageFetched := false
			allQueuesPaused := true

			// 2. Sequentially poll each queue with an extremely short block timeout (10ms)
			// This preserves exact FIFO order among streams and completely avoids starvation
			for _, qName := range queueNames {
				if pw.isQueuePaused(qName) {
					continue
				}
				allQueuesPaused = false
				streamKey := StreamKey(qName)

				streams, err := pw.rdb.XReadGroup(pw.consumerCtx, &redis.XReadGroupArgs{
					Group:    pw.group,
					Consumer: consumerName,
					Streams:  []string{streamKey, ">"},
					Count:    1,
					Block:    10 * time.Millisecond,
				}).Result()

				if err != nil {
					if err == redis.Nil {
						// Stream is empty, proceed instantly to check next queue
						continue
					}
					select {
					case <-pw.consumerCtx.Done():
						return
					default:
						pw.logger.Error("Failed to read group messages in priority worker", zap.Error(err))
						time.Sleep(50 * time.Millisecond)
						continue
					}
				}

				// 3. Process the fetched message
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

				// Break loop immediately after processing a message from a higher priority queue
				// to begin the next evaluation round (ensuring absolute higher priority dominance)
				if messageFetched {
					break
				}
			}

			// 4. Backoff to protect CPU if all queues are empty/paused
			if !messageFetched {
				if allQueuesPaused {
					time.Sleep(200 * time.Millisecond)
				} else {
					time.Sleep(50 * time.Millisecond)
				}
			}
		}
	}
}
