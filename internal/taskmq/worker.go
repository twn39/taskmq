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

type WorkerPool struct {
	rdb         *redis.Client
	logger      *zap.Logger
	queue       string
	group       string
	consumer    string
	concurrency int
	handlers    map[string]HandlerFunc
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

type WorkerOptions struct {
	Group       string
	Consumer    string
	Concurrency int
}

func NewWorkerPool(rdb *redis.Client, logger *zap.Logger, queue string, opts ...WorkerOptions) *WorkerPool {
	pool := &WorkerPool{
		rdb:         rdb,
		logger:      logger,
		queue:       queue,
		group:       "taskmq-group",
		consumer:    "taskmq-consumer-1",
		concurrency: 5,
		handlers:    make(map[string]HandlerFunc),
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
		}
	}

	return pool
}

// Register registers a handler function for a specific task name
func (w *WorkerPool) Register(taskName string, handler HandlerFunc) {
	w.handlers[taskName] = handler
}

// Start starts the worker pool consumers
func (w *WorkerPool) Start(ctx context.Context) error {
	streamKey := fmt.Sprintf("taskmq:queue:%s", w.queue)

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

	// Create a long-running context for the worker loop, independent of the short startup ctx
	w.ctx, w.cancel = context.WithCancel(context.Background())

	// Start workers
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.worker(streamKey)
	}

	return nil
}

// Stop stops the worker pool gracefully
func (w *WorkerPool) Stop() {
	w.logger.Info("Stopping TaskMQ worker pool gracefully")
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
	w.logger.Info("TaskMQ worker pool stopped")
}

func (w *WorkerPool) worker(streamKey string) {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			// Read messages from the stream using the long-running context w.ctx
			streams, err := w.rdb.XReadGroup(w.ctx, &redis.XReadGroupArgs{
				Group:    w.group,
				Consumer: w.consumer,
				Streams:  []string{streamKey, ">"},
				Count:    1,
				Block:    time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					// Timeout block, no new messages
					continue
				}
				// Silently ignore standard network I/O timeouts
				if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
					continue
				}
				if w.ctx.Err() != nil {
					return
				}
				w.logger.Error("Worker error reading stream", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					w.processMessage(w.ctx, streamKey, msg)
				}
			}
		}
	}
}

func (w *WorkerPool) processMessage(ctx context.Context, streamKey string, msg redis.XMessage) {
	taskData, ok := msg.Values["task"].(string)
	if !ok {
		w.logger.Error("Invalid message payload format, missing 'task' field")
		// Acknowledge corrupt messages so they don't block the queue
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	task, err := DeserializeTask(taskData)
	if err != nil {
		w.logger.Error("Failed to deserialize task", zap.Error(err))
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	handler, exists := w.handlers[task.Name]
	if !exists {
		w.logger.Warn("No handler registered for task", zap.String("task_name", task.Name))
		// Acknowledge unknown tasks so they don't block the queue
		_ = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
		return
	}

	// Execute handler
	err = handler(ctx, task)
	if err != nil {
		w.logger.Error("Task handler failed",
			zap.String("task_id", task.ID),
			zap.String("task_name", task.Name),
			zap.Error(err),
		)
		// In Stage 1 (MVP), we don't acknowledge failed tasks so they stay in PEL.
		// Future stages will implement retry and janitor reclamation logic.
		return
	}

	// Acknowledge successfully processed task
	err = w.rdb.XAck(ctx, streamKey, w.group, msg.ID).Err()
	if err != nil {
		w.logger.Error("Failed to acknowledge message ID",
			zap.String("message_id", msg.ID),
			zap.Error(err),
		)
	}
}
