package taskmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type cronManager struct {
	rdb    *redis.Client
	logger *zap.Logger
	queue  string
	codec  Codec
}

func newCronManager(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec) *cronManager {
	return &cronManager{
		rdb:    rdb,
		logger: logger,
		queue:  queue,
		codec:  codec,
	}
}

// Start launches the self-healing background routine
func (m *cronManager) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	configsKey := CronConfigsKey(m.queue)
	delayedKey := DelayedKey(m.queue)
	lockKey := fmt.Sprintf("taskmq:{%s}:cron_self_healing_lock", m.queue)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Acquire distributed lock to avoid concurrent self-healing scans in clustered deployment
			ok, err := m.rdb.SetNX(ctx, lockKey, "1", 9*time.Second).Result()
			if err != nil {
				m.logger.Error("Cron Self-Healing: failed to acquire lock", zap.Error(err))
				continue
			}
			if !ok {
				continue
			}

			// Fetch all registered cron configs from Redis Hash
			configs, err := m.rdb.HGetAll(ctx, configsKey).Result()
			if err != nil {
				m.logger.Error("Cron Self-Healing: failed to get configs", zap.Error(err))
				continue
			}

			if len(configs) == 0 {
				continue
			}

			// Fetch all delayed tasks in ZSET to check what is already scheduled
			delayedMembers, err := m.rdb.ZRange(ctx, delayedKey, 0, -1).Result()
			if err != nil {
				m.logger.Error("Cron Self-Healing: failed to get delayed ZSET", zap.Error(err))
				continue
			}

			activeCrons := make(map[string]bool)
			for _, member := range delayedMembers {
				task := &Task{}
				err := m.codec.Unmarshal([]byte(member), task)
				if err == nil && task.CronSpec != "" {
					activeCrons[task.Name] = true
				}
			}

			// Scan and heal missing schedules
			for jobName, configStr := range configs {
				if activeCrons[jobName] {
					continue
				}

				task := &Task{}
				err := m.codec.Unmarshal([]byte(configStr), task)
				if err != nil {
					continue
				}

				sched, err := CronParser.Parse(task.CronSpec)
				if err != nil {
					continue
				}

				nextTime := sched.Next(time.Now())
				m.logger.Warn("Cron Self-Healing: detected broken chain, rescheduling job", zap.String("job_name", jobName), zap.Time("next_run", nextTime))

				// Generate next task run using deterministic ID to prevent duplicates
				nextTask := NewTask(task.Name, task.Payload, TaskOptions{
					Queue:    task.Queue,
					MaxRetry: task.MaxRetry,
				})
				nextTask.CronSpec = task.CronSpec
				nextTask.ID = fmt.Sprintf("cron:%s:%d", jobName, nextTime.UnixMilli())

				serialized, err := m.codec.Marshal(nextTask)
				if err != nil {
					continue
				}

				_ = m.rdb.ZAdd(ctx, delayedKey, redis.Z{
					Score:  float64(nextTime.UnixMilli()),
					Member: string(serialized),
				}).Err()
			}
		}
	}
}

// Reschedule computes the next run and enqueues it to ZSET
func (m *cronManager) Reschedule(ctx context.Context, task *Task) error {
	if task.CronSpec == "" {
		return nil
	}

	sched, err := CronParser.Parse(task.CronSpec)
	if err != nil {
		m.logger.Error("Cron: invalid spec in task", zap.String("job_name", task.Name), zap.String("spec", task.CronSpec), zap.Error(err))
		return err
	}

	nextTime := sched.Next(time.Now())

	// Create next run using a deterministic task ID
	nextTask := NewTask(task.Name, task.Payload, TaskOptions{
		Queue:    task.Queue,
		MaxRetry: task.MaxRetry,
	})
	nextTask.CronSpec = task.CronSpec
	nextTask.ID = fmt.Sprintf("cron:%s:%d", task.Name, nextTime.UnixMilli())

	serialized, err := m.codec.Marshal(nextTask)
	if err != nil {
		m.logger.Error("Cron: failed to serialize next task", zap.Error(err))
		return err
	}

	delayedKey := DelayedKey(task.Queue)
	err = m.rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(nextTime.UnixMilli()),
		Member: string(serialized),
	}).Err()

	if err != nil {
		m.logger.Error("Cron: failed to ZADD next run to ZSET", zap.Error(err))
		return err
	}

	m.logger.Debug("Cron: scheduled next run", zap.String("job_name", task.Name), zap.Time("next_run", nextTime))
	return nil
}
