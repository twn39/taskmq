package taskmq

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type cronManager struct {
	rdb             *redis.Client
	logger          *zap.Logger
	queue           string
	codec           Codec
	healingInterval time.Duration
	healingLockTTL  time.Duration
}

func newCronManager(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, healingInterval time.Duration, healingLockTTL time.Duration) CronManager {
	return &cronManager{
		rdb:             rdb,
		logger:          logger,
		queue:           queue,
		codec:           codec,
		healingInterval: healingInterval,
		healingLockTTL:  healingLockTTL,
	}
}

// Run launches the self-healing background routine
func (m *cronManager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.healingInterval)
	defer ticker.Stop()

	configsKey := CronConfigsKey(m.queue)
	delayedKey := DelayedKey(m.queue)
	lockKey := fmt.Sprintf("taskmq:{%s}:cron_self_healing_lock", m.queue)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Acquire distributed lock to avoid concurrent self-healing scans in clustered deployment
			ok, err := m.rdb.SetNX(ctx, lockKey, "1", m.healingLockTTL).Result()
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

			// Determine the upper bound score for ZSET query and parse configs
			maxNextTime := time.Now()
			parsedConfigs := make(map[string]*Task)
			for jobName, configStr := range configs {
				task := &Task{}
				if err := m.codec.Unmarshal([]byte(configStr), task); err != nil {
					continue
				}
				parsedConfigs[jobName] = task

				sched, err := CronParser.Parse(task.CronSpec)
				if err != nil {
					continue
				}
				nextTime := sched.Next(time.Now())
				if nextTime.After(maxNextTime) {
					maxNextTime = nextTime
				}
			}

			// Fetch only tasks up to the max next execution time to optimize scanning
			delayedMembers, err := m.rdb.ZRangeByScore(ctx, delayedKey, &redis.ZRangeBy{
				Min: "0",
				Max: fmt.Sprintf("%d", maxNextTime.UnixMilli()),
			}).Result()
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
			for jobName, task := range parsedConfigs {
				if activeCrons[jobName] {
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
