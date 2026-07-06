package taskmq

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

var CronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

type cronManager struct {
	rdb             *redis.Client
	logger          *zap.Logger
	queue           string
	codec           Codec
	healingInterval time.Duration
	healingLockTTL  time.Duration
	scanBatchSize   int
	scanMaxCount    int
}

func newCronManager(
	rdb *redis.Client,
	logger *zap.Logger,
	queue string,
	codec Codec,
	healingInterval time.Duration,
	healingLockTTL time.Duration,
	scanBatchSize int,
	scanMaxCount int,
) CronManager {
	return &cronManager{
		rdb:             rdb,
		logger:          logger,
		queue:           queue,
		codec:           codec,
		healingInterval: healingInterval,
		healingLockTTL:  healingLockTTL,
		scanBatchSize:   scanBatchSize,
		scanMaxCount:    scanMaxCount,
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
				if err := m.codec.Unmarshal(unsafeStringToBytes(configStr), task); err != nil {
					m.logger.Error("Cron Self-Healing: failed to deserialize cron config", zap.String("job_name", jobName), zap.String("config", configStr), zap.Error(err))
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

			// Paginate ZSET query to prevent memory bloat
			offset := int64(0)
			limit := int64(100)
			if m.scanBatchSize > 0 {
				limit = int64(m.scanBatchSize)
			}
			maxScan := int64(1000)
			if m.scanMaxCount > 0 {
				maxScan = int64(m.scanMaxCount)
			}

			activeCrons := make(map[string]bool)
			scannedCount := int64(0)

			for scannedCount < maxScan {
				delayedMembers, err := m.rdb.ZRangeByScore(ctx, delayedKey, &redis.ZRangeBy{
					Min:    "0",
					Max:    fmt.Sprintf("%d", maxNextTime.UnixMilli()),
					Offset: offset,
					Count:  limit,
				}).Result()
				if err != nil {
					m.logger.Error("Cron Self-Healing: failed to get delayed ZSET chunk", zap.Error(err))
					break
				}

				if len(delayedMembers) == 0 {
					break
				}

				for _, member := range delayedMembers {
					task := &Task{}
					err := m.codec.Unmarshal(unsafeStringToBytes(member), task)
					if err != nil {
						m.logger.Error("Cron Self-Healing: failed to deserialize active cron task from delayed ZSET", zap.String("raw_member", member), zap.Error(err))
						continue
					}
					if task.CronSpec != "" {
						activeCrons[task.Name] = true
					}
				}

				scannedCount += int64(len(delayedMembers))
				offset += limit

				// Optimize: If all registered cron jobs are already active, break early.
				allFound := true
				for jobName := range parsedConfigs {
					if !activeCrons[jobName] {
						allFound = false
						break
					}
				}
				if allFound {
					break
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
					MaxRetry: Ptr(task.MaxRetry),
				})
				nextTask.CronSpec = task.CronSpec
				nextTask.ID = fmt.Sprintf("cron:%s:%d", jobName, nextTime.UnixMilli())

				serialized, err := m.codec.Marshal(nextTask)
				if err != nil {
					continue
				}

				errZAdd := m.rdb.ZAdd(ctx, delayedKey, redis.Z{
					Score:  float64(nextTime.UnixMilli()),
					Member: string(serialized),
				}).Err()
				if errZAdd == nil {
					_ = m.rdb.Publish(ctx, KeysFor(task.Queue).DelayedWakeupChannel(), fmt.Sprintf("%d", nextTime.UnixMilli())).Err()
				}
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
		MaxRetry: Ptr(task.MaxRetry),
	})
	nextTask.CronSpec = task.CronSpec
	nextTask.ID = fmt.Sprintf("cron:%s:%d", task.Name, nextTime.UnixMilli())

	serialized, err := m.codec.Marshal(nextTask)
	if err != nil {
		m.logger.Error("Cron: failed to serialize next task", zap.Error(err))
		return err
	}

	err = m.rdb.ZAdd(ctx, KeysFor(task.Queue).Delayed(), redis.Z{
		Score:  float64(nextTime.UnixMilli()),
		Member: string(serialized),
	}).Err()

	if err != nil {
		m.logger.Error("Cron: failed to ZADD next run to ZSET", zap.Error(err))
		return err
	}

	_ = m.rdb.Publish(ctx, KeysFor(task.Queue).DelayedWakeupChannel(), fmt.Sprintf("%d", nextTime.UnixMilli())).Err()

	m.logger.Debug("Cron: scheduled next run", zap.String("job_name", task.Name), zap.Time("next_run", nextTime))
	return nil
}
