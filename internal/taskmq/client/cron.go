package client

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// cronParser matches CronParser options used by the worker cron manager.
var cronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

//go:embed scripts/register_cron.lua
var registerCronScript string

var registerCronCmd = redis.NewScript(registerCronScript)

// cronService implements CronClient.
type cronService struct {
	d deps
}

func (s *cronService) RegisterCron(ctx context.Context, jobName string, spec string, task *taskmodel.Task, opts ...TaskOption) error {
	for _, opt := range opts {
		if err := opt(task); err != nil {
			return err
		}
	}

	sched, err := cronParser.Parse(spec)
	if err != nil {
		return fmt.Errorf("taskmq: invalid cron spec: %w", err)
	}

	task.Name = jobName
	task.CronSpec = spec
	if task.Queue == "" {
		task.Queue = "default"
	}
	if task.ID == "" {
		task.ID = generateUUID()
	}

	serialized, err := s.d.codec.Marshal(task)
	if err != nil {
		return err
	}

	qk := keys.KeysFor(task.Queue)
	configsKey := qk.CronConfigs()
	delayedKey := qk.Delayed()
	firstRun := sched.Next(time.Now())
	maxCount, overflow := int64(0), 0
	if s.d.lifecycle != nil {
		maxCount, overflow = s.d.lifecycle.DelayedLimits()
	}

	res, err := registerCronCmd.Run(ctx, s.d.rdb, []string{configsKey, delayedKey},
		jobName, spec, serialized, firstRun.UnixMilli(), maxCount, overflow).Result()
	if err != nil {
		return err
	}
	if err := lifecycle.MapEnqueueScriptResult(res, s.d.metrics(), true); err != nil {
		return err
	}
	_ = s.d.rdb.Publish(ctx, qk.DelayedWakeupChannel(), strconv.FormatInt(firstRun.UnixMilli(), 10)).Err()
	return nil
}

func (s *cronService) ListCronJobs(ctx context.Context, queue string) ([]*CronJob, error) {
	configsKey := keys.KeysFor(queue).CronConfigs()
	configs, err := s.d.rdb.HGetAll(ctx, configsKey).Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]*CronJob, 0, len(configs))
	for jobName, configStr := range configs {
		task := &taskmodel.Task{}
		err := s.d.codec.Unmarshal([]byte(configStr), task)
		if err != nil {
			continue
		}

		nextRun := time.Time{}
		if task.CronSpec != "" {
			if sched, err := cronParser.Parse(task.CronSpec); err == nil {
				nextRun = sched.Next(time.Now())
			}
		}

		jobs = append(jobs, &CronJob{
			JobName:     jobName,
			Task:        task,
			NextRunTime: nextRun,
		})
	}
	return jobs, nil
}

func (s *cronService) RunCronJob(ctx context.Context, queue string, jobName string) error {
	qk := keys.KeysFor(queue)
	configsKey := qk.CronConfigs()
	configStr, err := s.d.rdb.HGet(ctx, configsKey, jobName).Result()
	if err == redis.Nil {
		return fmt.Errorf("taskmq: cron job not found: %s", jobName)
	} else if err != nil {
		return err
	}

	// Honor EnqueueHardLimit + StreamMaxLen (same admission as client Enqueue).
	return s.d.lifecycle.XAddTask(ctx, s.d.rdb, qk.Stream(), []byte(configStr))
}

func (s *cronService) DeleteCronJob(ctx context.Context, queue string, jobName string) error {
	qk := keys.KeysFor(queue)
	configsKey := qk.CronConfigs()
	delayedKey := qk.Delayed()

	serialized, err := s.d.rdb.HGet(ctx, configsKey, jobName).Result()
	if err == redis.Nil {
		return fmt.Errorf("taskmq: cron job not found: %s", jobName)
	} else if err != nil {
		return err
	}

	_, _ = s.d.rdb.ZRem(ctx, delayedKey, serialized).Result()
	_, err = s.d.rdb.HDel(ctx, configsKey, jobName).Result()
	return err
}
