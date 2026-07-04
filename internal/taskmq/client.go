package taskmq

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrDuplicateTask is returned when a unique task cannot be enqueued because a duplicate already exists.
var ErrDuplicateTask = errors.New("taskmq: duplicate task in queue")

var enqueueUniqueCmd = redis.NewScript(`
	local lockKey = KEYS[1]
	local streamKey = KEYS[2]
	local lockVal = ARGV[1]
	local ttlMs = tonumber(ARGV[2])
	local serialized = ARGV[3]

	local currentLockVal = redis.call("GET", lockKey)
	if currentLockVal and currentLockVal ~= lockVal then
		return -1
	end
	redis.call("SET", lockKey, lockVal, "PX", ttlMs)
	redis.call("XADD", streamKey, "*", "task", serialized)
	return 1
`)

var enqueueUniqueDelayedCmd = redis.NewScript(`
	local lockKey = KEYS[1]
	local delayedKey = KEYS[2]
	local lockVal = ARGV[1]
	local ttlMs = tonumber(ARGV[2])
	local serialized = ARGV[3]
	local score = tonumber(ARGV[4])

	local currentLockVal = redis.call("GET", lockKey)
	if currentLockVal and currentLockVal ~= lockVal then
		return -1
	end
	redis.call("SET", lockKey, lockVal, "PX", ttlMs)
	redis.call("ZADD", delayedKey, score, serialized)
	return 1
`)

var registerCronCmd = redis.NewScript(`
	local configsKey = KEYS[1]
	local delayedKey = KEYS[2]
	local jobName = ARGV[1]
	local spec = ARGV[2]
	local serializedTask = ARGV[3]
	local firstRunScore = tonumber(ARGV[4])

	local existing = redis.call('HGET', configsKey, jobName)
	if existing == serializedTask then
		return 0
	end

	redis.call('HSET', configsKey, jobName, serializedTask)
	redis.call('ZADD', delayedKey, firstRunScore, serializedTask)
	return 1
`)

var deleteDeadLetterCmd = redis.NewScript(`
	local dlqKey = KEYS[1]
	local dlqIndexKey = KEYS[2]
	local taskID = ARGV[1]

	local serialized = redis.call("HGET", dlqIndexKey, taskID)
	if serialized then
		redis.call("ZREM", dlqKey, serialized)
		redis.call("HDEL", dlqIndexKey, taskID)
		return 1
	end
	return 0
`)

type ScheduledTask struct {
	*Task
	RunAt time.Time `json:"run_at"`
}

type CronJob struct {
	JobName     string    `json:"job_name"`
	*Task
	NextRunTime time.Time `json:"next_run_time"`
}

type Client interface {
	Enqueue(ctx context.Context, task *Task) error
	EnqueueIn(ctx context.Context, task *Task, delay time.Duration) error
	EnqueueAt(ctx context.Context, task *Task, at time.Time) error
	RegisterCron(ctx context.Context, jobName string, spec string, task *Task) error
	ListDeadLetters(ctx context.Context, queue string, limit int) ([]*Task, error)
	DeleteDeadLetter(ctx context.Context, queue string, taskID string) error
	RetryDeadLetter(ctx context.Context, queue string, taskID string) error
	RetryAllDeadLetters(ctx context.Context, queue string) (int64, error)
	PurgeAllDeadLetters(ctx context.Context, queue string) (int64, error)
	CancelTask(ctx context.Context, queue, taskID string) error
	Pause(ctx context.Context, queue string) error
	Resume(ctx context.Context, queue string) error
	IsPaused(ctx context.Context, queue string) (bool, error)
	ListScheduledTasks(ctx context.Context, queue string, limit int) ([]*ScheduledTask, error)
	RunScheduledTask(ctx context.Context, queue string, taskID string) error
	DeleteScheduledTask(ctx context.Context, queue string, taskID string) error
	ListCronJobs(ctx context.Context, queue string) ([]*CronJob, error)
	RunCronJob(ctx context.Context, queue string, jobName string) error
	DeleteCronJob(ctx context.Context, queue string, jobName string) error
}

type client struct {
	rdb              *redis.Client
	codec            Codec
	defaultUniqueTTL time.Duration
}

type ClientOption func(*client)

func WithClientCodec(codec Codec) ClientOption {
	return func(c *client) {
		c.codec = codec
	}
}

// WithDefaultUniqueTTL sets the default TTL for unique task locks.
func WithDefaultUniqueTTL(ttl time.Duration) ClientOption {
	return func(c *client) {
		c.defaultUniqueTTL = ttl
	}
}

func NewClient(rdb *redis.Client, opts ...ClientOption) Client {
	c := &client{
		rdb:   rdb,
		codec: JSONCodec{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// generateUUID generates a lightweight pseudo-random UUID-v4-like string
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (c *client) acquireUniqueLock(ctx context.Context, task *Task) (bool, error) {
	if task.UniqueKey == "" {
		return true, nil
	}

	if task.ID == "" {
		task.ID = generateUUID()
	}

	uniqueKey := UniqueKey(task.Queue, task.UniqueKey)
	ttl := time.Duration(task.UniqueTTLMs) * time.Millisecond
	if ttl <= 0 {
		if c.defaultUniqueTTL > 0 {
			ttl = c.defaultUniqueTTL
		} else {
			ttl = 1 * time.Hour // Default to 1 hour
		}
	}

	return c.rdb.SetNX(ctx, uniqueKey, task.ID, ttl).Result()
}

// Enqueue adds a task to the Redis stream immediately (active queue)
func (c *client) Enqueue(ctx context.Context, task *Task) error {
	if task.ID == "" {
		task.ID = generateUUID()
	}

	serialized, err := c.codec.Marshal(task)
	if err != nil {
		return err
	}

	streamKey := StreamKey(task.Queue)

	if task.UniqueKey != "" {
		ttl := time.Duration(task.UniqueTTLMs) * time.Millisecond
		if ttl <= 0 {
			if c.defaultUniqueTTL > 0 {
				ttl = c.defaultUniqueTTL
			} else {
				ttl = 1 * time.Hour // Default to 1 hour
			}
		}
		uniqueKey := UniqueKey(task.Queue, task.UniqueKey)
		res, err := enqueueUniqueCmd.Run(ctx, c.rdb, []string{uniqueKey, streamKey}, task.ID, int(ttl.Milliseconds()), serialized).Result()
		if err != nil {
			return err
		}
		if val, ok := res.(int64); ok && val == -1 {
			return ErrDuplicateTask
		}
		return nil
	}

	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"task": serialized,
		},
	}).Err()
}

// EnqueueIn adds a task to the delayed queue with a delay duration
func (c *client) EnqueueIn(ctx context.Context, task *Task, delay time.Duration) error {
	return c.EnqueueAt(ctx, task, time.Now().Add(delay))
}

// EnqueueAt adds a task to the delayed queue to be executed at a specific time
func (c *client) EnqueueAt(ctx context.Context, task *Task, at time.Time) error {
	if task.ID == "" {
		task.ID = generateUUID()
	}

	serialized, err := c.codec.Marshal(task)
	if err != nil {
		return err
	}

	delayedKey := DelayedKey(task.Queue)

	if task.UniqueKey != "" {
		ttl := time.Duration(task.UniqueTTLMs) * time.Millisecond
		if ttl <= 0 {
			if c.defaultUniqueTTL > 0 {
				ttl = c.defaultUniqueTTL
			} else {
				ttl = 1 * time.Hour // Default to 1 hour
			}
		}
		uniqueKey := UniqueKey(task.Queue, task.UniqueKey)
		res, err := enqueueUniqueDelayedCmd.Run(ctx, c.rdb, []string{uniqueKey, delayedKey}, task.ID, int(ttl.Milliseconds()), serialized, at.UnixMilli()).Result()
		if err != nil {
			return err
		}
		if val, ok := res.(int64); ok && val == -1 {
			return ErrDuplicateTask
		}
		return nil
	}

	return c.rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: serialized,
	}).Err()
}

// ListDeadLetters returns the list of dead-letter tasks in the queue, sorted by descending death time
func (c *client) ListDeadLetters(ctx context.Context, queue string, limit int) ([]*Task, error) {
	dlqKey := DLQKey(queue)
	members, err := c.rdb.ZRevRange(ctx, dlqKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*Task, 0, len(members))
	for _, m := range members {
		task := &Task{}
		err := c.codec.Unmarshal(unsafeStringToBytes(m), task)
		if err != nil {
			continue // skip corrupted data
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// DeleteDeadLetter removes a specific task from the dead-letter queue by task ID
func (c *client) DeleteDeadLetter(ctx context.Context, queue string, taskID string) error {
	dlqKey := DLQKey(queue)
	dlqIndexKey := DLQIndexKey(queue)
	_, err := deleteDeadLetterCmd.Run(ctx, c.rdb, []string{dlqKey, dlqIndexKey}, taskID).Result()
	if err == redis.Nil {
		return nil
	}
	return err
}

// RetryDeadLetter retries a dead-letter task by resetting retry counts and re-enqueuing it
func (c *client) RetryDeadLetter(ctx context.Context, queue string, taskID string) error {
	dlqIndexKey := DLQIndexKey(queue)

	// Fetch serialized task in O(1) from the Hash index
	serialized, err := c.rdb.HGet(ctx, dlqIndexKey, taskID).Result()
	if err == redis.Nil {
		return fmt.Errorf("task ID %s not found in DLQ", taskID)
	} else if err != nil {
		return err
	}

	task := &Task{}
	err = c.codec.Unmarshal(unsafeStringToBytes(serialized), task)
	if err != nil {
		return fmt.Errorf("failed to unmarshal dead letter task: %w", err)
	}

	// Reset execution metrics
	task.Retry = 0
	task.LastError = ""

	// Re-enqueue the task
	err = c.Enqueue(ctx, task)
	if err != nil {
		return err
	}

	// Remove from DLQ structures atomically in O(1)
	return c.DeleteDeadLetter(ctx, queue, taskID)
}

func (c *client) RegisterCron(ctx context.Context, jobName string, spec string, task *Task) error {
	sched, err := CronParser.Parse(spec)
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

	serialized, err := c.codec.Marshal(task)
	if err != nil {
		return err
	}

	configsKey := CronConfigsKey(task.Queue)
	delayedKey := DelayedKey(task.Queue)
	firstRun := sched.Next(time.Now())

	_, err = registerCronCmd.Run(ctx, c.rdb, []string{configsKey, delayedKey}, jobName, spec, serialized, firstRun.UnixMilli()).Result()
	return err
}

func (c *client) CancelTask(ctx context.Context, queue, taskID string) error {
	cancelledKey := fmt.Sprintf("taskmq:{%s}:cancelled:%s", queue, taskID)
	if err := c.rdb.Set(ctx, cancelledKey, "1", 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("taskmq: failed to set cancel marker: %w", err)
	}

	cancelChannel := fmt.Sprintf("taskmq:{%s}:cancel", queue)
	return c.rdb.Publish(ctx, cancelChannel, taskID).Err()
}

func (c *client) Pause(ctx context.Context, queue string) error {
	pausedKey := PausedKey(queue)
	if err := c.rdb.Set(ctx, pausedKey, "1", 0).Err(); err != nil {
		return fmt.Errorf("taskmq: failed to set pause marker: %w", err)
	}

	controlChannel := ControlChannel(queue)
	return c.rdb.Publish(ctx, controlChannel, "pause").Err()
}

func (c *client) Resume(ctx context.Context, queue string) error {
	pausedKey := PausedKey(queue)
	if err := c.rdb.Del(ctx, pausedKey).Err(); err != nil {
		return fmt.Errorf("taskmq: failed to delete pause marker: %w", err)
	}

	controlChannel := ControlChannel(queue)
	return c.rdb.Publish(ctx, controlChannel, "resume").Err()
}

func (c *client) IsPaused(ctx context.Context, queue string) (bool, error) {
	pausedKey := PausedKey(queue)
	val, err := c.rdb.Exists(ctx, pausedKey).Result()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}

func (c *client) ListScheduledTasks(ctx context.Context, queue string, limit int) ([]*ScheduledTask, error) {
	delayedKey := DelayedKey(queue)
	zs, err := c.rdb.ZRangeWithScores(ctx, delayedKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	scheduled := make([]*ScheduledTask, 0, len(zs))
	for _, z := range zs {
		task := &Task{}
		memberStr, ok := z.Member.(string)
		if !ok {
			continue
		}
		err := c.codec.Unmarshal([]byte(memberStr), task)
		if err != nil {
			continue
		}
		runAt := time.UnixMilli(int64(z.Score))
		scheduled = append(scheduled, &ScheduledTask{
			Task:  task,
			RunAt: runAt,
		})
	}
	return scheduled, nil
}

func (c *client) RunScheduledTask(ctx context.Context, queue string, taskID string) error {
	delayedKey := DelayedKey(queue)
	members, err := c.rdb.ZRange(ctx, delayedKey, 0, -1).Result()
	if err != nil {
		return err
	}

	var targetMember string
	for _, m := range members {
		task := &Task{}
		if err := c.codec.Unmarshal([]byte(m), task); err == nil {
			if task.ID == taskID {
				targetMember = m
				break
			}
		}
	}

	if targetMember == "" {
		return fmt.Errorf("taskmq: task not found in scheduled tasks: %s", taskID)
	}

	res, err := c.rdb.ZRem(ctx, delayedKey, targetMember).Result()
	if err != nil {
		return err
	}
	if res == 0 {
		return fmt.Errorf("taskmq: task already processed or deleted: %s", taskID)
	}

	streamKey := StreamKey(queue)
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		ID:     "*",
		Values: map[string]interface{}{
			"task": targetMember,
		},
	}).Err()
}

func (c *client) DeleteScheduledTask(ctx context.Context, queue string, taskID string) error {
	delayedKey := DelayedKey(queue)
	members, err := c.rdb.ZRange(ctx, delayedKey, 0, -1).Result()
	if err != nil {
		return err
	}

	var targetMember string
	var targetTask *Task
	for _, m := range members {
		task := &Task{}
		if err := c.codec.Unmarshal([]byte(m), task); err == nil {
			if task.ID == taskID {
				targetMember = m
				targetTask = task
				break
			}
		}
	}

	if targetMember == "" {
		return fmt.Errorf("taskmq: task not found in scheduled tasks: %s", taskID)
	}

	res, err := c.rdb.ZRem(ctx, delayedKey, targetMember).Result()
	if err != nil {
		return err
	}
	if res == 0 {
		return fmt.Errorf("taskmq: task already processed or deleted: %s", taskID)
	}

	if targetTask.UniqueKey != "" {
		lockKey := UniqueKey(targetTask.Queue, targetTask.UniqueKey)
		_ = c.rdb.Del(ctx, lockKey).Err()
	}
	return nil
}

func (c *client) ListCronJobs(ctx context.Context, queue string) ([]*CronJob, error) {
	configsKey := CronConfigsKey(queue)
	configs, err := c.rdb.HGetAll(ctx, configsKey).Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]*CronJob, 0, len(configs))
	for jobName, configStr := range configs {
		task := &Task{}
		err := c.codec.Unmarshal([]byte(configStr), task)
		if err != nil {
			continue
		}

		nextRun := time.Time{}
		if task.CronSpec != "" {
			if sched, err := CronParser.Parse(task.CronSpec); err == nil {
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

func (c *client) RunCronJob(ctx context.Context, queue string, jobName string) error {
	configsKey := CronConfigsKey(queue)
	configStr, err := c.rdb.HGet(ctx, configsKey, jobName).Result()
	if err == redis.Nil {
		return fmt.Errorf("taskmq: cron job not found: %s", jobName)
	} else if err != nil {
		return err
	}

	streamKey := StreamKey(queue)
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		ID:     "*",
		Values: map[string]interface{}{
			"task": configStr,
		},
	}).Err()
}

func (c *client) DeleteCronJob(ctx context.Context, queue string, jobName string) error {
	configsKey := CronConfigsKey(queue)
	delayedKey := DelayedKey(queue)

	serialized, err := c.rdb.HGet(ctx, configsKey, jobName).Result()
	if err == redis.Nil {
		return fmt.Errorf("taskmq: cron job not found: %s", jobName)
	} else if err != nil {
		return err
	}

	_, _ = c.rdb.ZRem(ctx, delayedKey, serialized).Result()
	_, err = c.rdb.HDel(ctx, configsKey, jobName).Result()
	return err
}

func (c *client) RetryAllDeadLetters(ctx context.Context, queue string) (int64, error) {
	dlqKey := DLQKey(queue)

	var totalRetried int64
	for {
		members, err := c.rdb.ZRange(ctx, dlqKey, 0, 99).Result()
		if err != nil {
			return totalRetried, err
		}
		if len(members) == 0 {
			break
		}

		for _, m := range members {
			task := &Task{}
			if err := c.codec.Unmarshal(unsafeStringToBytes(m), task); err != nil {
				_ = c.DeleteDeadLetter(ctx, queue, task.ID)
				continue
			}

			task.Retry = 0
			task.LastError = ""

			if err := c.Enqueue(ctx, task); err != nil {
				return totalRetried, err
			}

			if err := c.DeleteDeadLetter(ctx, queue, task.ID); err != nil {
				return totalRetried, err
			}
			totalRetried++
		}
	}
	return totalRetried, nil
}

func (c *client) PurgeAllDeadLetters(ctx context.Context, queue string) (int64, error) {
	dlqKey := DLQKey(queue)
	dlqIndexKey := DLQIndexKey(queue)

	count, err := c.rdb.ZCard(ctx, dlqKey).Result()
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}

	err = c.rdb.Del(ctx, dlqKey, dlqIndexKey).Err()
	if err != nil {
		return 0, err
	}
	return count, nil
}
