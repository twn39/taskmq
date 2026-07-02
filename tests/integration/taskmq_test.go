package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

// 1. Existing MVP flow test
func TestTaskMQ_MVPFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "mvp_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	type WelcomeEmail struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}

	runChan := make(chan *WelcomeEmail, 1)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Group:       "test-group",
					Consumer:    "test-consumer",
					Concurrency: 2,
				})
				pool.Register("email:welcome", func(ctx context.Context, task *taskmq.Task) error {
					var email WelcomeEmail
					if err := json.Unmarshal(task.Payload, &email); err != nil {
						return err
					}
					runChan <- &email
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	rdb.Del(ctx, streamKey)
	defer rdb.Del(ctx, streamKey)

	app.RequireStart()
	defer app.RequireStop()

	emailData := WelcomeEmail{
		Email: "test@example.com",
		Name:  "John Doe",
	}
	payloadBytes, err := json.Marshal(emailData)
	assert.NoError(t, err)

	task := taskmq.NewTask("email:welcome", payloadBytes, taskmq.TaskOptions{
		Queue: queueName,
	})

	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case result := <-runChan:
		assert.Equal(t, "test@example.com", result.Email)
		assert.Equal(t, "John Doe", result.Name)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task execution")
	}

	time.Sleep(100 * time.Millisecond)

	pending, err := rdb.XPending(ctx, streamKey, "test-group").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

// 2. Delayed Tasks Flow Test
func TestTaskMQ_DelayedFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "delayed_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	delayedKey := taskmq.DelayedKey(queueName)

	runChan := make(chan time.Time, 1)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Group:       "delayed-group",
					Consumer:    "delayed-consumer",
					Concurrency: 1,
				})
				pool.Register("task:delayed", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- time.Now()
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, delayedKey)
	defer rdb.Del(ctx, streamKey, delayedKey)

	enqueueTime := time.Now()

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue with a 2-second delay
	task := taskmq.NewTask("task:delayed", []byte("delayed data"), taskmq.TaskOptions{
		Queue: queueName,
	})
	err := client.EnqueueIn(ctx, task, 2*time.Second)
	assert.NoError(t, err)

	select {
	case execTime := <-runChan:
		duration := execTime.Sub(enqueueTime)
		assert.GreaterOrEqual(t, duration.Seconds(), 1.8, "Task should execute after approximately 2 seconds")
		t.Logf("Task executed after %v", duration)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for delayed task execution")
	}
}

// 3. Retry and Exponential Backoff Flow Test
func TestTaskMQ_RetryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "retry_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	delayedKey := taskmq.DelayedKey(queueName)

	var execCount int64
	doneChan := make(chan bool, 1)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Group:       "retry-group",
					Consumer:    "retry-consumer",
					Concurrency: 1,
				})
				pool.Register("task:fail", func(ctx context.Context, task *taskmq.Task) error {
					current := atomic.AddInt64(&execCount, 1)
					if current >= 3 {
						doneChan <- true
					}
					return errors.New("simulated handler failure")
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, delayedKey)
	defer rdb.Del(ctx, streamKey, delayedKey)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue a task that allows 3 retries (MaxRetry = 3)
	task := taskmq.NewTask("task:fail", []byte("fail payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 3,
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case <-doneChan:
		t.Log("Task failed 3 times and completed retry loop")
		assert.Equal(t, int64(3), atomic.LoadInt64(&execCount))
	case <-ctx.Done():
		t.Fatal("Timeout waiting for retries to complete")
	}

	time.Sleep(100 * time.Millisecond)

	// Stream PEL should be empty now that task exceeded max retries and got XACKed
	pending, err := rdb.XPending(ctx, streamKey, "retry-group").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

// 4. Janitor Recovery Flow Test
func TestTaskMQ_JanitorRecoveryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueName := "janitor_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	var runCount int64
	doneChan := make(chan bool, 1)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Group:       "janitor-group",
					Consumer:    "janitor-consumer",
					Concurrency: 1,
				})
				pool.Register("task:crash", func(ctx context.Context, task *taskmq.Task) error {
					count := atomic.AddInt64(&runCount, 1)
					if count == 1 {
						// Simulate worker crash: return error, but bypass standard retry logic
						// by modifying the task object manually to prevent normal retry, and NOT calling XAck.
						// We do this by throwing a custom error, and intercepting it, or simply sleeping
						// until the janitor claims it.
						// Actually, returning a custom error in Go handler:
						// Let's make it block/sleep to hold the execution, or just return an error but we hack the worker logic?
						// Wait, if we return an error, the worker will run handleFailure.
						// To simulate a crash (where the message stays in PEL but is NOT rescheduled to ZSET),
						// we can return nil but NOT XAck it? No, w.processMessage automatically ACKs if handler returns nil.
						// What if we sleep to block the worker, but wait, if the worker is blocked, it won't process it.
						// Wait, we can return an error that we check, or we can just make the handler sleep indefinitely,
						// and since the worker has concurrency = 1, if it sleeps, it is busy. But the Janitor will run XAutoClaim
						// and assign it to the consumer, which will spawn another processMessage in a goroutine!
						// Yes! The Janitor spawns processMessage in a new goroutine!
						// Let's make the handler sleep for 10 seconds on the first run, and return nil on the second run!
						t.Log("Simulating crashed worker on first execution (handler sleeping...)")
						time.Sleep(10 * time.Second)
						return nil
					}
					
					t.Log("Janitor successfully recovered and executed task on second run!")
					doneChan <- true
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey)
	defer rdb.Del(ctx, streamKey)

	app.RequireStart()
	defer app.RequireStop()

	task := taskmq.NewTask("task:crash", []byte("crash payload"), taskmq.TaskOptions{
		Queue: queueName,
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case <-doneChan:
		assert.Equal(t, int64(2), atomic.LoadInt64(&runCount), "Task should have run twice (once stalled, once recovered by janitor)")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for Janitor recovery")
	}
}

// 5. Timeout Cancellation Flow Test
func TestTaskMQ_TimeoutCancellationFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "timeout_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	delayedKey := taskmq.DelayedKey(queueName)

	var execCount int64

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Group:       "timeout-group",
					Consumer:    "timeout-consumer",
					Concurrency: 1,
				})
				pool.Register("task:slow", func(ctx context.Context, task *taskmq.Task) error {
					atomic.AddInt64(&execCount, 1)

					// Sleep for 3 seconds, but check if context is cancelled
					select {
					case <-time.After(3 * time.Second):
						return nil
					case <-ctx.Done():
						// Context cancelled (timed out)
						return ctx.Err()
					}
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, delayedKey)
	defer rdb.Del(ctx, streamKey, delayedKey)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue a task with TimeoutMs = 500 (0.5 second), MaxRetry = 2
	task := taskmq.NewTask("task:slow", []byte("slow payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 2,
		Timeout:  500 * time.Millisecond,
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for the task to time out and start its second execution (execCount >= 2)
	timeoutTicker := time.NewTicker(100 * time.Millisecond)
	defer timeoutTicker.Stop()

	for atomic.LoadInt64(&execCount) < 2 {
		select {
		case <-timeoutTicker.C:
		case <-ctx.Done():
			t.Fatal("Timeout waiting for task timeout and retry execution")
		}
	}
	t.Log("Task successfully timed out on first run and triggered retry")
}

// 6. Uniqueness / Deduplication Flow Test
func TestTaskMQ_UniquenessFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "unique_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	uniqueLockKey := taskmq.UniqueKey(queueName, "my-unique-key")

	runChan := make(chan bool, 1)

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Group:       "unique-group",
					Consumer:    "unique-consumer",
					Concurrency: 1,
				})
				pool.Register("task:unique", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- true
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, uniqueLockKey)
	defer rdb.Del(ctx, streamKey, uniqueLockKey)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Enqueue unique task 1
	task1 := taskmq.NewTask("task:unique", []byte("data 1"), taskmq.TaskOptions{
		Queue:     queueName,
		UniqueKey: "my-unique-key",
		UniqueTTL: 5 * time.Second,
	})
	err := client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	// 2. Try enqueuing unique task 2 (with same unique key)
	task2 := taskmq.NewTask("task:unique", []byte("data 2"), taskmq.TaskOptions{
		Queue:     queueName,
		UniqueKey: "my-unique-key",
		UniqueTTL: 5 * time.Second,
	})
	err = client.Enqueue(ctx, task2)
	assert.ErrorIs(t, err, taskmq.ErrDuplicateTask, "Should return ErrDuplicateTask on duplicates")

	// 3. Wait for task 1 to run successfully
	select {
	case <-runChan:
		t.Log("Task 1 executed successfully")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task 1 execution")
	}

	// Wait briefly to allow unlocking to finish in worker hook
	time.Sleep(100 * time.Millisecond)

	// 4. Try enqueuing task 2 again (should succeed now that task 1 completed and lock was released)
	err = client.Enqueue(ctx, task2)
	assert.NoError(t, err, "Should allow enqueuing after lock has been released")

	select {
	case <-runChan:
		t.Log("Task 2 executed successfully after lock release")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task 2 execution")
	}
}

// 7. Dead Letter Queue (DLQ) Flow Test
func TestTaskMQ_DLQFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "dlq_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	dlqKey := taskmq.DLQKey(queueName)

	runChan := make(chan error, 2)
	var attempt int64

	var rdb *goredis.Client
	var client taskmq.Client

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Group:       "dlq-group",
					Consumer:    "dlq-consumer",
					Concurrency: 1,
				})
				pool.Register("task:fail", func(ctx context.Context, task *taskmq.Task) error {
					att := atomic.AddInt64(&attempt, 1)
					if att == 1 {
						err := errors.New("simulated handler error")
						runChan <- err
						return err
					}
					// Second run succeeds!
					runChan <- nil
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client),
	)

	rdb.Del(ctx, streamKey, dlqKey)
	defer rdb.Del(ctx, streamKey, dlqKey)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue a task with MaxRetry = 1 (runs once, fails, immediately archived to DLQ)
	task := taskmq.NewTask("task:fail", []byte("fail payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 1,
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for first handler run to fail
	select {
	case err := <-runChan:
		assert.ErrorContains(t, err, "simulated handler error")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task execution failure")
	}

	// Wait briefly to allow archiving to DLQ to complete
	time.Sleep(100 * time.Millisecond)

	// 1. List dead letters and verify
	deadTasks, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasks, 1)
	assert.Equal(t, task.ID, deadTasks[0].ID)
	assert.Equal(t, "simulated handler error", deadTasks[0].LastError)

	// 2. Retry dead letter
	err = client.RetryDeadLetter(ctx, queueName, task.ID)
	assert.NoError(t, err)

	// Verify it was removed from DLQ ZSET index immediately by RetryDeadLetter
	deadTasksAfter, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasksAfter, 0)

	// Wait for the retried task to execute successfully
	select {
	case err := <-runChan:
		assert.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for retried task success execution")
	}

	// Wait briefly
	time.Sleep(100 * time.Millisecond)

	// Verify DLQ remains empty on success
	deadTasksAfterSuccess, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasksAfterSuccess, 0)

	// 3. Test DeleteDeadLetter
	// Reset attempt to 1 so the next enqueued task fails again
	atomic.StoreInt64(&attempt, 0)

	taskToDelete := taskmq.NewTask("task:fail", []byte("delete payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 1,
	})

	err = client.Enqueue(ctx, taskToDelete)
	assert.NoError(t, err)

	select {
	case err := <-runChan:
		assert.ErrorContains(t, err, "simulated handler error")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for second task failure")
	}

	time.Sleep(100 * time.Millisecond)

	// Verify it is in DLQ
	deadTasksBeforeDelete, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasksBeforeDelete, 1)

	// Delete it
	err = client.DeleteDeadLetter(ctx, queueName, taskToDelete.ID)
	assert.NoError(t, err)

	// Verify DLQ is empty again
	deadTasksAfterDelete, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, deadTasksAfterDelete, 0)
}
