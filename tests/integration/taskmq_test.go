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
	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
	"github.com/twn39/taskmq/internal/logger"
	internalredis "github.com/twn39/taskmq/internal/redis"
	"github.com/twn39/taskmq/internal/taskmq"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func TestTaskMQ_CronFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := "cron_test_queue"

	var runCount int64
	doneChan := make(chan bool, 1)

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
					Concurrency:         2,
					CronHealingInterval: 2 * time.Second,
					CronHealingLockTTL:  1800 * time.Millisecond,
				})
				// Register handler for the cron job
				pool.Register("cron:ticker", func(ctx context.Context, task *taskmq.Task) error {
					val := atomic.AddInt64(&runCount, 1)
					if val >= 3 {
						select {
						case doneChan <- true:
						default:
						}
					}
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		taskmq.StreamKey(queueName),
		taskmq.DelayedKey(queueName),
		taskmq.CronConfigsKey(queueName),
	).Err()
	assert.NoError(t, err)

	// Register the Cron task before starting the worker
	task := taskmq.NewTask("cron:ticker", []byte("tick-payload"), taskmq.TaskOptions{
		Queue: queueName,
	})
	// Trigger every 2 seconds
	err = client.RegisterCron(ctx, "cron:ticker", "*/2 * * * * *", task)
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// Wait for cron to trigger at least 3 times
	select {
	case <-doneChan:
		// Success!
	case <-ctx.Done():
		t.Fatal("Timeout waiting for cron to execute 3 times")
	}

	// Now verify Self-Healing:
	// 1. Corrupt/Delete the ZSET entry representing the next scheduled run
	delayedKey := taskmq.DelayedKey(queueName)
	err = rdb.Del(ctx, delayedKey).Err()
	assert.NoError(t, err)

	// Reset run counter
	atomic.StoreInt64(&runCount, 0)

	// 2. Wait for Self-healing loop to detect the missing scheduled execution and heal it
	// Self-healing loop runs every 10 seconds. So within 15 seconds, it should heal and execute at least once.
	healCtx, healCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer healCancel()

	healChan := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-healCtx.Done():
				return
			default:
				if atomic.LoadInt64(&runCount) >= 1 {
					healChan <- true
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	select {
	case <-healChan:
		// Self-healing recovered the scheduler chain successfully!
	case <-healCtx.Done():
		t.Fatal("Timeout waiting for self-healing loop to reschedule missing cron task")
	}
}

func TestTaskMQ_GRPCFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "grpc_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	runChan := make(chan string, 1)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			taskmq.NewClient,
			taskmq.NewGRPCServer,
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Concurrency: 2,
				})
				pool.Register("task:grpc-test", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Invoke(taskmq.RegisterGRPCServerLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	err := rdb.Del(ctx, streamKey).Err()
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// 1. Establish gRPC Client Connection to localhost:50051 (default port in config)
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	grpcClient := taskmqv1.NewTaskMQServiceClient(conn)

	// 2. Perform Enqueue via gRPC
	grpcTask := &taskmqv1.Task{
		Queue:   queueName,
		Name:    "task:grpc-test",
		Payload: []byte("grpc-payload"),
	}

	resp, err := grpcClient.Enqueue(ctx, &taskmqv1.EnqueueRequest{Task: grpcTask})
	if err != nil {
		t.Fatalf("gRPC Enqueue failed: %v", err)
	}
	assert.NotEmpty(t, resp.TaskId)

	// 3. Verify task is executed
	select {
	case result := <-runChan:
		assert.Equal(t, "grpc-payload", result)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task to execute via gRPC Enqueue")
	}
}

func TestTaskMQ_BinaryCodec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "binary_codec_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	runChan := make(chan string, 1)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	binaryCodec := taskmq.BinaryCodec{}

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb *goredis.Client) taskmq.Client {
				return taskmq.NewClient(rdb, taskmq.WithClientCodec(binaryCodec))
			},
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Concurrency: 2,
					Codec:       binaryCodec,
				})
				pool.Register("task:binary-test", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	err := rdb.Del(ctx, streamKey).Err()
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	task := taskmq.NewTask("task:binary-test", []byte("binary-payload"), taskmq.TaskOptions{
		Queue: queueName,
	})

	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case result := <-runChan:
		assert.Equal(t, "binary-payload", result)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task to execute via BinaryCodec")
	}
}

func TestTaskMQ_SyncExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "sync_exec_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	runChan := make(chan string, 1)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb *goredis.Client) taskmq.Client {
				return taskmq.NewClient(rdb)
			},
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Concurrency:   2,
					SyncExecution: true,
				})
				pool.Register("task:sync-exec-test", func(ctx context.Context, task *taskmq.Task) error {
					runChan <- string(task.Payload)
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	err := rdb.Del(ctx, streamKey).Err()
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	task := taskmq.NewTask("task:sync-exec-test", []byte("sync-exec-payload"), taskmq.TaskOptions{
		Queue: queueName,
	})

	err = client.Enqueue(ctx, task)
	assert.NoError(t, err)

	select {
	case result := <-runChan:
		assert.Equal(t, "sync-exec-payload", result)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for task to execute via SyncExecution")
	}
}

func TestTaskMQ_ExecutionPoolPanicRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "pool_panic_test_queue"
	streamKey := taskmq.StreamKey(queueName)
	dlqKey := taskmq.DLQKey(queueName)

	var rdb *goredis.Client
	var client taskmq.Client
	var worker taskmq.Worker

	app := fxtest.New(t,
		fx.Provide(
			NewTestConfig,
			logger.NewLogger,
			internalredis.NewRedisClient,
			func(rdb *goredis.Client) taskmq.Client {
				return taskmq.NewClient(rdb)
			},
			func(rdb *goredis.Client, logger *zap.Logger) taskmq.Worker {
				pool := taskmq.NewWorkerPool(rdb, logger, queueName, taskmq.WorkerOptions{
					Concurrency: 2,
				})
				pool.Register("task:panic-test", func(ctx context.Context, task *taskmq.Task) error {
					panic("something went terribly wrong")
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis
	_ = rdb.Del(ctx, streamKey).Err()
	_ = rdb.Del(ctx, dlqKey).Err()

	app.RequireStart()
	defer app.RequireStop()

	task := taskmq.NewTask("task:panic-test", []byte("panic-payload"), taskmq.TaskOptions{
		Queue:    queueName,
		MaxRetry: 1, // Fail immediately to DLQ
	})

	err := client.Enqueue(ctx, task)
	assert.NoError(t, err)

	// Wait for task to fail and end up in DLQ
	time.Sleep(1 * time.Second)

	// Check DLQ
	dlqTasks, err := client.ListDeadLetters(ctx, queueName, 10)
	assert.NoError(t, err)
	assert.Len(t, dlqTasks, 1)
	assert.Contains(t, dlqTasks[0].LastError, "task handler panicked: something went terribly wrong")
}

func TestTaskMQ_CronSelfHealing_CustomConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "cron_healing_custom_test_queue"

	var runCount int64
	doneChan := make(chan bool, 1)

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
					Concurrency:         1,
					CronHealingInterval: 1 * time.Second,
					CronHealingLockTTL:  800 * time.Millisecond,
				})
				pool.Register("cron:healing:custom", func(ctx context.Context, task *taskmq.Task) error {
					val := atomic.AddInt64(&runCount, 1)
					if val >= 1 {
						select {
						case doneChan <- true:
						default:
						}
					}
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx,
		taskmq.StreamKey(queueName),
		taskmq.DelayedKey(queueName),
		taskmq.CronConfigsKey(queueName),
	).Err()
	assert.NoError(t, err)

	// Register the Cron task before starting the worker
	task := taskmq.NewTask("cron:healing:custom", []byte("healing-payload"), taskmq.TaskOptions{
		Queue: queueName,
	})
	// Trigger every 1 second
	err = client.RegisterCron(ctx, "cron:healing:custom", "*/1 * * * * *", task)
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// Wait for cron to run at least once
	select {
	case <-doneChan:
		// Success!
	case <-ctx.Done():
		t.Fatal("Timeout waiting for cron task execution")
	}

	// Corrupt/Delete the ZSET entry to simulate broken chain
	delayedKey := taskmq.DelayedKey(queueName)
	err = rdb.Del(ctx, delayedKey).Err()
	assert.NoError(t, err)

	// Reset run counter
	atomic.StoreInt64(&runCount, 0)

	// Wait for self-healing loop to heal and reschedule (interval is 1s, so it should heal within 2s)
	healCtx, healCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer healCancel()

	healChan := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-healCtx.Done():
				return
			default:
				if atomic.LoadInt64(&runCount) >= 1 {
					healChan <- true
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	select {
	case <-healChan:
		// Successfully healed!
	case <-healCtx.Done():
		t.Fatal("Timeout waiting for custom config self-healing loop to reschedule task")
	}
}

func TestTaskMQ_BackpressureFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := "backpressure_test_queue"
	streamKey := taskmq.StreamKey(queueName)

	var runCount int64
	var retryCount int64
	doneChan := make(chan bool, 2)

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
					Concurrency:       1,
					ExecutionPoolSize: 1, // Only 1 concurrent task execution allowed
				})
				pool.Register("task:slow", func(ctx context.Context, task *taskmq.Task) error {
					atomic.AddInt64(&runCount, 1)
					if task.Retry > 0 {
						atomic.AddInt64(&retryCount, 1)
					}
					
					// Sleep duration determined by payload
					sleepMs := 100
					if string(task.Payload) == "payload 1" {
						sleepMs = 2000
					}
					time.Sleep(time.Duration(sleepMs) * time.Millisecond)
					doneChan <- true
					return nil
				})
				return pool
			},
		),
		fx.Invoke(taskmq.RegisterWorkerPoolLifecycle),
		fx.Populate(&rdb, &client, &worker),
	)

	// Clean up Redis before test
	err := rdb.Del(ctx, streamKey).Err()
	assert.NoError(t, err)

	app.RequireStart()
	defer app.RequireStop()

	// Enqueue Task 1 and Task 2.
	// Task 1 has 5 seconds timeout (runs for 2 seconds).
	// Task 2 has 1 second timeout (runs for 0.1 second).
	// Because of backpressure, Task 2 should NOT be pulled into memory while Task 1 is executing.
	// Therefore, Task 2 will NOT time out in the queue, and both will finish successfully!
	task1 := taskmq.NewTask("task:slow", []byte("payload 1"), taskmq.TaskOptions{
		Queue:    queueName,
		Timeout:  5 * time.Second,
		MaxRetry: 1,
	})
	task2 := taskmq.NewTask("task:slow", []byte("payload 2"), taskmq.TaskOptions{
		Queue:    queueName,
		Timeout:  1 * time.Second,
		MaxRetry: 1,
	})

	err = client.Enqueue(ctx, task1)
	assert.NoError(t, err)

	// Wait briefly to make sure Task 1 starts executing
	time.Sleep(100 * time.Millisecond)

	err = client.Enqueue(ctx, task2)
	assert.NoError(t, err)

	// Wait for both tasks to execute successfully
	for i := 0; i < 2; i++ {
		select {
		case <-doneChan:
			// One task finished successfully
		case <-ctx.Done():
			t.Fatal("Timeout waiting for tasks to execute under backpressure")
		}
	}

	assert.Equal(t, int64(2), atomic.LoadInt64(&runCount), "Both tasks should run successfully")
	assert.Equal(t, int64(0), atomic.LoadInt64(&retryCount), "No task should have failed/retried due to queue timeout")
}


