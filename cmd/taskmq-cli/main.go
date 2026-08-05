package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	mqclient "github.com/twn39/taskmq/internal/taskmq/client"
	mqkeys "github.com/twn39/taskmq/internal/taskmq/keys"
	"github.com/twn39/taskmq/internal/taskmq/metricsq"
	"github.com/urfave/cli/v3"
)

var (
	rdb       redis.UniversalClient
	queues    mqclient.QueueController
	dlq       mqclient.DLQManager
	inspect   mqclient.TaskInspector
	events    mqclient.EventReader
	workers   mqclient.WorkerViewer
	canceler  mqclient.TaskCanceler
	scheduled mqclient.ScheduledTaskManager
	active    mqclient.ActiveTaskManager
)

func initRedis(cmd *cli.Command) error {
	addr := cmd.String("redis-addr")
	db := int(cmd.Int("redis-db"))
	password := cmd.String("redis-password")

	rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		DB:       db,
		Password: password,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis at %s: %w", addr, err)
	}

	// NewClient still returns the facade; CLI only keeps narrow ports.
	c := mqclient.NewClient(rdb)
	queues = c
	dlq = c
	inspect = c
	events = c
	workers = c
	canceler = c
	scheduled = c
	active = c
	return nil
}

func printUsage() {
	fmt.Println("Usage: taskmq-cli [global options] <command> [command options] [args]")
	fmt.Println()
	fmt.Println("Global Options:")
	fmt.Println("  -redis-addr string      Redis server address (default \"localhost:6379\", env: REDIS_ADDR)")
	fmt.Println("  -redis-db int           Redis database number (default 0, env: REDIS_DB)")
	fmt.Println("  -redis-password string  Redis password (default \"\", env: REDIS_PASSWORD)")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  stats                   Display real-time statistics of all active queues")
	fmt.Println("  pause <queue>           Pause message consumption of specified queue")
	fmt.Println("  resume <queue>          Resume message consumption of specified queue")
	fmt.Println("  dlq list <queue> [limit] List dead-lettered tasks in specified queue")
	fmt.Println("  dlq retry <queue> <id>  Retry a specified dead-lettered task")
	fmt.Println("  dlq delete <queue> <id> Delete a specified dead-lettered task")
	fmt.Println("  task get <queue> <id>   Show durable task metadata by id")
	fmt.Println("  task cancel <queue> <id> Mark task cancelled (before-run drop / mid-run signal)")
	fmt.Println("  task scheduled <queue> [limit] List delayed/scheduled tasks")
	fmt.Println("  task active <queue> [limit] List stream messages (pending/processing)")
	fmt.Println("  events <queue> [limit]  List recent queue lifecycle events")
	fmt.Println("  workers <queue>         List live worker heartbeats")
	fmt.Println("  metrics [queue]         Redis-backed queue counters + depths (all queues if omitted)")
}

func main() {
	cmd := &cli.Command{
		Name:  "taskmq-cli",
		Usage: "Distributed task queue CLI",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "redis-addr",
				Value:   "localhost:6379",
				Usage:   "Redis server address",
				Sources: cli.EnvVars("REDIS_ADDR"),
			},
			&cli.IntFlag{
				Name:    "redis-db",
				Value:   0,
				Usage:   "Redis database number",
				Sources: cli.EnvVars("REDIS_DB"),
			},
			&cli.StringFlag{
				Name:    "redis-password",
				Value:   "",
				Usage:   "Redis password",
				Sources: cli.EnvVars("REDIS_PASSWORD"),
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.NArg() > 0 {
				fmt.Printf("Error: Unknown command '%s'\n", cmd.Args().Get(0))
				printUsage()
				return cli.Exit("", 1)
			}
			printUsage()
			return cli.Exit("", 1)
		},
		Commands: []*cli.Command{
			{
				Name:  "stats",
				Usage: "Display real-time statistics of all active queues",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := initRedis(cmd); err != nil {
						return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
					}
					handleStats(ctx, rdb)
					return nil
				},
			},
			{
				Name:      "pause",
				Usage:     "Pause message consumption of specified queue",
				ArgsUsage: "<queue>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 1 {
						return cli.Exit("Error: Missing queue name. Usage: taskmq-cli pause <queue>", 1)
					}
					queue := cmd.Args().Get(0)
					if err := initRedis(cmd); err != nil {
						return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
					}
					if err := queues.Pause(ctx, queue); err != nil {
						return cli.Exit(fmt.Sprintf("Error: Failed to pause queue %s: %v", queue, err), 1)
					}
					fmt.Printf("Success: Queue '%s' paused successfully.\n", queue)
					return nil
				},
			},
			{
				Name:      "resume",
				Usage:     "Resume message consumption of specified queue",
				ArgsUsage: "<queue>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 1 {
						return cli.Exit("Error: Missing queue name. Usage: taskmq-cli resume <queue>", 1)
					}
					queue := cmd.Args().Get(0)
					if err := initRedis(cmd); err != nil {
						return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
					}
					if err := queues.Resume(ctx, queue); err != nil {
						return cli.Exit(fmt.Sprintf("Error: Failed to resume queue %s: %v", queue, err), 1)
					}
					fmt.Printf("Success: Queue '%s' resumed successfully.\n", queue)
					return nil
				},
			},
			{
				Name:  "dlq",
				Usage: "Dead-letter queue operations",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() > 0 {
						sub := cmd.Args().Get(0)
						return cli.Exit(fmt.Sprintf("Error: Unknown dlq sub-command '%s'. Supported: list, retry, delete", sub), 1)
					}
					return cli.Exit("Error: Missing sub-command. Usage: taskmq-cli dlq [list|retry|delete]", 1)
				},
				Commands: []*cli.Command{
					{
						Name:      "list",
						Usage:     "List dead-lettered tasks in specified queue",
						ArgsUsage: "<queue> [limit]",
						Action: func(ctx context.Context, cmd *cli.Command) error {
							if cmd.NArg() < 1 {
								return cli.Exit("Error: Missing queue name. Usage: taskmq-cli dlq list <queue> [limit]", 1)
							}
							queue := cmd.Args().Get(0)
							limit := 20
							if cmd.NArg() >= 2 {
								if lim, err := strconv.Atoi(cmd.Args().Get(1)); err == nil && lim > 0 {
									limit = lim
								}
							}
							if err := initRedis(cmd); err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							tasks, err := dlq.ListDeadLetters(ctx, queue, limit)
							if err != nil {
								return cli.Exit(fmt.Sprintf("Error: Failed to list DLQ for queue %s: %v", queue, err), 1)
							}

							if len(tasks) == 0 {
								fmt.Printf("Queue '%s' dead-letter queue is empty.\n", queue)
								return nil
							}

							w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
							fmt.Fprintln(w, "TASK ID\tTASK NAME\tRETRIES\tLAST ERROR")
							for _, t := range tasks {
								fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", t.ID, t.Name, t.Retry, t.LastError)
							}
							_ = w.Flush()
							return nil
						},
					},
					{
						Name:      "retry",
						Usage:     "Retry a specified dead-lettered task",
						ArgsUsage: "<queue> <id>",
						Action: func(ctx context.Context, cmd *cli.Command) error {
							if cmd.NArg() < 2 {
								return cli.Exit("Error: Missing arguments. Usage: taskmq-cli dlq retry <queue> <id>", 1)
							}
							queue := cmd.Args().Get(0)
							taskID := cmd.Args().Get(1)
							if err := initRedis(cmd); err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							if err := dlq.RetryDeadLetter(ctx, queue, taskID); err != nil {
								return cli.Exit(fmt.Sprintf("Error: Failed to retry dead letter %s in queue %s: %v", taskID, queue, err), 1)
							}
							fmt.Printf("Success: Task '%s' in queue '%s' successfully re-enqueued for retry.\n", taskID, queue)
							return nil
						},
					},
					{
						Name:      "delete",
						Usage:     "Delete a specified dead-lettered task",
						ArgsUsage: "<queue> <id>",
						Action: func(ctx context.Context, cmd *cli.Command) error {
							if cmd.NArg() < 2 {
								return cli.Exit("Error: Missing arguments. Usage: taskmq-cli dlq delete <queue> <id>", 1)
							}
							queue := cmd.Args().Get(0)
							taskID := cmd.Args().Get(1)
							if err := initRedis(cmd); err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							if err := dlq.DeleteDeadLetter(ctx, queue, taskID); err != nil {
								return cli.Exit(fmt.Sprintf("Error: Failed to delete dead letter %s in queue %s: %v", taskID, queue, err), 1)
							}
							fmt.Printf("Success: Task '%s' deleted from DLQ in queue '%s'.\n", taskID, queue)
							return nil
						},
					},
				},
			},
			{
				Name:      "events",
				Usage:     "List recent queue lifecycle events",
				ArgsUsage: "<queue> [limit]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 1 {
						return cli.Exit("Error: Usage: taskmq-cli events <queue> [limit]", 1)
					}
					queue := cmd.Args().Get(0)
					limit := int64(20)
					if cmd.NArg() >= 2 {
						if lim, err := strconv.ParseInt(cmd.Args().Get(1), 10, 64); err == nil && lim > 0 {
							limit = lim
						}
					}
					if err := initRedis(cmd); err != nil {
						return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
					}
					evs, err := events.ListEvents(ctx, queue, limit)
					if err != nil {
						return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
					}
					if len(evs) == 0 {
						fmt.Printf("No events for queue '%s'.\n", queue)
						return nil
					}
					w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
					fmt.Fprintln(w, "ID\tTYPE\tTASK\tNAME\tERROR")
					for _, e := range evs {
						fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", e.ID, e.Type, e.TaskID, e.Name, e.Error)
					}
					_ = w.Flush()
					return nil
				},
			},
			{
				Name:      "workers",
				Usage:     "List live worker heartbeats for a queue",
				ArgsUsage: "<queue>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 1 {
						return cli.Exit("Error: Usage: taskmq-cli workers <queue>", 1)
					}
					queue := cmd.Args().Get(0)
					if err := initRedis(cmd); err != nil {
						return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
					}
					list, err := workers.ListWorkers(ctx, queue)
					if err != nil {
						return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
					}
					if len(list) == 0 {
						fmt.Printf("No live workers for queue '%s'.\n", queue)
						return nil
					}
					w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
					fmt.Fprintln(w, "CONSUMER\tHOST\tPID\tIN_USE/CONC\tACTIVE_TASK\tUPDATED")
					for _, wv := range list {
						fmt.Fprintf(w, "%s\t%s\t%d\t%d/%d\t%s\t%s\n",
							wv.Consumer, wv.Host, wv.PID, wv.InUse, wv.Concurrency, wv.ActiveTask,
							wv.UpdatedAt.Format(time.RFC3339))
					}
					_ = w.Flush()
					return nil
				},
			},
			{
				Name:      "metrics",
				Usage:     "Show Redis-backed queue counters and depths (cross-process)",
				ArgsUsage: "[queue]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := initRedis(cmd); err != nil {
						return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
					}
					var names []string
					if cmd.NArg() >= 1 {
						names = []string{cmd.Args().Get(0)}
					} else {
						redisKeys, err := rdb.Keys(ctx, mqkeys.StreamScanPattern()).Result()
						if err != nil {
							return cli.Exit(fmt.Sprintf("Error: Failed to scan queues: %v", err), 1)
						}
						seen := make(map[string]bool)
						for _, key := range redisKeys {
							if q, ok := mqkeys.ParseQueueFromStreamKey(key); ok && !seen[q] {
								seen[q] = true
								names = append(names, q)
							}
						}
					}
					if len(names) == 0 {
						fmt.Println("No TaskMQ queues found.")
						return nil
					}
					store := metricsq.NewStore(rdb)
					w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
					fmt.Fprintln(w, "QUEUE\tSTREAM\tDELAYED\tDLQ\tCOMPLETED\tPAUSED\tPROCESSED\tFAILED\tRETRIED\tDEFERRED")
					for _, q := range names {
						d, err := metricsq.Depths(ctx, rdb, q)
						if err != nil {
							return cli.Exit(fmt.Sprintf("Error: depths %s: %v", q, err), 1)
						}
						snap, err := store.Snapshot(ctx, q)
						if err != nil {
							return cli.Exit(fmt.Sprintf("Error: snapshot %s: %v", q, err), 1)
						}
						paused := "no"
						if d.Paused {
							paused = "yes"
						}
						fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\n",
							q, d.StreamLen, d.Delayed, d.DLQ, d.Completed, paused,
							snap[metricsq.FieldProcessed]+snap[metricsq.FieldCompleted],
							snap[metricsq.FieldFailed]+snap[metricsq.FieldDLQ],
							snap[metricsq.FieldRetried],
							snap[metricsq.FieldDeferred],
						)
					}
					_ = w.Flush()
					return nil
				},
			},
			{
				Name:  "task",
				Usage: "Task inspection and control",
				Commands: []*cli.Command{
					{
						Name:      "get",
						Usage:     "Show durable task metadata by queue and id",
						ArgsUsage: "<queue> <id>",
						Action: func(ctx context.Context, cmd *cli.Command) error {
							if cmd.NArg() < 2 {
								return cli.Exit("Error: Missing arguments. Usage: taskmq-cli task get <queue> <id>", 1)
							}
							queue := cmd.Args().Get(0)
							taskID := cmd.Args().Get(1)
							if err := initRedis(cmd); err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							info, err := inspect.GetTaskInfo(ctx, queue, taskID)
							if err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							if info == nil {
								return cli.Exit(fmt.Sprintf("Task '%s' not found in queue '%s' (no meta)", taskID, queue), 1)
							}
							fmt.Printf("ID:          %s\n", info.ID)
							fmt.Printf("Queue:       %s\n", info.Queue)
							fmt.Printf("Name:        %s\n", info.Name)
							fmt.Printf("State:       %s\n", info.State)
							fmt.Printf("Retry:       %d / %d\n", info.Retry, info.MaxRetry)
							if info.LastError != "" {
								fmt.Printf("LastError:   %s\n", info.LastError)
							}
							if info.StreamID != "" {
								fmt.Printf("StreamID:    %s\n", info.StreamID)
							}
							if len(info.Result) > 0 {
								fmt.Printf("Result:      %s\n", string(info.Result))
							}
							if !info.UpdatedAt.IsZero() {
								fmt.Printf("UpdatedAt:   %s\n", info.UpdatedAt.Format(time.RFC3339))
							}
							if !info.CompletedAt.IsZero() {
								fmt.Printf("CompletedAt: %s\n", info.CompletedAt.Format(time.RFC3339))
							}
							return nil
						},
					},
					{
						Name:      "cancel",
						Usage:     "Mark a task as cancelled (before-run drop / mid-run signal)",
						ArgsUsage: "<queue> <id>",
						Action: func(ctx context.Context, cmd *cli.Command) error {
							if cmd.NArg() < 2 {
								return cli.Exit("Error: Missing arguments. Usage: taskmq-cli task cancel <queue> <id>", 1)
							}
							queue := cmd.Args().Get(0)
							taskID := cmd.Args().Get(1)
							if err := initRedis(cmd); err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							if err := canceler.CancelTask(ctx, queue, taskID); err != nil {
								return cli.Exit(fmt.Sprintf("Error: Failed to cancel task %s in queue %s: %v", taskID, queue, err), 1)
							}
							fmt.Printf("Success: Task '%s' in queue '%s' marked cancelled.\n", taskID, queue)
							return nil
						},
					},
					{
						Name:      "scheduled",
						Usage:     "List delayed/scheduled tasks for a queue",
						ArgsUsage: "<queue> [limit]",
						Action: func(ctx context.Context, cmd *cli.Command) error {
							if cmd.NArg() < 1 {
								return cli.Exit("Error: Missing queue name. Usage: taskmq-cli task scheduled <queue> [limit]", 1)
							}
							queue := cmd.Args().Get(0)
							limit := 20
							if cmd.NArg() >= 2 {
								if lim, err := strconv.Atoi(cmd.Args().Get(1)); err == nil && lim > 0 {
									limit = lim
								}
							}
							if err := initRedis(cmd); err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							list, err := scheduled.ListScheduledTasks(ctx, queue, limit)
							if err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							if len(list) == 0 {
								fmt.Printf("No scheduled tasks for queue '%s'.\n", queue)
								return nil
							}
							w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
							fmt.Fprintln(w, "TASK ID\tNAME\tRUN AT\tRETRIES")
							for _, st := range list {
								fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", st.ID, st.Name, st.RunAt.Format(time.RFC3339), st.Retry)
							}
							_ = w.Flush()
							return nil
						},
					},
					{
						Name:      "active",
						Usage:     "List stream messages (pending or processing)",
						ArgsUsage: "<queue> [limit]",
						Action: func(ctx context.Context, cmd *cli.Command) error {
							if cmd.NArg() < 1 {
								return cli.Exit("Error: Missing queue name. Usage: taskmq-cli task active <queue> [limit]", 1)
							}
							queue := cmd.Args().Get(0)
							limit := 20
							if cmd.NArg() >= 2 {
								if lim, err := strconv.Atoi(cmd.Args().Get(1)); err == nil && lim > 0 {
									limit = lim
								}
							}
							if err := initRedis(cmd); err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							list, err := active.ListActiveTasks(ctx, queue, limit)
							if err != nil {
								return cli.Exit(fmt.Sprintf("Error: %v", err), 1)
							}
							if len(list) == 0 {
								fmt.Printf("No active/stream tasks for queue '%s'.\n", queue)
								return nil
							}
							w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
							fmt.Fprintln(w, "TASK ID\tNAME\tSTATUS\tSTREAM ID\tCONSUMER\tDELIVERIES")
							for _, at := range list {
								fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n",
									at.ID, at.Name, at.Status, at.StreamID, at.Consumer, at.Deliveries)
							}
							_ = w.Flush()
							return nil
						},
					},
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stdout, "%v\n", err)
		os.Exit(1)
	}
}

func handleStats(ctx context.Context, rdb redis.UniversalClient) {
	// Scan Redis to dynamically discover queues
	redisKeys, err := rdb.Keys(ctx, mqkeys.StreamScanPattern()).Result()
	if err != nil {
		fmt.Printf("Error: Failed to scan Redis for queues: %v\n", err)
		os.Exit(1)
	}

	// Extract unique queue names
	queuesMap := make(map[string]bool)
	for _, key := range redisKeys {
		if queueName, ok := mqkeys.ParseQueueFromStreamKey(key); ok {
			queuesMap[queueName] = true
		}
	}

	if len(queuesMap) == 0 {
		fmt.Println("No active TaskMQ queues found in Redis database.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
	fmt.Fprintln(w, "QUEUE NAME\tSTATUS\tACTIVE (STREAM)\tSCHEDULED (ZSET)\tDEAD LETTER (DLQ)")

	for q := range queuesMap {
		qk := mqkeys.KeysFor(q)

		// 1. Get Pause State
		isPaused, err := rdb.Exists(ctx, qk.Paused()).Result()
		status := "Active"
		if err == nil && isPaused > 0 {
			status = "Paused"
		}

		// 2. Get Active Count
		activeCount, _ := rdb.XLen(ctx, qk.Stream()).Result()

		// 3. Get Scheduled Count
		scheduledCount, _ := rdb.ZCard(ctx, qk.Delayed()).Result()

		// 4. Get DLQ Count
		dlqCount, _ := rdb.ZCard(ctx, qk.DLQ()).Result()

		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\n", q, status, activeCount, scheduledCount, dlqCount)
	}
	_ = w.Flush()
}
