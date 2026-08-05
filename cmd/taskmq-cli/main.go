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
	"github.com/urfave/cli/v3"
)

var (
	rdb     *redis.Client
	queues  mqclient.QueueController
	dlq     mqclient.DLQManager
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
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stdout, "%v\n", err)
		os.Exit(1)
	}
}

func handleStats(ctx context.Context, rdb *redis.Client) {
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
