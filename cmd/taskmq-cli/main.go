package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq"
)

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
	// 1. Setup global flags
	globalFlags := flag.NewFlagSet("global", flag.ExitOnError)
	redisAddr := globalFlags.String("redis-addr", getEnv("REDIS_ADDR", "localhost:6379"), "Redis server address")
	redisDB := globalFlags.Int("redis-db", getEnvInt("REDIS_DB", 0), "Redis database number")
	redisPassword := globalFlags.String("redis-password", getEnv("REDIS_PASSWORD", ""), "Redis password")

	// Find where global options end and commands begin
	cmdIdx := 1
	for i := 1; i < len(os.Args); i++ {
		if !strings.HasPrefix(os.Args[i], "-") {
			cmdIdx = i
			break
		}
	}

	// Parse global flags if any were supplied before the command
	if cmdIdx > 1 {
		_ = globalFlags.Parse(os.Args[1:cmdIdx])
	}

	// Ensure there is at least one command supplied
	if len(os.Args) <= cmdIdx {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[cmdIdx]
	cmdArgs := os.Args[cmdIdx+1:]

	// 2. Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		DB:       *redisDB,
		Password: *redisPassword,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("Error: Failed to connect to Redis at %s: %v\n", *redisAddr, err)
		os.Exit(1)
	}

	client := taskmq.NewClient(rdb)

	// 3. Dispatch commands
	switch command {
	case "stats":
		handleStats(ctx, rdb)
	case "pause":
		if len(cmdArgs) < 1 {
			fmt.Println("Error: Missing queue name. Usage: taskmq-cli pause <queue>")
			os.Exit(1)
		}
		queue := cmdArgs[0]
		if err := client.Pause(ctx, queue); err != nil {
			fmt.Printf("Error: Failed to pause queue %s: %v\n", queue, err)
			os.Exit(1)
		}
		fmt.Printf("Success: Queue '%s' paused successfully.\n", queue)

	case "resume":
		if len(cmdArgs) < 1 {
			fmt.Println("Error: Missing queue name. Usage: taskmq-cli resume <queue>")
			os.Exit(1)
		}
		queue := cmdArgs[0]
		if err := client.Resume(ctx, queue); err != nil {
			fmt.Printf("Error: Failed to resume queue %s: %v\n", queue, err)
			os.Exit(1)
		}
		fmt.Printf("Success: Queue '%s' resumed successfully.\n", queue)

	case "dlq":
		if len(cmdArgs) < 1 {
			fmt.Println("Error: Missing sub-command. Usage: taskmq-cli dlq [list|retry|delete]")
			os.Exit(1)
		}
		dlqSubCmd := cmdArgs[0]
		dlqArgs := cmdArgs[1:]

		switch dlqSubCmd {
		case "list":
			if len(dlqArgs) < 1 {
				fmt.Println("Error: Missing queue name. Usage: taskmq-cli dlq list <queue> [limit]")
				os.Exit(1)
			}
			queue := dlqArgs[0]
			limit := 20
			if len(dlqArgs) >= 2 {
				if lim, err := strconv.Atoi(dlqArgs[1]); err == nil && lim > 0 {
					limit = lim
				}
			}

			tasks, err := client.ListDeadLetters(ctx, queue, limit)
			if err != nil {
				fmt.Printf("Error: Failed to list DLQ for queue %s: %v\n", queue, err)
				os.Exit(1)
			}

			if len(tasks) == 0 {
				fmt.Printf("Queue '%s' dead-letter queue is empty.\n", queue)
				return
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "TASK ID\tTASK NAME\tRETRIES\tLAST ERROR")
			for _, t := range tasks {
				fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", t.ID, t.Name, t.Retry, t.LastError)
			}
			_ = w.Flush()

		case "retry":
			if len(dlqArgs) < 2 {
				fmt.Println("Error: Missing arguments. Usage: taskmq-cli dlq retry <queue> <id>")
				os.Exit(1)
			}
			queue := dlqArgs[0]
			taskID := dlqArgs[1]

			if err := client.RetryDeadLetter(ctx, queue, taskID); err != nil {
				fmt.Printf("Error: Failed to retry dead letter %s in queue %s: %v\n", taskID, queue, err)
				os.Exit(1)
			}
			fmt.Printf("Success: Task '%s' in queue '%s' successfully re-enqueued for retry.\n", taskID, queue)

		case "delete":
			if len(dlqArgs) < 2 {
				fmt.Println("Error: Missing arguments. Usage: taskmq-cli dlq delete <queue> <id>")
				os.Exit(1)
			}
			queue := dlqArgs[0]
			taskID := dlqArgs[1]

			if err := client.DeleteDeadLetter(ctx, queue, taskID); err != nil {
				fmt.Printf("Error: Failed to delete dead letter %s in queue %s: %v\n", taskID, queue, err)
				os.Exit(1)
			}
			fmt.Printf("Success: Task '%s' deleted from DLQ in queue '%s'.\n", taskID, queue)

		default:
			fmt.Printf("Error: Unknown dlq sub-command '%s'. Supported: list, retry, delete\n", dlqSubCmd)
			os.Exit(1)
		}

	default:
		fmt.Printf("Error: Unknown command '%s'\n", command)
		printUsage()
		os.Exit(1)
	}
}

func handleStats(ctx context.Context, rdb *redis.Client) {
	// Scan Redis to dynamically discover queues
	keys, err := rdb.Keys(ctx, "taskmq:{*}:queue").Result()
	if err != nil {
		fmt.Printf("Error: Failed to scan Redis for queues: %v\n", err)
		os.Exit(1)
	}

	// Extract unique queue names
	queuesMap := make(map[string]bool)
	for _, key := range keys {
		// key is in format: taskmq:{myqueue}:queue
		start := len("taskmq:{")
		end := strings.Index(key, "}:queue")
		if start < len(key) && end > start {
			queueName := key[start:end]
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
		pausedKey := taskmq.PausedKey(q)
		streamKey := taskmq.StreamKey(q)
		delayedKey := taskmq.DelayedKey(q)
		dlqKey := taskmq.DLQKey(q)

		// 1. Get Pause State
		isPaused, err := rdb.Exists(ctx, pausedKey).Result()
		status := "Active"
		if err == nil && isPaused > 0 {
			status = "Paused"
		}

		// 2. Get Active Count
		activeCount, _ := rdb.XLen(ctx, streamKey).Result()

		// 3. Get Scheduled Count
		scheduledCount, _ := rdb.ZCard(ctx, delayedKey).Result()

		// 4. Get DLQ Count
		dlqCount, _ := rdb.ZCard(ctx, dlqKey).Result()

		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\n", q, status, activeCount, scheduledCount, dlqCount)
	}
	_ = w.Flush()
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok {
		if idx, err := strconv.Atoi(val); err == nil {
			return idx
		}
	}
	return fallback
}
