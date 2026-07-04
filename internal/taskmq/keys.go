package taskmq

import (
	"fmt"
)

func StreamKey(queue string) string {
	return fmt.Sprintf("taskmq:{%s}:queue", queue)
}

func DelayedKey(queue string) string {
	return fmt.Sprintf("taskmq:{%s}:delayed", queue)
}

func DLQKey(queue string) string {
	return fmt.Sprintf("taskmq:{%s}:dlq", queue)
}

func UniqueKey(queue, uniqueKey string) string {
	return fmt.Sprintf("taskmq:{%s}:unique:%s", queue, uniqueKey)
}

func CronConfigsKey(queue string) string {
	return fmt.Sprintf("taskmq:{%s}:cron_configs", queue)
}
