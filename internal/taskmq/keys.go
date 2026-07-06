package taskmq

import (
	"fmt"
)

type QueueKeys struct {
	queue string
}

// KeysFor returns a typed QueueKeys generator for the specified queue name.
func KeysFor(queue string) QueueKeys {
	return QueueKeys{queue: queue}
}

func (k QueueKeys) Stream() string {
	return fmt.Sprintf("taskmq:{%s}:queue", k.queue)
}

func (k QueueKeys) Delayed() string {
	return fmt.Sprintf("taskmq:{%s}:delayed", k.queue)
}

func (k QueueKeys) DLQ() string {
	return fmt.Sprintf("taskmq:{%s}:dlq", k.queue)
}

func (k QueueKeys) DLQIndex() string {
	return fmt.Sprintf("taskmq:{%s}:dlq_index", k.queue)
}

func (k QueueKeys) Unique(uniqueKey string) string {
	return fmt.Sprintf("taskmq:{%s}:unique:%s", k.queue, uniqueKey)
}

func (k QueueKeys) CronConfigs() string {
	return fmt.Sprintf("taskmq:{%s}:cron_configs", k.queue)
}

func (k QueueKeys) Paused() string {
	return fmt.Sprintf("taskmq:{%s}:paused", k.queue)
}

func (k QueueKeys) Control() string {
	return fmt.Sprintf("taskmq:{%s}:control", k.queue)
}

func (k QueueKeys) Cancelled(taskID string) string {
	return fmt.Sprintf("taskmq:{%s}:cancelled:%s", k.queue, taskID)
}

func (k QueueKeys) CancelChannel() string {
	return fmt.Sprintf("taskmq:{%s}:cancel", k.queue)
}

func (k QueueKeys) DelayedWakeupChannel() string {
	return fmt.Sprintf("taskmq:{%s}:delayed_wakeup", k.queue)
}

// Compatibility layer for older global functions to prevent breaking external dependencies and tests

func StreamKey(queue string) string {
	return KeysFor(queue).Stream()
}

func DelayedKey(queue string) string {
	return KeysFor(queue).Delayed()
}

func DLQKey(queue string) string {
	return KeysFor(queue).DLQ()
}

func DLQIndexKey(queue string) string {
	return KeysFor(queue).DLQIndex()
}

func UniqueKey(queue, uniqueKey string) string {
	return KeysFor(queue).Unique(uniqueKey)
}

func CronConfigsKey(queue string) string {
	return KeysFor(queue).CronConfigs()
}

func PausedKey(queue string) string {
	return KeysFor(queue).Paused()
}

func ControlChannel(queue string) string {
	return KeysFor(queue).Control()
}

func CancelledKey(queue, taskID string) string {
	return KeysFor(queue).Cancelled(taskID)
}

func CancelChannel(queue string) string {
	return KeysFor(queue).CancelChannel()
}

func DelayedWakeupChannel(queue string) string {
	return KeysFor(queue).DelayedWakeupChannel()
}
