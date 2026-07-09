package worker

import "context"

// MultiQueueWorker defines a worker pool coordinator that multiplexes multiple queue worker instances.
type MultiQueueWorker interface {
	Worker
	Queue(name string) Worker
}

// ExecutionPool defines the concurrency execution limit control and monitoring contract.
type ExecutionPool interface {
	Acquire(ctx context.Context) error
	Release()
	Size() int
	InUse() int
}
