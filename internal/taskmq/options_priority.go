package taskmq

import (
	"errors"
	"fmt"
)

type PriorityWorkerOptions struct {
	BaseWorkerOptions
	priorityQueues   []QueuePriority
	priorityStrategy string
}

type PriorityWorkerOption interface {
	ApplyPriorityWorker(*PriorityWorkerOptions) error
}

type priorityOption func(*PriorityWorkerOptions) error

func (o priorityOption) ApplyPriorityWorker(opts *PriorityWorkerOptions) error {
	return o(opts)
}

func defaultPriorityWorkerOptions(codec Codec) PriorityWorkerOptions {
	return PriorityWorkerOptions{
		BaseWorkerOptions: defaultBaseWorkerOptions(codec),
	}
}

func WithPriorityQueues(queues []QueuePriority) priorityOption {
	return priorityOption(func(o *PriorityWorkerOptions) error {
		if len(queues) == 0 {
			return errors.New("priority queues cannot be empty")
		}
		o.priorityQueues = queues
		return nil
	})
}

func WithPriorityStrategy(strategy string) priorityOption {
	return priorityOption(func(o *PriorityWorkerOptions) error {
		if strategy != "strict" && strategy != "weighted" && strategy != "" {
			return fmt.Errorf("invalid priority strategy: %s (must be 'strict' or 'weighted')", strategy)
		}
		o.priorityStrategy = strategy
		return nil
	})
}
