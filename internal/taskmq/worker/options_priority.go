package worker

import (
	"errors"
	"fmt"

	"github.com/twn39/taskmq/internal/taskmq/codec"
)

// PriorityWorkerOptions is WorkerConfig plus multi-queue priority settings.
type PriorityWorkerOptions struct {
	WorkerConfig
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

func defaultPriorityWorkerOptions(codec codec.Codec) PriorityWorkerOptions {
	return PriorityWorkerOptions{
		WorkerConfig: defaultWorkerConfig(codec),
	}
}

// applyPriorityWorkerOptions fills PriorityWorkerOptions from mixed option list.
func applyPriorityWorkerOptions(codec codec.Codec, opts []PriorityWorkerOption) (PriorityWorkerOptions, error) {
	cfg := defaultPriorityWorkerOptions(codec)
	for _, o := range opts {
		if o == nil {
			continue
		}
		if err := o.ApplyPriorityWorker(&cfg); err != nil {
			return PriorityWorkerOptions{}, err
		}
	}
	return cfg, nil
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
