package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/twn39/taskmq/internal/taskmq/worker"
)

func TestWorkerOptions_Validation(t *testing.T) {
	// Concurrency <= 0 check
	optConcurrency := worker.WithConcurrency(-5)
	assert.NotNil(t, optConcurrency)

	// Empty group check
	optGroup := worker.WithGroup("")
	assert.NotNil(t, optGroup)

	// Empty consumer check
	optConsumer := worker.WithConsumer("")
	assert.NotNil(t, optConsumer)
}

func TestPriorityOptions_Validation(t *testing.T) {
	// Empty priority queues error
	optQueues := worker.WithPriorityQueues(nil)
	assert.NotNil(t, optQueues)

	// Invalid priority strategy
	optStrategy := worker.WithPriorityStrategy("invalid-strategy")
	assert.NotNil(t, optStrategy)
}
