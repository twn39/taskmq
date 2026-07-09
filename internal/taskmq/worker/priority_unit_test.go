package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortQueues(t *testing.T) {
	queues := []QueuePriority{
		{Name: "low", Weight: 1},
		{Name: "high", Weight: 10},
		{Name: "medium", Weight: 5},
	}

	sorted := sortQueues(queues)
	expected := []string{"high", "medium", "low"}
	assert.Equal(t, expected, sorted)
}

func TestShuffleQueues(t *testing.T) {
	queues := []QueuePriority{
		{Name: "low", Weight: 1},
		{Name: "high", Weight: 9},
	}

	// Run shuffle multiple times to ensure we get both distributions, and count occurrences
	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		shuffled := shuffleQueues(queues)
		assert.Len(t, shuffled, 2)
		assert.Contains(t, shuffled, "low")
		assert.Contains(t, shuffled, "high")
		counts[shuffled[0]]++
	}

	// With weights 9 vs 1, "high" should be chosen first significantly more times than "low"
	t.Logf("First queue chosen: high=%d, low=%d", counts["high"], counts["low"])
	assert.Greater(t, counts["high"], counts["low"])
	assert.Greater(t, counts["low"], 0, "Low priority queue should be chosen first at least once in 1000 shuffles")
}

func TestShuffleQueues_ZeroWeights(t *testing.T) {
	queues := []QueuePriority{
		{Name: "q1", Weight: 0},
		{Name: "q2", Weight: -5},
	}

	// Should fallback to default weight of 1 and run successfully without division by zero or panic
	shuffled := shuffleQueues(queues)
	assert.Len(t, shuffled, 2)
	assert.Contains(t, shuffled, "q1")
	assert.Contains(t, shuffled, "q2")
}
