package taskmq

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func NewClient(rdb *redis.Client) *Client {
	return &Client{rdb: rdb}
}

// generateUUID generates a lightweight pseudo-random UUID-v4-like string
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Enqueue adds a task to the Redis stream
func (c *Client) Enqueue(ctx context.Context, task *Task) error {
	if task.ID == "" {
		task.ID = generateUUID()
	}

	serialized, err := task.Serialize()
	if err != nil {
		return err
	}

	streamKey := fmt.Sprintf("taskmq:queue:%s", task.Queue)
	err = c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"task": serialized,
		},
	}).Err()

	return err
}
