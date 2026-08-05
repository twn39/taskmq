package client

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
)

// deps is the shared Redis/codec/lifecycle kernel used by all client services.
type deps struct {
	rdb              *redis.Client
	codec            codec.Codec
	defaultUniqueTTL time.Duration
	lifecycle        *lifecycle.Lifecycle
}

func (d deps) hardLimit() int64 {
	if d.lifecycle == nil {
		return 0
	}
	return d.lifecycle.Config().EnqueueHardLimit
}

func (d deps) streamMaxLen() int64 {
	if d.lifecycle == nil {
		return 0
	}
	return d.lifecycle.Config().StreamMaxLen
}

func (d deps) delayedMaxCount() int64 {
	if d.lifecycle == nil {
		return 0
	}
	return d.lifecycle.Config().DelayedMaxCount
}

func (d deps) uniqueTTL(task *taskmodel.Task) time.Duration {
	ttl := time.Duration(task.UniqueTTLMs) * time.Millisecond
	if ttl <= 0 {
		if d.defaultUniqueTTL > 0 {
			ttl = d.defaultUniqueTTL
		} else {
			ttl = 1 * time.Hour
		}
	}
	return ttl
}

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
