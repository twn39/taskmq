package lifecycle

import (
	"context"
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/enqueue_stream.lua
var enqueueStreamScript string

//go:embed scripts/enqueue_unique_with_limit.lua
var enqueueUniqueWithLimitScript string

//go:embed scripts/enqueue_delayed_with_limit.lua
var enqueueDelayedWithLimitScript string

//go:embed scripts/enqueue_unique_delayed_with_limit.lua
var enqueueUniqueDelayedWithLimitScript string

//go:embed scripts/force_promote_member.lua
var forcePromoteMemberScript string

// Redis scripts for capacity-aware admission (owned by lifecycle).
var (
	EnqueueStreamCmd             = redis.NewScript(enqueueStreamScript)
	EnqueueUniqueWithLimitCmd    = redis.NewScript(enqueueUniqueWithLimitScript)
	EnqueueDelayedWithLimitCmd   = redis.NewScript(enqueueDelayedWithLimitScript)
	EnqueueUniqueDelayedLimitCmd = redis.NewScript(enqueueUniqueDelayedWithLimitScript)
	ForcePromoteMemberCmd        = redis.NewScript(forcePromoteMemberScript)
)

// LoadScripts pre-loads all admission Lua scripts into Redis using SCRIPT LOAD.
// Essential before executing scripts inside Redis Pipelines (rdb.Pipeline()) to prevent NOSCRIPT errors.
func LoadScripts(ctx context.Context, rdb redis.UniversalClient) error {
	if rdb == nil {
		return nil
	}
	scripts := []*redis.Script{
		EnqueueStreamCmd,
		EnqueueUniqueWithLimitCmd,
		EnqueueDelayedWithLimitCmd,
		EnqueueUniqueDelayedLimitCmd,
		ForcePromoteMemberCmd,
	}
	for _, s := range scripts {
		if err := s.Load(ctx, rdb).Err(); err != nil {
			return err
		}
	}
	return nil
}
