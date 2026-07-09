package lifecycle

import (
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
