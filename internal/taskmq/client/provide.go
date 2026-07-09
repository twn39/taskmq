package client

import "go.uber.org/fx"

// ProvideISP projects Client onto narrow ISP ports for Fx graphs.
// Include this module (or the same Provide funcs) whenever constructors
// depend on EnqueueClient / AdminClient / CronClient / etc.
var ProvideISP = fx.Options(
	fx.Provide(
		func(c Client) EnqueueClient { return c },
		func(c Client) AdminClient { return c },
		func(c Client) CronClient { return c },
		func(c Client) DLQManager { return c },
		func(c Client) QueueController { return c },
		func(c Client) TaskCanceler { return c },
		func(c Client) ScheduledTaskManager { return c },
		func(c Client) ActiveTaskManager { return c },
	),
)
