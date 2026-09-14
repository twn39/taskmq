package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"go.uber.org/zap"
)

// DaemonSet aggregates and manages multiple background Runners (delayed schedulers, janitors, cron managers).
// It runs all runners concurrently in background goroutines until context cancellation.
type DaemonSet interface {
	Runner
	Add(name string, r Runner)
	Runners() map[string]Runner
}

type daemonSet struct {
	mu      sync.RWMutex
	runners map[string]Runner
	logger  *zap.Logger
}

// NewDaemonSet creates a new empty DaemonSet.
func NewDaemonSet(logger *zap.Logger) DaemonSet {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &daemonSet{
		runners: make(map[string]Runner),
		logger:  logger,
	}
}

func (d *daemonSet) Add(name string, r Runner) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if r != nil {
		d.runners[name] = r
	}
}

func (d *daemonSet) Runners() map[string]Runner {
	d.mu.RLock()
	defer d.mu.RUnlock()
	res := make(map[string]Runner, len(d.runners))
	for k, v := range d.runners {
		res[k] = v
	}
	return res
}

func (d *daemonSet) Run(ctx context.Context) error {
	d.mu.RLock()
	runners := make(map[string]Runner, len(d.runners))
	for k, v := range d.runners {
		runners[k] = v
	}
	d.mu.RUnlock()

	if len(runners) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(runners))

	for name, r := range runners {
		rName := name
		runner := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Error("Daemon runner exited with error", zap.String("runner", rName), zap.Error(err))
				errCh <- fmt.Errorf("daemon %s: %w", rName, err)
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return ctx.Err()
}

// QueueDaemonConfig configures the daemons for a specific queue.
type QueueDaemonConfig struct {
	Queue             string
	Group             string
	Consumer          string
	Concurrency       int
	SchedulerInterval time.Duration
	JanitorInterval   time.Duration
	JanitorMinIdle    time.Duration
	CronInterval      time.Duration
	CronLockTTL       time.Duration
	DisableScheduler  bool
	DisableJanitor    bool
	DisableRetention  bool
	DisableCron       bool
}

// BuildQueueDaemons constructs standard maintenance daemons for a queue and adds them to ds.
func BuildQueueDaemons(
	ds DaemonSet,
	rdb redis.UniversalClient,
	logger *zap.Logger,
	c codec.Codec,
	lc *lifecycle.Lifecycle,
	cfg QueueDaemonConfig,
) {
	if cfg.Group == "" {
		cfg.Group = "taskmq-daemon-group"
	}
	if cfg.Consumer == "" {
		cfg.Consumer = "taskmq-daemon-consumer"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if cfg.SchedulerInterval <= 0 {
		cfg.SchedulerInterval = 500 * time.Millisecond
	}
	if cfg.JanitorInterval <= 0 {
		cfg.JanitorInterval = 3 * time.Second
	}
	if cfg.JanitorMinIdle <= 0 {
		cfg.JanitorMinIdle = 5 * time.Second
	}
	if cfg.CronInterval <= 0 {
		cfg.CronInterval = 1 * time.Minute
	}
	if cfg.CronLockTTL <= 0 {
		cfg.CronLockTTL = 50 * time.Second
	}

	var cronMgr CronManager
	if !cfg.DisableCron {
		cronMgr = DefaultCronManagerFactory(rdb, logger, cfg.Queue, c, cfg.CronInterval, cfg.CronLockTTL, 100, 1000, lc)
		ds.Add(fmt.Sprintf("%s:cron", cfg.Queue), cronMgr)
	}

	if !cfg.DisableScheduler {
		var hardLimit, maxLen int64
		if lc != nil {
			hardLimit, maxLen = lc.StreamLimits()
		}
		sched := DefaultSchedulerFactory(rdb, logger, cfg.Queue, cronMgr, c, cfg.SchedulerInterval, hardLimit, maxLen)
		ds.Add(fmt.Sprintf("%s:scheduler", cfg.Queue), sched)
	}

	if !cfg.DisableJanitor {
		jan := NewPELRecoveryJanitorWithCodec(rdb, logger, cfg.Queue, cfg.Group, cfg.Consumer, cfg.Concurrency, cfg.JanitorInterval, cfg.JanitorMinIdle, c, 0)
		ds.Add(fmt.Sprintf("%s:janitor", cfg.Queue), jan)
	}

	if !cfg.DisableRetention && lc != nil {
		ret := DefaultRetentionJanitorFactory(rdb, logger, cfg.Queue, cfg.Group, c, lc)
		ds.Add(fmt.Sprintf("%s:retention", cfg.Queue), ret)
	}
}
