package client

import (
	"github.com/redis/go-redis/v9"
	"github.com/twn39/taskmq/internal/taskmq/codec"
	"github.com/twn39/taskmq/internal/taskmq/events"
	"github.com/twn39/taskmq/internal/taskmq/lifecycle"
	"github.com/twn39/taskmq/internal/taskmq/meta"
)

// admin composes operational ports into AdminClient.
type admin struct {
	DLQManager
	QueueController
	ScheduledTaskManager
	ActiveTaskManager
	TaskCanceler
	TaskInspector
	EventReader
	WorkerViewer
}

// facade is the convenience Client implementation via interface embedding.
type facade struct {
	EnqueueClient
	CronClient
	AdminClient
	d deps
}

// Lifecycle returns the attached lifecycle (may be nil).
// Not part of Client interface; used by tests/tools via type assertion when needed.
func (f *facade) Lifecycle() *lifecycle.Lifecycle {
	return f.d.lifecycle
}

// NewClient creates a TaskMQ client facade combining enqueue, cron, and admin ports.
func NewClient(rdb redis.UniversalClient, opts ...ClientOption) Client {
	d := deps{
		rdb:   rdb,
		codec: codec.JSONCodec{},
		meta:  meta.NewStore(rdb),
	}
	for _, opt := range opts {
		opt(&d)
	}
	if d.meta == nil {
		d.meta = meta.NewStore(rdb)
	}
	if d.events == nil {
		d.events = events.NewPublisher(rdb, d.eventsMaxLen)
	}

	enq := &enqueuer{d: d}
	ctl := &control{d: d}
	dlqSvc := &dlq{d: d, enq: enq}
	sch := &scheduled{d: d}
	act := &active{d: d, cancel: ctl}
	crn := &cronService{d: d}
	insp := &inspector{d: d}

	adm := &admin{
		DLQManager:           dlqSvc,
		QueueController:      ctl,
		ScheduledTaskManager: sch,
		ActiveTaskManager:    act,
		TaskCanceler:         ctl,
		TaskInspector:        insp,
		EventReader:          insp,
		WorkerViewer:         insp,
	}

	f := &facade{
		EnqueueClient: enq,
		CronClient:    crn,
		AdminClient:   adm,
		d:             d,
	}

	// Compile-time interface checks.
	var (
		_ Client               = f
		_ EnqueueClient        = enq
		_ CronClient           = crn
		_ AdminClient          = adm
		_ DLQManager           = dlqSvc
		_ QueueController      = ctl
		_ TaskCanceler         = ctl
		_ ScheduledTaskManager = sch
		_ ActiveTaskManager    = act
		_ TaskInspector        = insp
		_ EventReader          = insp
		_ WorkerViewer         = insp
	)
	return f
}
