package client

import (
	"context"

	"github.com/twn39/taskmq/internal/taskmq/events"
	"github.com/twn39/taskmq/internal/taskmq/heartbeat"
	"github.com/twn39/taskmq/internal/taskmq/meta"
)

// inspector implements TaskInspector, EventReader, WorkerViewer.
type inspector struct {
	d deps
}

// GetTaskInfo returns durable task metadata, or nil if not found.
func (i *inspector) GetTaskInfo(ctx context.Context, queue, taskID string) (*TaskInfoView, error) {
	if i.d.meta == nil {
		i.d.meta = meta.NewStore(i.d.rdb)
	}
	info, err := i.d.meta.Get(ctx, queue, taskID)
	if err != nil || info == nil {
		return nil, err
	}
	return &TaskInfoView{
		ID:           info.ID,
		Queue:        info.Queue,
		Name:         info.Name,
		State:        info.State,
		Retry:        info.Retry,
		MaxRetry:     info.MaxRetry,
		LastError:    info.LastError,
		TimeoutMs:    info.TimeoutMs,
		DeadlineMs:   info.DeadlineMs,
		UniqueKey:    info.UniqueKey,
		GroupKey:     info.GroupKey,
		StreamID:     info.StreamID,
		Result:       info.Result,
		Progress:     info.Progress,
		ProgressData: info.ProgressData,
		CreatedAt:    info.CreatedAt,
		UpdatedAt:    info.UpdatedAt,
		CompletedAt:  info.CompletedAt,
	}, nil
}

// UpdateProgress writes progress to meta and emits a progress event.
func (i *inspector) UpdateProgress(ctx context.Context, queue, taskID string, percent int, data string) error {
	if i.d.meta == nil {
		i.d.meta = meta.NewStore(i.d.rdb)
	}
	if err := i.d.meta.SetProgress(ctx, queue, taskID, percent, data); err != nil {
		return err
	}
	if i.d.events != nil {
		_ = i.d.events.Publish(ctx, events.Event{
			Type:     events.TypeProgress,
			Queue:    queue,
			TaskID:   taskID,
			Progress: percent,
			Data:     data,
		})
	}
	return nil
}

// ListEvents returns recent lifecycle events for a queue.
func (i *inspector) ListEvents(ctx context.Context, queue string, limit int64) ([]EventView, error) {
	evs, err := events.ListRecent(ctx, i.d.rdb, queue, limit)
	if err != nil {
		return nil, err
	}
	out := make([]EventView, 0, len(evs))
	for _, e := range evs {
		out = append(out, EventView{
			ID:          e.ID,
			Type:        e.Type,
			Queue:       e.Queue,
			TaskID:      e.TaskID,
			Name:        e.Name,
			Error:       e.Error,
			Progress:    e.Progress,
			Data:        e.Data,
			TimestampMs: e.TimestampMs,
		})
	}
	return out, nil
}

// ListWorkers returns live consumer heartbeats for a queue.
func (i *inspector) ListWorkers(ctx context.Context, queue string) ([]WorkerView, error) {
	list, err := heartbeat.List(ctx, i.d.rdb, queue)
	if err != nil {
		return nil, err
	}
	out := make([]WorkerView, 0, len(list))
	for _, w := range list {
		out = append(out, WorkerView{
			Queue:       w.Queue,
			Consumer:    w.Consumer,
			Host:        w.Host,
			PID:         w.PID,
			Concurrency: w.Concurrency,
			InUse:       w.InUse,
			ActiveTask:  w.ActiveTask,
			StartedAt:   w.StartedAt,
			UpdatedAt:   w.UpdatedAt,
		})
	}
	return out, nil
}
