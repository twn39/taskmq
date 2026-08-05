package grpcserver

import (
	"context"
	"time"

	taskmqv1 "github.com/twn39/taskmq/api/proto/taskmq/v1"
	"github.com/twn39/taskmq/internal/taskmq/client"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// API is the ISP surface required by the gRPC service.
// Matches proto TaskMQService: enqueue + cron + DLQ + ops/inspect.
// Metrics scrape and dashboard remain on HTTP admin — see docs/OPERATIONS.md.
type API interface {
	client.EnqueueClient
	client.CronClient
	client.DLQManager
	client.QueueController
	client.TaskCanceler
	client.TaskInspector
	client.ScheduledTaskManager
	client.ActiveTaskManager
}

// GRPCServer implements the TaskMQ gRPC service.
type GRPCServer struct {
	taskmqv1.UnimplementedTaskMQServiceServer
	api    API
	logger *zap.Logger
}

// NewGRPCServer creates a gRPC adapter over client ISP ports.
// client.Client satisfies API via embedding.
func NewGRPCServer(api API, logger *zap.Logger) *GRPCServer {
	return &GRPCServer{
		api:    api,
		logger: logger,
	}
}

func toTaskmqTask(t *taskmqv1.Task) *taskmodel.Task {
	if t == nil {
		return nil
	}
	task := taskmodel.NewTask(t.Name, t.Payload, taskmodel.TaskOptions{
		ID:        t.Id,
		Queue:     t.Queue,
		MaxRetry:  taskmodel.Ptr(int(t.MaxRetry)),
		Timeout:   time.Duration(t.TimeoutMs) * time.Millisecond,
		UniqueKey: t.UniqueKey,
		UniqueTTL: time.Duration(t.UniqueTtlMs) * time.Millisecond,
	})
	task.CronSpec = t.CronSpec
	return task
}

func toProtoTask(t *taskmodel.Task) *taskmqv1.Task {
	if t == nil {
		return nil
	}
	return &taskmqv1.Task{
		Id:          t.ID,
		Queue:       t.Queue,
		Name:        t.Name,
		Payload:     t.Payload,
		MaxRetry:    int32(t.MaxRetry),
		TimeoutMs:   int64(t.TimeoutMs),
		UniqueKey:   t.UniqueKey,
		UniqueTtlMs: int64(t.UniqueTTLMs),
		CronSpec:    t.CronSpec,
	}
}

func (s *GRPCServer) Enqueue(ctx context.Context, req *taskmqv1.EnqueueRequest) (*taskmqv1.EnqueueResponse, error) {
	task := toTaskmqTask(req.GetTask())
	if task == nil {
		return nil, status.Error(codes.InvalidArgument, "task cannot be nil")
	}

	err := s.api.Enqueue(ctx, task)
	if err != nil {
		s.logger.Error("gRPC Enqueue failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to enqueue task: %v", err)
	}

	return &taskmqv1.EnqueueResponse{TaskId: task.ID}, nil
}

func (s *GRPCServer) EnqueueIn(ctx context.Context, req *taskmqv1.EnqueueInRequest) (*taskmqv1.EnqueueResponse, error) {
	task := toTaskmqTask(req.GetTask())
	if task == nil {
		return nil, status.Error(codes.InvalidArgument, "task cannot be nil")
	}

	delay := time.Duration(req.GetDelayMs()) * time.Millisecond
	err := s.api.EnqueueIn(ctx, task, delay)
	if err != nil {
		s.logger.Error("gRPC EnqueueIn failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to enqueue delayed task: %v", err)
	}

	return &taskmqv1.EnqueueResponse{TaskId: task.ID}, nil
}

func (s *GRPCServer) EnqueueAt(ctx context.Context, req *taskmqv1.EnqueueAtRequest) (*taskmqv1.EnqueueResponse, error) {
	task := toTaskmqTask(req.GetTask())
	if task == nil {
		return nil, status.Error(codes.InvalidArgument, "task cannot be nil")
	}

	at := time.UnixMilli(req.GetTimestampMs())
	err := s.api.EnqueueAt(ctx, task, at)
	if err != nil {
		s.logger.Error("gRPC EnqueueAt failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to enqueue task at timestamp: %v", err)
	}

	return &taskmqv1.EnqueueResponse{TaskId: task.ID}, nil
}

func (s *GRPCServer) EnqueueBulk(ctx context.Context, req *taskmqv1.EnqueueBulkRequest) (*taskmqv1.EnqueueBulkResponse, error) {
	protoTasks := req.GetTasks()
	if len(protoTasks) == 0 {
		return &taskmqv1.EnqueueBulkResponse{}, nil
	}
	tasks := make([]*taskmodel.Task, 0, len(protoTasks))
	for _, pt := range protoTasks {
		t := toTaskmqTask(pt)
		if t == nil {
			return nil, status.Error(codes.InvalidArgument, "bulk contains nil task")
		}
		tasks = append(tasks, t)
	}
	res, err := s.api.EnqueueBulk(ctx, tasks, client.WithBulkFailFast(req.GetFailFast()))
	out := &taskmqv1.EnqueueBulkResponse{}
	if res != nil {
		out.TaskIds = res.Succeeded
		for _, idx := range res.FailedIndexes {
			out.FailedIndexes = append(out.FailedIndexes, int32(idx))
		}
	}
	if err != nil && (res == nil || len(res.Succeeded) == 0) {
		s.logger.Error("gRPC EnqueueBulk failed", zap.Error(err))
		return out, status.Errorf(codes.Internal, "bulk enqueue failed: %v", err)
	}
	// Partial success returns OK with failed_indexes populated.
	return out, nil
}

func (s *GRPCServer) RegisterCron(ctx context.Context, req *taskmqv1.RegisterCronRequest) (*taskmqv1.RegisterCronResponse, error) {
	task := toTaskmqTask(req.GetTask())
	if task == nil {
		return nil, status.Error(codes.InvalidArgument, "task cannot be nil")
	}

	err := s.api.RegisterCron(ctx, req.GetJobName(), req.GetCronSpec(), task)
	if err != nil {
		s.logger.Error("gRPC RegisterCron failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to register cron task: %v", err)
	}

	return &taskmqv1.RegisterCronResponse{Success: true}, nil
}

func (s *GRPCServer) ListDeadLetters(ctx context.Context, req *taskmqv1.ListDeadLettersRequest) (*taskmqv1.ListDeadLettersResponse, error) {
	tasks, err := s.api.ListDeadLetters(ctx, req.GetQueue(), int(req.GetLimit()))
	if err != nil {
		s.logger.Error("gRPC ListDeadLetters failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list dead letters: %v", err)
	}

	protoTasks := make([]*taskmqv1.Task, 0, len(tasks))
	for _, t := range tasks {
		protoTasks = append(protoTasks, toProtoTask(t))
	}

	return &taskmqv1.ListDeadLettersResponse{Tasks: protoTasks}, nil
}

func (s *GRPCServer) DeleteDeadLetter(ctx context.Context, req *taskmqv1.DeleteDeadLetterRequest) (*taskmqv1.DeleteDeadLetterResponse, error) {
	err := s.api.DeleteDeadLetter(ctx, req.GetQueue(), req.GetTaskId())
	if err != nil {
		s.logger.Error("gRPC DeleteDeadLetter failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to delete dead letter: %v", err)
	}

	return &taskmqv1.DeleteDeadLetterResponse{Success: true}, nil
}

func (s *GRPCServer) RetryDeadLetter(ctx context.Context, req *taskmqv1.RetryDeadLetterRequest) (*taskmqv1.RetryDeadLetterResponse, error) {
	err := s.api.RetryDeadLetter(ctx, req.GetQueue(), req.GetTaskId())
	if err != nil {
		s.logger.Error("gRPC RetryDeadLetter failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to retry dead letter: %v", err)
	}

	return &taskmqv1.RetryDeadLetterResponse{Success: true}, nil
}

// --- Ops / inspect ---

func (s *GRPCServer) PauseQueue(ctx context.Context, req *taskmqv1.PauseQueueRequest) (*taskmqv1.PauseQueueResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	if err := s.api.Pause(ctx, req.GetQueue()); err != nil {
		s.logger.Error("gRPC PauseQueue failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to pause queue: %v", err)
	}
	return &taskmqv1.PauseQueueResponse{Success: true}, nil
}

func (s *GRPCServer) ResumeQueue(ctx context.Context, req *taskmqv1.ResumeQueueRequest) (*taskmqv1.ResumeQueueResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	if err := s.api.Resume(ctx, req.GetQueue()); err != nil {
		s.logger.Error("gRPC ResumeQueue failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to resume queue: %v", err)
	}
	return &taskmqv1.ResumeQueueResponse{Success: true}, nil
}

func (s *GRPCServer) IsQueuePaused(ctx context.Context, req *taskmqv1.IsQueuePausedRequest) (*taskmqv1.IsQueuePausedResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	paused, err := s.api.IsPaused(ctx, req.GetQueue())
	if err != nil {
		s.logger.Error("gRPC IsQueuePaused failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to check pause state: %v", err)
	}
	return &taskmqv1.IsQueuePausedResponse{Paused: paused}, nil
}

func (s *GRPCServer) CancelTask(ctx context.Context, req *taskmqv1.CancelTaskRequest) (*taskmqv1.CancelTaskResponse, error) {
	if req.GetQueue() == "" || req.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue and task_id are required")
	}
	if err := s.api.CancelTask(ctx, req.GetQueue(), req.GetTaskId()); err != nil {
		s.logger.Error("gRPC CancelTask failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to cancel task: %v", err)
	}
	return &taskmqv1.CancelTaskResponse{Success: true}, nil
}

func (s *GRPCServer) GetTask(ctx context.Context, req *taskmqv1.GetTaskRequest) (*taskmqv1.GetTaskResponse, error) {
	if req.GetQueue() == "" || req.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue and task_id are required")
	}
	info, err := s.api.GetTaskInfo(ctx, req.GetQueue(), req.GetTaskId())
	if err != nil {
		s.logger.Error("gRPC GetTask failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get task: %v", err)
	}
	if info == nil {
		return &taskmqv1.GetTaskResponse{Found: false}, nil
	}
	return &taskmqv1.GetTaskResponse{Found: true, Task: toProtoTaskInfo(info)}, nil
}

func (s *GRPCServer) ListScheduledTasks(ctx context.Context, req *taskmqv1.ListScheduledTasksRequest) (*taskmqv1.ListScheduledTasksResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 20
	}
	list, err := s.api.ListScheduledTasks(ctx, req.GetQueue(), limit)
	if err != nil {
		s.logger.Error("gRPC ListScheduledTasks failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list scheduled tasks: %v", err)
	}
	out := &taskmqv1.ListScheduledTasksResponse{}
	for _, st := range list {
		out.Tasks = append(out.Tasks, &taskmqv1.ScheduledTask{
			Task:    toProtoTask(st.Task),
			RunAtMs: st.RunAt.UnixMilli(),
		})
	}
	return out, nil
}

func (s *GRPCServer) ListActiveTasks(ctx context.Context, req *taskmqv1.ListActiveTasksRequest) (*taskmqv1.ListActiveTasksResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 20
	}
	list, err := s.api.ListActiveTasks(ctx, req.GetQueue(), limit)
	if err != nil {
		s.logger.Error("gRPC ListActiveTasks failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list active tasks: %v", err)
	}
	out := &taskmqv1.ListActiveTasksResponse{}
	for _, at := range list {
		item := &taskmqv1.ActiveTask{
			Task:       toProtoTask(at.Task),
			StreamId:   at.StreamID,
			Status:     at.Status,
			Consumer:   at.Consumer,
			Deliveries: at.Deliveries,
		}
		if !at.EnqueuedAt.IsZero() {
			item.EnqueuedAtMs = at.EnqueuedAt.UnixMilli()
		}
		out.Tasks = append(out.Tasks, item)
	}
	return out, nil
}

func toProtoTaskInfo(info *client.TaskInfoView) *taskmqv1.TaskInfo {
	if info == nil {
		return nil
	}
	ti := &taskmqv1.TaskInfo{
		Id:           info.ID,
		Queue:        info.Queue,
		Name:         info.Name,
		State:        info.State,
		Retry:        int32(info.Retry),
		MaxRetry:     int32(info.MaxRetry),
		LastError:    info.LastError,
		TimeoutMs:    int64(info.TimeoutMs),
		DeadlineMs:   info.DeadlineMs,
		UniqueKey:    info.UniqueKey,
		GroupKey:     info.GroupKey,
		StreamId:     info.StreamID,
		Result:       info.Result,
		Progress:     int32(info.Progress),
		ProgressData: info.ProgressData,
	}
	if !info.CreatedAt.IsZero() {
		ti.CreatedAtMs = info.CreatedAt.UnixMilli()
	}
	if !info.UpdatedAt.IsZero() {
		ti.UpdatedAtMs = info.UpdatedAt.UnixMilli()
	}
	if !info.CompletedAt.IsZero() {
		ti.CompletedAtMs = info.CompletedAt.UnixMilli()
	}
	return ti
}
