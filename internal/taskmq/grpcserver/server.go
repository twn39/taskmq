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

// API is the ISP-narrow surface required by the gRPC service.
type API interface {
	client.EnqueueClient
	client.CronClient
	client.DLQManager
}

// GRPCServer implements the TaskMQ gRPC service.
type GRPCServer struct {
	taskmqv1.UnimplementedTaskMQServiceServer
	api    API
	logger *zap.Logger
}

// NewGRPCServer creates a gRPC adapter over enqueue/cron/dlq ports.
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
