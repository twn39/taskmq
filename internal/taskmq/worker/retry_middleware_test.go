package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twn39/taskmq/internal/taskmq/policy"
	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"
	"go.uber.org/zap"
)

func TestRetryAndDLQMiddleware_Outcomes(t *testing.T) {
	logger := zap.NewNop()
	bizErr := errors.New("handler failed")

	t.Run("retry success returns Handled and schedules", func(t *testing.T) {
		mb := &mockBroker{}
		rp := &mockRetryPolicy{should: true}
		dlq := policy.NewStandardDeadLetterPolicy("", nil)

		handlers := []CoreHandlerFunc{
			RetryAndDLQMiddleware(mb, rp, dlq, logger),
			func(c *ConsumeContext) error { return bizErr },
		}
		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &taskmodel.Task{ID: "t1", Queue: "q1", Name: "n", MaxRetry: 3},
			MessageID: "1-0",
			Queue:     "q1",
			Group:     "g",
			handlers:  handlers,
			index:     -1,
		}
		err := c.Next()
		if !IsHandled(err) {
			t.Fatalf("expected Handled, got %v", err)
		}
		if !errors.Is(err, bizErr) {
			t.Fatalf("expected unwrap to bizErr, got %v", err)
		}
		if mb.schedRetry != 1 {
			t.Fatalf("schedRetry=%d", mb.schedRetry)
		}
		if mb.moveToDLQCnt != 0 {
			t.Fatalf("moveToDLQCnt=%d", mb.moveToDLQCnt)
		}
		if c.Task.Retry != 1 {
			t.Fatalf("Retry=%d", c.Task.Retry)
		}
	})

	t.Run("no retry moves to DLQ as Handled", func(t *testing.T) {
		mb := &mockBroker{}
		rp := &mockRetryPolicy{should: false}
		dlq := policy.NewStandardDeadLetterPolicy("", nil)

		handlers := []CoreHandlerFunc{
			RetryAndDLQMiddleware(mb, rp, dlq, logger),
			func(c *ConsumeContext) error { return bizErr },
		}
		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &taskmodel.Task{ID: "t2", Queue: "q1", Name: "n", MaxRetry: 0, Retry: 0},
			MessageID: "2-0",
			Queue:     "q1",
			Group:     "g",
			handlers:  handlers,
			index:     -1,
		}
		err := c.Next()
		if !IsHandled(err) {
			t.Fatalf("expected Handled, got %v", err)
		}
		if mb.moveToDLQCnt != 1 {
			t.Fatalf("moveToDLQCnt=%d", mb.moveToDLQCnt)
		}
		if mb.schedRetry != 0 {
			t.Fatalf("schedRetry=%d", mb.schedRetry)
		}
	})

	t.Run("ErrNoHandler goes to DLQ as Handled", func(t *testing.T) {
		mb := &mockBroker{}
		rp := &mockRetryPolicy{should: true} // would retry, but no-handler forces DLQ
		dlq := policy.NewStandardDeadLetterPolicy("", nil)

		handlers := []CoreHandlerFunc{
			RetryAndDLQMiddleware(mb, rp, dlq, logger),
			func(c *ConsumeContext) error {
				return fmtNoHandler("missing")
			},
		}
		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &taskmodel.Task{ID: "t3", Queue: "q1", Name: "missing", MaxRetry: 5},
			MessageID: "3-0",
			Queue:     "q1",
			Group:     "g",
			handlers:  handlers,
			index:     -1,
		}
		err := c.Next()
		if !IsHandled(err) {
			t.Fatalf("expected Handled, got %v", err)
		}
		if !errors.Is(err, taskmodel.ErrNoHandler) {
			t.Fatalf("expected ErrNoHandler, got %v", err)
		}
		if mb.moveToDLQCnt != 1 {
			t.Fatalf("moveToDLQCnt=%d", mb.moveToDLQCnt)
		}
	})

	t.Run("broker MoveToDLQ failure is not Handled", func(t *testing.T) {
		mb := &mockBroker{moveErr: errors.New("redis down")}
		rp := &mockRetryPolicy{should: false}
		dlq := policy.NewStandardDeadLetterPolicy("", nil)

		handlers := []CoreHandlerFunc{
			RetryAndDLQMiddleware(mb, rp, dlq, logger),
			func(c *ConsumeContext) error { return bizErr },
		}
		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &taskmodel.Task{ID: "t4", Queue: "q1", Name: "n"},
			MessageID: "4-0",
			Queue:     "q1",
			Group:     "g",
			handlers:  handlers,
			index:     -1,
		}
		err := c.Next()
		if IsHandled(err) {
			t.Fatalf("settlement failure must not be Handled: %v", err)
		}
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("broker ScheduleRetry failure is not Handled", func(t *testing.T) {
		mb := &mockBroker{schedErr: errors.New("redis down")}
		rp := &mockRetryPolicy{should: true}
		dlq := policy.NewStandardDeadLetterPolicy("", nil)

		handlers := []CoreHandlerFunc{
			RetryAndDLQMiddleware(mb, rp, dlq, logger),
			func(c *ConsumeContext) error { return bizErr },
		}
		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &taskmodel.Task{ID: "t5", Queue: "q1", Name: "n", MaxRetry: 3},
			MessageID: "5-0",
			Queue:     "q1",
			Group:     "g",
			handlers:  handlers,
			index:     -1,
		}
		err := c.Next()
		if IsHandled(err) {
			t.Fatalf("settlement failure must not be Handled: %v", err)
		}
	})

	t.Run("success returns nil without broker failure paths", func(t *testing.T) {
		mb := &mockBroker{}
		rp := &mockRetryPolicy{should: true}
		dlq := policy.NewStandardDeadLetterPolicy("", nil)

		handlers := []CoreHandlerFunc{
			RetryAndDLQMiddleware(mb, rp, dlq, logger),
			func(c *ConsumeContext) error { return nil },
		}
		c := &ConsumeContext{
			Context:   context.Background(),
			Task:      &taskmodel.Task{ID: "t6", Queue: "q1", Name: "n"},
			MessageID: "6-0",
			Queue:     "q1",
			Group:     "g",
			handlers:  handlers,
			index:     -1,
		}
		if err := c.Next(); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if mb.schedRetry != 0 || mb.moveToDLQCnt != 0 {
			t.Fatal("success must not schedule or DLQ")
		}
	})
}

func fmtNoHandler(name string) error {
	return fmt.Errorf("%w: %s", taskmodel.ErrNoHandler, name)
}
