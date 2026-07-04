package taskmq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestDefaultWorkerOptions(t *testing.T) {
	codec := JSONCodec{}
	opts := defaultWorkerOptions(codec)

	if opts.concurrency != 5 {
		t.Errorf("expected default concurrency to be 5, got %d", opts.concurrency)
	}
	if opts.group != "taskmq-group" {
		t.Errorf("expected default group to be 'taskmq-group', got %s", opts.group)
	}
	if opts.consumer != "taskmq-consumer-1" {
		t.Errorf("expected default consumer to be 'taskmq-consumer-1', got %s", opts.consumer)
	}
	if opts.codec != codec {
		t.Error("expected codec to match the passed codec")
	}
	if opts.cronHealingInterval != 1*time.Minute {
		t.Errorf("expected default cronHealingInterval to be 1m, got %v", opts.cronHealingInterval)
	}
	if opts.schedulerPollInterval != 500*time.Millisecond {
		t.Errorf("expected default schedulerPollInterval to be 500ms, got %v", opts.schedulerPollInterval)
	}
	if opts.janitorInterval != 3*time.Second {
		t.Errorf("expected default janitorInterval to be 3s, got %v", opts.janitorInterval)
	}
}

func TestWithOptionFunctions(t *testing.T) {
	codec := JSONCodec{}
	opts := defaultWorkerOptions(codec)

	// Test WithConcurrency
	err := WithConcurrency(10)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.concurrency != 10 {
		t.Errorf("expected concurrency to be 10, got %d", opts.concurrency)
	}

	err = WithConcurrency(-1)(&opts)
	if err == nil {
		t.Error("expected error for invalid concurrency, got nil")
	}

	// Test WithGroup
	err = WithGroup("my-group")(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.group != "my-group" {
		t.Errorf("expected group to be 'my-group', got %s", opts.group)
	}

	err = WithGroup("")(&opts)
	if err == nil {
		t.Error("expected error for empty group, got nil")
	}

	// Test WithConsumer
	err = WithConsumer("my-consumer")(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.consumer != "my-consumer" {
		t.Errorf("expected consumer to be 'my-consumer', got %s", opts.consumer)
	}

	err = WithConsumer("")(&opts)
	if err == nil {
		t.Error("expected error for empty consumer, got nil")
	}

	// Test WithExecutionPoolSize
	err = WithExecutionPoolSize(20)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.executionPoolSize != 20 {
		t.Errorf("expected execution pool size to be 20, got %d", opts.executionPoolSize)
	}

	err = WithExecutionPoolSize(0)(&opts)
	if err == nil {
		t.Error("expected error for invalid execution pool size, got nil")
	}

	// Test WithRateLimit
	err = WithRateLimit(100, 10*time.Second)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.rateLimitMax != 100 || opts.rateLimitDuration != 10*time.Second {
		t.Errorf("expected rate limits to be max=100, dur=10s, got max=%d, dur=%v", opts.rateLimitMax, opts.rateLimitDuration)
	}

	err = WithRateLimit(-1, 10*time.Second)(&opts)
	if err == nil {
		t.Error("expected error for invalid rate limit max, got nil")
	}

	// Test WithContext
	customCtx := context.WithValue(context.Background(), "test", "val")
	err = WithContext(customCtx)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.context != customCtx {
		t.Error("expected custom context to be set")
	}

	err = WithContext(nil)(&opts)
	if err == nil {
		t.Error("expected error for nil context, got nil")
	}

	// Test WithGroupKeyExtractor
	extractor := func(payload []byte) string { return "custom" }
	err = WithGroupKeyExtractor(extractor)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.groupKeyExtractor == nil || opts.groupKeyExtractor(nil) != "custom" {
		t.Error("expected groupKeyExtractor to be set correctly")
	}

	err = WithGroupKeyExtractor(nil)(&opts)
	if err == nil {
		t.Error("expected error for nil extractor, got nil")
	}
}

func TestInspectJSONField(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		field    string
		expected string
	}{
		{
			name:     "Flat string key",
			payload:  []byte(`{"user_id":"123","group":"tenant-1"}`),
			field:    "group",
			expected: "tenant-1",
		},
		{
			name:     "Flat integer key",
			payload:  []byte(`{"user_id":456,"group":"tenant-2"}`),
			field:    "user_id",
			expected: "456",
		},
		{
			name:     "Key not found",
			payload:  []byte(`{"user_id":456,"group":"tenant-2"}`),
			field:    "missing",
			expected: "",
		},
		{
			name:     "Invalid JSON",
			payload:  []byte(`{invalid,"user_id":456}`),
			field:    "user_id",
			expected: "",
		},
		{
			name:     "Empty payload",
			payload:  []byte(``),
			field:    "user_id",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := inspectJSONField(tt.payload, tt.field)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

type dummyCronManager struct{}
func (d *dummyCronManager) Run(ctx context.Context) error { return nil }
func (d *dummyCronManager) Reschedule(ctx context.Context, task *Task) error { return nil }

type dummyScheduler struct{}
func (d *dummyScheduler) Run(ctx context.Context) error { return nil }

type dummyJanitor struct{}
func (d *dummyJanitor) Run(ctx context.Context) error { return nil }
func (d *dummyJanitor) RegisterProcessor(fn func(ctx context.Context, msg redis.XMessage)) {}

func TestCustomFactories(t *testing.T) {
	opts := defaultWorkerOptions(JSONCodec{})

	cronCalled := false
	schedulerCalled := false
	janitorCalled := false

	cronFactory := func(rdb *redis.Client, logger *zap.Logger, queue string, codec Codec, healingInterval time.Duration, healingLockTTL time.Duration, scanBatchSize int, scanMaxCount int) CronManager {
		cronCalled = true
		return &dummyCronManager{}
	}
	schedulerFactory := func(rdb *redis.Client, logger *zap.Logger, queue string, cronManager CronManager, codec Codec, pollInterval time.Duration) Runner {
		schedulerCalled = true
		return &dummyScheduler{}
	}
	janitorFactory := func(rdb *redis.Client, logger *zap.Logger, queue string, group string, consumer string, concurrency int, checkInterval time.Duration, minIdleTime time.Duration) PELRecoveryJanitor {
		janitorCalled = true
		return &dummyJanitor{}
	}

	err := WithCronManagerFactory(cronFactory)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = WithSchedulerFactory(schedulerFactory)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = WithJanitorFactory(janitorFactory)(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify buildDefaultComponents uses them
	buildDefaultComponents(nil, nil, "test-queue", &opts)

	if !cronCalled {
		t.Error("expected cronManagerFactory to be called")
	}
	if !schedulerCalled {
		t.Error("expected schedulerFactory to be called")
	}
	if !janitorCalled {
		t.Error("expected janitorFactory to be called")
	}

	// Test nil errors
	if WithCronManagerFactory(nil)(&opts) == nil {
		t.Error("expected error with nil factory")
	}
	if WithSchedulerFactory(nil)(&opts) == nil {
		t.Error("expected error with nil factory")
	}
	if WithJanitorFactory(nil)(&opts) == nil {
		t.Error("expected error with nil factory")
	}
}
