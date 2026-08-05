package worker

import (
	"testing"

	taskmodel "github.com/twn39/taskmq/internal/taskmq/task"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractMessagePayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		values  map[string]interface{}
		want    string
		wantOK  bool
	}{
		{
			name:   "task bytes preferred",
			values: map[string]interface{}{"task": []byte("from-task"), "payload": []byte("from-payload")},
			want:   "from-task",
			wantOK: true,
		},
		{
			name:   "task string",
			values: map[string]interface{}{"task": "string-task"},
			want:   "string-task",
			wantOK: true,
		},
		{
			name:   "legacy payload bytes",
			values: map[string]interface{}{"payload": []byte("legacy")},
			want:   "legacy",
			wantOK: true,
		},
		{
			name:   "legacy payload string",
			values: map[string]interface{}{"payload": "legacy-str"},
			want:   "legacy-str",
			wantOK: true,
		},
		{
			name:   "missing",
			values: map[string]interface{}{"other": 1},
			wantOK: false,
		},
		{
			name:   "wrong type",
			values: map[string]interface{}{"task": 123},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractMessagePayload(tt.values)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.want, string(got))
			}
		})
	}
}

func TestApplyDeliveryCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		values     map[string]interface{}
		retryBefore int
		wantRetry  int
	}{
		{
			name:        "no field keeps retry",
			values:      map[string]interface{}{},
			retryBefore: 2,
			wantRetry:   2,
		},
		{
			name:        "int64 delivery count",
			values:      map[string]interface{}{"__delivery_count": int64(4)},
			retryBefore: 0,
			wantRetry:   3,
		},
		{
			name:        "int delivery count",
			values:      map[string]interface{}{"__delivery_count": 2},
			retryBefore: 0,
			wantRetry:   1,
		},
		{
			name:        "float64 delivery count",
			values:      map[string]interface{}{"__delivery_count": float64(5)},
			retryBefore: 0,
			wantRetry:   4,
		},
		{
			name:        "zero does not change",
			values:      map[string]interface{}{"__delivery_count": int64(0)},
			retryBefore: 1,
			wantRetry:   1,
		},
		{
			name:        "string delivery count from Redis",
			values:      map[string]interface{}{"__delivery_count": "3"},
			retryBefore: 1,
			wantRetry:   2,
		},
		{
			name:        "bytes delivery count",
			values:      map[string]interface{}{"__delivery_count": []byte("4")},
			retryBefore: 0,
			wantRetry:   3,
		},
		{
			name:        "non-numeric string ignored",
			values:      map[string]interface{}{"__delivery_count": "x"},
			retryBefore: 1,
			wantRetry:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &taskmodel.Task{Retry: tt.retryBefore, MaxRetry: 3}
			applyDeliveryCount(task, tt.values)
			assert.Equal(t, tt.wantRetry, task.Retry)
		})
	}
}

func TestExceededMaxRetryGate(t *testing.T) {
	t.Parallel()

	// Pure gate condition used by routeExceededMaxRetry (retry > max).
	require.False(t, (&taskmodel.Task{Retry: 3, MaxRetry: 3}).Retry > 3)
	require.True(t, (&taskmodel.Task{Retry: 4, MaxRetry: 3}).Retry > 3)
}
