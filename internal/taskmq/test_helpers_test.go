package taskmq

import (
	"math"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// TaskCmpOpts defines a standard set of comparison options for comparing Task structs.
// It compares time.Time fields approximately (within 1 second margin).
var TaskCmpOpts = []cmp.Option{
	cmp.Comparer(func(x, y time.Time) bool {
		// If both are zero, they are equal
		if x.IsZero() && y.IsZero() {
			return true
		}
		// Compare times within 1 second margin
		return math.Abs(float64(x.Sub(y))) < float64(time.Second)
	}),
}

// TaskCmpOptsIgnoreID is the same as TaskCmpOpts but also ignores the ID field.
var TaskCmpOptsIgnoreID = append(TaskCmpOpts, cmpopts.IgnoreFields(Task{}, "ID"))
