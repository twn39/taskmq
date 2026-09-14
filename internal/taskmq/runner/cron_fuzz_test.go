package runner_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twn39/taskmq/internal/taskmq/runner"
)

// FuzzCronParser verifies that CronParser robustly parses arbitrary cron specifications
// and that any successfully parsed schedule produces safe, monotonic Next timestamps without panics or hangs.
func FuzzCronParser(f *testing.F) {
	// Seed corpus
	f.Add("*/5 * * * *")
	f.Add("0 */10 * * * *")
	f.Add("@every 1m")
	f.Add("@every 5s")
	f.Add("@every 24h")
	f.Add("@hourly")
	f.Add("@daily")
	f.Add("@midnight")
	f.Add("@weekly")
	f.Add("@monthly")
	f.Add("@yearly")
	f.Add("0 0 1 1 *")
	f.Add("59 23 31 12 5")
	f.Add("0 0 12 ? * WED")
	f.Add("CRON_TZ=UTC 0 0 * * *")
	f.Add("TZ=")
	f.Add("CRON_TZ=")
	f.Add("TZ=UTC")
	f.Add("")
	f.Add("invalid spec")
	f.Add("*/0 * * * *")
	f.Add("99999999 * * * *")
	f.Add(strings.Repeat("* ", 50))
	f.Add("0 0 31 2 *") // Feb 31 (impossible date boundary)

	f.Fuzz(func(t *testing.T, spec string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ParseCronSpec panicked on spec %q: %v", spec, r)
			}
		}()

		sched, err := runner.ParseCronSpec(spec)
		if err != nil {
			// Expected for malformed inputs; verify return is nil
			require.Nil(t, sched)
			return
		}

		require.NotNil(t, sched)

		// Verification of Next schedule safety
		now := time.Now()
		next := sched.Next(now)

		// Next should either be a future time or zero (if schedule terminated/exhausted)
		if !next.IsZero() {
			require.True(t, next.After(now) || next.Equal(now))

			// Verify second step monotonicity
			next2 := sched.Next(next)
			if !next2.IsZero() {
				require.True(t, next2.After(next) || next2.Equal(next))
			}
		}
	})
}
