// Package retry centralizes the delivery retry policy: how many times to
// try, and how long to wait between tries. Keeping every retry-related
// number in one small, pure (no I/O) package means the backoff math is
// unit-testable without a database or a running worker, and no magic
// numbers are scattered through internal/delivery or cmd/worker.
package retry

import (
	"math"
	"math/rand"
	"time"
)

// Policy controls retry timing. Built once from config at startup.
type Policy struct {
	// MaxAttempts is how many times adapter.Send is tried in total before a
	// delivery is given up on and sent to the DLQ. Attempt 1 is the first
	// try (from the original Kafka message); attempts 2..MaxAttempts come
	// from the reclaim scan.
	MaxAttempts int

	// InitialBackoff is the delay before attempt 2 (the first retry).
	InitialBackoff time.Duration

	// MaxBackoff caps how large a delay ever gets -- without a cap,
	// exponential growth eventually means waiting hours between tries.
	MaxBackoff time.Duration

	// Jitter is a fraction (e.g. 0.2 = ±20%) of randomness applied around
	// the computed exponential delay, so deliveries that failed at the
	// same moment (e.g. a Redis blip affecting many users at once) don't
	// all retry at the exact same instant and hit it again simultaneously.
	Jitter float64

	// StaleClaimTimeout is how long a delivery may sit at status='pending'
	// before the reclaim scan treats it as abandoned (its worker likely
	// crashed). Must be comfortably larger than how long one delivery
	// attempt can legitimately take, or a claim still genuinely in
	// progress would be reclaimed out from under its owner.
	StaleClaimTimeout time.Duration
}

// BackoffFor returns how long to wait before the next attempt, given that
// `attempt` attempts have already been made (attempt=1 means the first try
// just failed, so this is the delay before attempt 2).
func (p Policy) BackoffFor(attempt int) time.Duration {
	exp := float64(p.InitialBackoff) * math.Pow(2, float64(attempt-1))
	if exp <= 0 || exp > float64(p.MaxBackoff) {
		exp = float64(p.MaxBackoff)
	}

	if p.Jitter <= 0 {
		return time.Duration(exp)
	}

	delta := exp * p.Jitter
	jittered := exp - delta + rand.Float64()*2*delta // uniform in [exp-delta, exp+delta]
	if jittered < 0 {
		jittered = 0
	}
	return time.Duration(jittered)
}
