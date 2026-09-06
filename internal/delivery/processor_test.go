package delivery_test

// These are integration tests against the real local Postgres (not a
// mock) — the stale-claim race case specifically exercises Postgres's own
// row-locking behavior, which a mock/fake DB could not meaningfully prove.
// They need `make up` + migrations applied. Adapter and DLQ dependencies
// are faked so every test is deterministic and fast: no real Redis/Kafka
// traffic, no flakiness from external services.

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"realtime-notification-system/internal/adapters"
	"realtime-notification-system/internal/db"
	"realtime-notification-system/internal/delivery"
	"realtime-notification-system/internal/models"
	"realtime-notification-system/internal/preferences"
	"realtime-notification-system/internal/retry"
)

const testDSN = "postgres://notify:notify@localhost:5433/notify?sslmode=disable"

// TestMain gives each invocation of this suite a clean slate: leftover
// rows from a previous run that was interrupted (e.g. Ctrl-C mid-test)
// won't be there to confuse anything. This does NOT protect against
// interference *within* one run -- see the comment on ReclaimDue's global
// scan below for how each test is made robust to that instead.
func TestMain(m *testing.M) {
	if conn, err := db.Connect(testDSN); err == nil {
		conn.Exec(`DELETE FROM delivery_status WHERE event_id LIKE 'test-%'`)
		conn.Close()
	}
	os.Exit(m.Run())
}

func testStore(t *testing.T) *db.Store {
	t.Helper()
	conn, err := db.Connect(testDSN)
	if err != nil {
		t.Skipf("postgres not reachable at %s (is `make up` running with migrations applied?): %v", testDSN, err)
	}
	t.Cleanup(func() { conn.Close() })
	return db.NewStore(conn)
}

// fakeAdapter lets each test script exactly how Send behaves without
// touching Redis or any real channel.
type fakeAdapter struct {
	name  string
	mu    sync.Mutex
	fn    func(callNum int) error
	calls int
}

func newFakeAdapter(name string, fn func(callNum int) error) *fakeAdapter {
	return &fakeAdapter{name: name, fn: fn}
}

func (f *fakeAdapter) Name() string { return f.name }

func (f *fakeAdapter) Send(ctx context.Context, event models.Event) error {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	return f.fn(n)
}

func (f *fakeAdapter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeDLQ records every record published to it, in memory, instead of
// touching Kafka.
type fakeDLQ struct {
	mu      sync.Mutex
	records []any
}

func (d *fakeDLQ) PublishValue(_ context.Context, _ string, value any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.records = append(d.records, value)
	return nil
}

func (d *fakeDLQ) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.records)
}

func testEvent() models.Event {
	return models.Event{
		EventID:   "test-" + uuid.NewString(),
		UserID:    "test-user-" + uuid.NewString(),
		Type:      "test.event",
		CreatedAt: time.Now().UTC(),
	}
}

// uniqueChannel returns a channel name unique to this call. ReclaimDue's
// scan is intentionally global (it has to look across the whole table for
// anything due -- that's how a real reclaim sweep works in production
// too), so a stray retryable/pending row left behind by another test can
// legitimately be swept up in the same call as this test's own row. A
// unique channel per test means that stray row is simply absent from
// *this* test's Processor.Adapters map and gets skipped, not mishandled --
// but note this does NOT make ReclaimSweep's returned count (n) exclusive
// to this test's row; assertions below check this test's own row via
// getStatus() instead of relying on n being an exact count.
func uniqueChannel() string {
	return "test-channel-" + uuid.NewString()
}

// fastPolicy uses short, test-friendly durations so the retry/stale-claim
// tests resolve in well under the real 45s/60s defaults. Jitter is 0 so
// BackoffFor's own unit tests can assert exact values; the integration
// tests below poll for the resulting state (waitForStatus) rather than
// asserting exact timing, since Postgres reclaim/backoff windows still
// pass through this project's local Docker/VM environment.
func fastPolicy() retry.Policy {
	return retry.Policy{
		MaxAttempts:       3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        300 * time.Millisecond,
		Jitter:            0,
		StaleClaimTimeout: 300 * time.Millisecond,
	}
}

func newProcessor(store *db.Store, dlq delivery.DLQPublisher, policy retry.Policy, adapter adapters.Adapter) *delivery.Processor {
	checker := preferences.NewChecker(store)
	return delivery.NewProcessor(store, checker, dlq, policy, []adapters.Adapter{adapter})
}

func getStatus(t *testing.T, store *db.Store, eventID string) db.DeliveryStatus {
	t.Helper()
	rows, err := store.GetByEventID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 delivery_status row for event %s, got %d", eventID, len(rows))
	}
	return rows[0]
}

// waitForStatus repeatedly runs a reclaim sweep and checks eventID's row
// until it reaches one of wantStatuses or a generous overall timeout
// elapses. This project's test environment (Postgres inside a local VM,
// shared with a long-running, memory-pressured host) can occasionally
// stall well past what a single fixed time.Sleep would allow for. Polling
// tests the guarantee the system actually makes -- "eventually retried and
// resolved" -- rather than "resolved within an exact number of
// milliseconds," which was never the real claim (see docs/INTERVIEW_NOTES.md).
func waitForStatus(t *testing.T, store *db.Store, proc *delivery.Processor, eventID string, wantStatuses ...string) db.DeliveryStatus {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		proc.ReclaimSweep(context.Background())
		row := getStatus(t, store, eventID)
		for _, want := range wantStatuses {
			if row.Status == want {
				return row
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("event %s did not reach status %v within 10s; last status = %q", eventID, wantStatuses, row.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Test 1: successful delivery, pending -> sent.
func TestHandleNew_SuccessfulDelivery(t *testing.T) {
	store := testStore(t)
	event := testEvent()
	adapter := newFakeAdapter(uniqueChannel(), func(int) error { return nil })
	proc := newProcessor(store, &fakeDLQ{}, fastPolicy(), adapter)

	if ok := proc.HandleNew(context.Background(), event); !ok {
		t.Fatal("HandleNew returned commitOK=false, want true")
	}

	row := getStatus(t, store, event.EventID)
	if row.Status != "sent" {
		t.Errorf("status = %q, want sent", row.Status)
	}
	if row.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", row.Attempts)
	}
	if adapter.callCount() != 1 {
		t.Errorf("adapter.Send called %d times, want exactly 1", adapter.callCount())
	}
}

// Test 7: an already-sent delivery redelivered by Kafka must not resend.
func TestHandleNew_DuplicateRedelivery_DoesNotResend(t *testing.T) {
	store := testStore(t)
	event := testEvent()
	adapter := newFakeAdapter(uniqueChannel(), func(int) error { return nil })
	proc := newProcessor(store, &fakeDLQ{}, fastPolicy(), adapter)

	proc.HandleNew(context.Background(), event) // first delivery
	proc.HandleNew(context.Background(), event) // Kafka "redelivers" the same event

	if adapter.callCount() != 1 {
		t.Errorf("adapter.Send called %d times across 2 deliveries of the same event, want exactly 1", adapter.callCount())
	}
	row := getStatus(t, store, event.EventID)
	if row.Status != "sent" {
		t.Errorf("status = %q, want sent", row.Status)
	}
}

// Test 6: worker crashes after Send succeeds but before the Kafka commit
// lands -> Kafka redelivers -> must not lose or duplicate the notification.
func TestHandleNew_RedeliveredBeforeCommit_NotLostNotDuplicated(t *testing.T) {
	store := testStore(t)
	event := testEvent()
	adapter := newFakeAdapter(uniqueChannel(), func(int) error { return nil })
	proc := newProcessor(store, &fakeDLQ{}, fastPolicy(), adapter)

	proc.HandleNew(context.Background(), event) // succeeds; imagine the crash happens right here, before commit
	proc.HandleNew(context.Background(), event) // Kafka redelivers the uncommitted message

	if adapter.callCount() != 1 {
		t.Errorf("adapter.Send called %d times across the redelivery, want exactly 1 (not lost, not duplicated)", adapter.callCount())
	}
	row := getStatus(t, store, event.EventID)
	if row.Status != "sent" {
		t.Errorf("status after redelivery = %q, want still sent", row.Status)
	}
}

// Test 2: retryable failure, pending -> retryable -> sent.
func TestReclaimSweep_RetryThenSucceed(t *testing.T) {
	store := testStore(t)
	event := testEvent()
	adapter := newFakeAdapter(uniqueChannel(), func(n int) error {
		if n == 1 {
			return errors.New("transient: connection refused")
		}
		return nil
	})
	policy := fastPolicy()
	proc := newProcessor(store, &fakeDLQ{}, policy, adapter)

	proc.HandleNew(context.Background(), event)
	row := getStatus(t, store, event.EventID)
	if row.Status != "retryable" {
		t.Fatalf("status after first failed attempt = %q, want retryable", row.Status)
	}
	if row.NextAttemptAt == nil {
		t.Fatal("expected next_attempt_at to be set for a retryable row")
	}

	row = waitForStatus(t, store, proc, event.EventID, "sent", "dead")
	if row.Status != "sent" {
		t.Errorf("status after retry = %q, want sent", row.Status)
	}
	if row.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (1 initial + 1 retry)", row.Attempts)
	}
	if adapter.callCount() != 2 {
		t.Errorf("adapter.Send called %d times, want 2", adapter.callCount())
	}
}

// Test 3: permanent failure, pending -> dead, with no retry attempted.
func TestHandleNew_PermanentFailure_GoesDeadImmediately(t *testing.T) {
	store := testStore(t)
	event := testEvent()
	permErr := &adapters.PermanentError{Err: errors.New("event can never be encoded")}
	adapter := newFakeAdapter(uniqueChannel(), func(int) error { return permErr })
	dlq := &fakeDLQ{}
	proc := newProcessor(store, dlq, fastPolicy(), adapter)

	proc.HandleNew(context.Background(), event)

	row := getStatus(t, store, event.EventID)
	if row.Status != "dead" {
		t.Fatalf("status = %q, want dead", row.Status)
	}
	if row.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 -- a permanent failure must not retry at all", row.Attempts)
	}
	if adapter.callCount() != 1 {
		t.Errorf("adapter.Send called %d times, want exactly 1", adapter.callCount())
	}
	if dlq.count() != 1 {
		t.Errorf("DLQ received %d records, want exactly 1", dlq.count())
	}
}

// Test 4 & 10: retries exhausted -> DLQ, and no infinite retry loop after.
func TestReclaimSweep_MaxAttemptsExceeded_DeadLettersAndStops(t *testing.T) {
	store := testStore(t)
	event := testEvent()
	adapter := newFakeAdapter(uniqueChannel(), func(int) error {
		return errors.New("transient: always fails")
	})
	policy := fastPolicy() // MaxAttempts: 3
	dlq := &fakeDLQ{}
	proc := newProcessor(store, dlq, policy, adapter)

	proc.HandleNew(context.Background(), event) // attempt 1: fails -> retryable

	row := waitForStatus(t, store, proc, event.EventID, "dead")
	if row.Attempts != policy.MaxAttempts {
		t.Errorf("attempts = %d, want exactly MaxAttempts=%d", row.Attempts, policy.MaxAttempts)
	}
	if adapter.callCount() != policy.MaxAttempts {
		t.Errorf("adapter.Send called %d times, want exactly %d -- proves retries stopped, not an infinite loop", adapter.callCount(), policy.MaxAttempts)
	}
	if dlq.count() != 1 {
		t.Errorf("DLQ received %d records, want exactly 1", dlq.count())
	}

	// Prove it has really stopped: sweep several more times and confirm
	// a dead delivery is never picked up again.
	for i := 0; i < 3; i++ {
		time.Sleep(policy.MaxBackoff)
		proc.ReclaimSweep(context.Background())
	}
	if adapter.callCount() != policy.MaxAttempts {
		t.Errorf("adapter.Send called %d times after extra sweeps, want still %d", adapter.callCount(), policy.MaxAttempts)
	}
}

// Test 5: worker crashes immediately after claiming (before any Send) ->
// the row must not be stuck at pending forever; the reclaim scan recovers it.
// This is the fix for the bug identified in the Phase A/B audit.
func TestReclaimSweep_StalePendingClaim_Recovered(t *testing.T) {
	store := testStore(t)
	event := testEvent()
	channel := uniqueChannel()
	adapter := newFakeAdapter(channel, func(int) error { return nil })
	policy := fastPolicy()
	proc := newProcessor(store, &fakeDLQ{}, policy, adapter)

	// Claim directly (bypassing Processor) and never call process() on it
	// -- exactly what a crash between ClaimNew and everything after it
	// looks like from the database's point of view.
	claim, err := store.ClaimNew(context.Background(), event, channel)
	if err != nil || claim == nil {
		t.Fatalf("ClaimNew: claim=%v err=%v", claim, err)
	}
	if got := getStatus(t, store, event.EventID).Status; got != "pending" {
		t.Fatalf("status right after claim = %q, want pending", got)
	}

	// Not yet stale: a sweep now must NOT touch *our* row -- it could
	// still be genuinely in progress. (Not asserting on the sweep's
	// returned count here: ReclaimDue's scan is global, so it could
	// harmlessly also reclaim an unrelated leftover row from another
	// test; what matters is that THIS row's claim is untouched.)
	proc.ReclaimSweep(context.Background())
	if got := getStatus(t, store, event.EventID); got.ClaimedAt == nil || !got.ClaimedAt.Equal(claim.ClaimedAt) {
		t.Fatalf("this row's claim changed before the stale timeout elapsed -- it could still be genuinely in progress")
	}

	row := waitForStatus(t, store, proc, event.EventID, "sent")
	if row.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (1 original claim + 1 recovery attempt)", row.Attempts)
	}
}

// Test 8: Worker A holds a now-stale claim; Worker B reclaims and
// completes it first. Worker A must not be able to overwrite B's result.
func TestStaleClaimRace_OldWorkerCannotOverwriteNewerState(t *testing.T) {
	store := testStore(t)
	event := testEvent()
	channel := uniqueChannel()
	policy := fastPolicy()

	claimA, err := store.ClaimNew(context.Background(), event, channel)
	if err != nil || claimA == nil {
		t.Fatalf("ClaimNew: claim=%v err=%v", claimA, err)
	}

	// Time passes; A's claim goes stale. B's sweep reclaims and succeeds.
	adapter := newFakeAdapter(channel, func(int) error { return nil })
	proc := newProcessor(store, &fakeDLQ{}, policy, adapter)
	waitForStatus(t, store, proc, event.EventID, "sent")

	// Worker A "wakes up" and tries to record its own (now stale) result
	// using the claim it was originally handed.
	applied, err := store.MarkSent(context.Background(), *claimA)
	if err != nil {
		t.Fatalf("MarkSent: %v", err)
	}
	if applied {
		t.Fatal("Worker A's stale MarkSent was applied -- it should have been discarded (claim superseded by Worker B)")
	}

	if got := getStatus(t, store, event.EventID).Status; got != "sent" {
		t.Errorf("status after Worker A's stale write attempt = %q, want still sent (unchanged)", got)
	}
}

// Test 9: backoff delay is exponential with a cap (jitter=0, so exact).
func TestBackoffFor_ExponentialWithCap(t *testing.T) {
	policy := retry.Policy{InitialBackoff: time.Second, MaxBackoff: 10 * time.Second, Jitter: 0}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // 16s uncapped, capped to MaxBackoff
	}
	for _, c := range cases {
		if got := policy.BackoffFor(c.attempt); got != c.want {
			t.Errorf("BackoffFor(%d) = %s, want %s", c.attempt, got, c.want)
		}
	}
}

// Test 9: jitter keeps the delay within the expected band around the
// exponential value, rather than being exactly fixed (thundering-herd check).
func TestBackoffFor_JitterStaysWithinBounds(t *testing.T) {
	policy := retry.Policy{InitialBackoff: time.Second, MaxBackoff: 10 * time.Second, Jitter: 0.2}
	exp := 4 * time.Second // attempt 3
	lo := time.Duration(float64(exp) * 0.8)
	hi := time.Duration(float64(exp) * 1.2)

	for i := 0; i < 50; i++ {
		if got := policy.BackoffFor(3); got < lo || got > hi {
			t.Fatalf("BackoffFor(3) = %s, want within [%s, %s]", got, lo, hi)
		}
	}
}
