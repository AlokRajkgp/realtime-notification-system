package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"realtime-notification-system/internal/models"
)

// Claim represents ownership of one delivery attempt. ClaimedAt is a
// fencing token, not just a timestamp: every Mark* call below must present
// back the exact ClaimedAt value it was issued. If another worker has
// since reclaimed the same row (see ReclaimDue), its claimed_at will have
// moved on, the WHERE clause won't match, and the stale caller's write is
// silently discarded instead of corrupting newer state. This is what
// answers "Worker A must not overwrite Worker B's newer state" without
// needing a distributed lock — Postgres's own row-level locking during the
// UPDATE is enough.
type Claim struct {
	ID        int64
	Attempts  int
	ClaimedAt time.Time
}

// ReclaimedRow is one row the reclaim scan picked up: a fresh Claim on it,
// plus the channel and event needed to actually retry it (event_json is
// unmarshaled back into an Event here so callers never touch the column).
type ReclaimedRow struct {
	Claim   Claim
	Channel string
	Event   models.Event
}

// ClaimNew tries to create the delivery_status row for (event, channel) —
// this is the first-touch path, driven by a freshly-consumed Kafka
// message. A nil claim (with err == nil) means a row already existed: a
// duplicate delivery of the same Kafka message, or a race with another
// worker instance. The event is JSON-encoded and stored on the row so a
// later retry has the payload to resend without needing Kafka again (its
// offset will already be committed by then).
func (s *Store) ClaimNew(ctx context.Context, event models.Event, channel string) (*Claim, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	var claim Claim
	claim.Attempts = 1
	err = s.db.QueryRowxContext(ctx, `
		INSERT INTO delivery_status (event_id, user_id, channel, status, attempts, claimed_at, event_json)
		VALUES ($1, $2, $3, 'pending', 1, now(), $4)
		ON CONFLICT (event_id, channel) DO NOTHING
		RETURNING id, claimed_at
	`, event.EventID, event.UserID, channel, body).Scan(&claim.ID, &claim.ClaimedAt)

	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	default:
		return &claim, nil
	}
}

// ReclaimDue atomically finds and reclaims every row that's either a
// retryable delivery whose backoff has elapsed, or a pending delivery
// whose claim has gone stale (its worker likely crashed) — and moves each
// one back to 'pending' with a fresh claimed_at and an incremented
// attempts count, in one statement.
//
// Concurrency: this single UPDATE is safe to run from multiple worker
// processes at once with no extra locking. Postgres takes a row lock as it
// evaluates each candidate row; if two workers' scans overlap on the same
// row, one blocks until the other's UPDATE commits, then re-evaluates its
// WHERE clause against the now-committed row — which no longer matches
// (status is 'pending' again with a brand-new claimed_at), so the second
// worker simply doesn't reclaim it. That's ordinary Postgres MVCC
// behavior, not application code we had to write.
func (s *Store) ReclaimDue(ctx context.Context, staleClaimTimeout time.Duration) ([]ReclaimedRow, error) {
	rows, err := s.db.QueryxContext(ctx, `
		UPDATE delivery_status
		SET status = 'pending', claimed_at = now(), attempts = attempts + 1
		WHERE (status = 'retryable' AND next_attempt_at <= now())
		   OR (status = 'pending' AND claimed_at < now() - make_interval(secs => $1))
		RETURNING id, channel, attempts, claimed_at, event_json
	`, staleClaimTimeout.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ReclaimedRow
	for rows.Next() {
		var r ReclaimedRow
		var eventJSON []byte
		if err := rows.Scan(&r.Claim.ID, &r.Channel, &r.Claim.Attempts, &r.Claim.ClaimedAt, &eventJSON); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(eventJSON, &r.Event); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkSent records a successful delivery. Returns applied=false if claim
// is stale (someone else has since reclaimed this row) — the caller's
// result is then simply discarded rather than corrupting newer state.
func (s *Store) MarkSent(ctx context.Context, claim Claim) (applied bool, err error) {
	return s.markTerminal(ctx, claim, "sent", nil)
}

// MarkSkipped records that preferences/DND intentionally blocked delivery.
func (s *Store) MarkSkipped(ctx context.Context, claim Claim) (applied bool, err error) {
	return s.markTerminal(ctx, claim, "skipped", nil)
}

// MarkDead records a permanent failure or exhausted retries — this is the
// DLQ marker. lastErr should never be nil for a dead delivery.
func (s *Store) MarkDead(ctx context.Context, claim Claim, lastErr error) (applied bool, err error) {
	return s.markTerminal(ctx, claim, "dead", lastErr)
}

func (s *Store) markTerminal(ctx context.Context, claim Claim, status string, lastErr error) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE delivery_status
		SET status = $3, last_error = $4, updated_at = now()
		WHERE id = $1 AND claimed_at = $2 AND status = 'pending'
	`, claim.ID, claim.ClaimedAt, status, errMsg(lastErr))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// MarkRetryable schedules a transient failure for another try after delay.
// delay is deliberately a duration, not an absolute time.Time computed by
// the caller: next_attempt_at is computed here as now() + delay using
// Postgres's own clock, and ReclaimDue later compares it against
// Postgres's own now() too. Computing the deadline with the Go process's
// clock and then comparing it against the database's clock would be
// vulnerable to ordinary clock skew between the two machines/containers —
// in local dev, the Postgres container's clock and the host's were
// observed to differ by over 100ms, comparable to this project's smallest
// configured backoff, which caused real, intermittent test flakiness
// before this was fixed. One clock is used for both the write and the
// read, exactly like ReclaimDue already does for the stale-claim check.
//
// Returns applied=false under the same stale-claim condition as the
// terminal Mark* methods.
func (s *Store) MarkRetryable(ctx context.Context, claim Claim, delay time.Duration, lastErr error) (applied bool, err error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE delivery_status
		SET status = 'retryable', next_attempt_at = now() + make_interval(secs => $3), last_error = $4, updated_at = now()
		WHERE id = $1 AND claimed_at = $2 AND status = 'pending'
	`, claim.ID, claim.ClaimedAt, delay.Seconds(), errMsg(lastErr))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func errMsg(err error) *string {
	if err == nil {
		return nil
	}
	msg := err.Error()
	return &msg
}
