// Package delivery drives the delivery_status state machine: claim, check
// preferences, send via the right channel adapter, and record the outcome
// — retrying transient failures with backoff, and giving up (to the DLQ)
// on permanent ones or once retries are exhausted.
//
// This logic lives here, separate from cmd/worker, purely so it's
// unit-testable: a test can build a Processor with a real Store (against
// the real local Postgres) and a fake Adapter, and drive it directly
// without needing Kafka or a running worker process at all. cmd/worker is
// intentionally thin — it just wires this up and owns the Kafka loop.
package delivery

import (
	"context"
	"log"
	"time"

	"realtime-notification-system/internal/adapters"
	"realtime-notification-system/internal/db"
	"realtime-notification-system/internal/models"
	"realtime-notification-system/internal/preferences"
	"realtime-notification-system/internal/retry"
)

// DLQPublisher is the one method Processor needs from a DLQ producer.
// *kafkaclient.Producer satisfies this already (see PublishValue) — this
// interface exists purely so tests can substitute an in-memory fake and
// exercise the dead-letter path deterministically, with no Kafka needed.
type DLQPublisher interface {
	PublishValue(ctx context.Context, key string, value any) error
}

// Processor bundles everything a delivery attempt needs.
type Processor struct {
	Store    *db.Store
	Checker  *preferences.Checker
	DLQ      DLQPublisher
	Policy   retry.Policy
	Adapters map[string]adapters.Adapter // keyed by Adapter.Name()
}

// NewProcessor builds a Processor from a list of adapters, keying them by
// name for the reclaim path's channel lookups.
func NewProcessor(store *db.Store, checker *preferences.Checker, dlq DLQPublisher, policy retry.Policy, adapterList []adapters.Adapter) *Processor {
	byName := make(map[string]adapters.Adapter, len(adapterList))
	for _, a := range adapterList {
		byName[a.Name()] = a
	}
	return &Processor{Store: store, Checker: checker, DLQ: dlq, Policy: policy, Adapters: byName}
}

// HandleNew is called for a freshly-consumed Kafka message: it claims and
// processes every registered adapter's channel for this event. It returns
// commitOK=false only when a genuine Postgres error prevented even
// creating a claim (e.g. the database is down) — the caller should then
// NOT commit the Kafka offset, so the message is redelivered and we get
// another chance to record it at all. Every other outcome (sent, skipped,
// retryable, dead, or duplicate) is fine to commit: from that point on,
// the reclaim scan (not Kafka redelivery) drives any further retries.
func (p *Processor) HandleNew(ctx context.Context, event models.Event) (commitOK bool) {
	commitOK = true
	for name, adapter := range p.Adapters {
		claim, err := p.Store.ClaimNew(ctx, event, name)
		if err != nil {
			log.Printf("delivery: claim failed event=%s channel=%s: %v", event.EventID, name, err)
			commitOK = false
			continue
		}
		if claim == nil {
			log.Printf("delivery: duplicate delivery skipped event=%s channel=%s", event.EventID, name)
			continue
		}
		p.process(ctx, adapter, event, *claim)
	}
	return commitOK
}

// ReclaimSweep scans for retryable-and-due or stale-pending rows, reclaims
// them, and re-drives each one through the same process() logic as a live
// Kafka message. It returns how many rows it reclaimed, purely for
// logging/tests.
func (p *Processor) ReclaimSweep(ctx context.Context) (int, error) {
	rows, err := p.Store.ReclaimDue(ctx, p.Policy.StaleClaimTimeout)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		adapter, ok := p.Adapters[row.Channel]
		if !ok {
			// A channel that no longer exists (e.g. removed from
			// config) -- nothing sane to do with it here; it'll keep
			// showing up on every sweep until an operator intervenes.
			// Acceptable for this project's scope; not expected in
			// practice since the adapter list is static per deploy.
			log.Printf("delivery: reclaimed row for unknown channel=%s event=%s, skipping", row.Channel, row.Event.EventID)
			continue
		}
		log.Printf("delivery: reclaimed event=%s user=%s channel=%s attempt=%d", row.Event.EventID, row.Event.UserID, row.Channel, row.Claim.Attempts)
		p.process(ctx, adapter, row.Event, row.Claim)
	}
	return len(rows), nil
}

// process runs one already-claimed attempt: preference check, send,
// record outcome. A preference-check error is treated exactly like a
// transient send failure — both just mean "couldn't determine the outcome
// this time, worth trying again."
func (p *Processor) process(ctx context.Context, adapter adapters.Adapter, event models.Event, claim db.Claim) {
	channel := adapter.Name()

	allowed, err := p.Checker.Allowed(ctx, event.UserID, channel)
	if err != nil {
		log.Printf("delivery: preference check failed event=%s channel=%s attempt=%d: %v", event.EventID, channel, claim.Attempts, err)
		p.scheduleRetryOrDeadLetter(ctx, adapter, event, claim, err)
		return
	}
	if !allowed {
		applied, err := p.Store.MarkSkipped(ctx, claim)
		logMarkOutcome("skipped", event, channel, claim, applied, err)
		return
	}

	sendErr := adapter.Send(ctx, event)
	if sendErr == nil {
		applied, err := p.Store.MarkSent(ctx, claim)
		logMarkOutcome("sent", event, channel, claim, applied, err)
		return
	}

	log.Printf("delivery: send failed event=%s user=%s channel=%s attempt=%d: %v", event.EventID, event.UserID, channel, claim.Attempts, sendErr)
	p.scheduleRetryOrDeadLetter(ctx, adapter, event, claim, sendErr)
}

// scheduleRetryOrDeadLetter decides, given the error and how many attempts
// have happened, whether this delivery gets another try or is given up on.
func (p *Processor) scheduleRetryOrDeadLetter(ctx context.Context, adapter adapters.Adapter, event models.Event, claim db.Claim, cause error) {
	channel := adapter.Name()

	if adapters.IsPermanent(cause) || claim.Attempts >= p.Policy.MaxAttempts {
		p.deadLetter(ctx, event, channel, claim, cause)
		return
	}

	delay := p.Policy.BackoffFor(claim.Attempts)
	applied, err := p.Store.MarkRetryable(ctx, claim, delay, cause)
	if err != nil {
		log.Printf("delivery: mark retryable failed event=%s channel=%s: %v", event.EventID, channel, err)
		return
	}
	if !applied {
		log.Printf("delivery: retry not scheduled event=%s channel=%s attempt=%d: claim superseded by another worker", event.EventID, channel, claim.Attempts)
		return
	}
	log.Printf("delivery: scheduled retry event=%s user=%s channel=%s attempt=%d/%d delay=%s", event.EventID, event.UserID, channel, claim.Attempts, p.Policy.MaxAttempts, delay)
}

// deadLetter publishes the DLQ record first, then marks the row dead.
// Publishing first (rather than marking dead first) means: if the process
// crashes between the two steps, the worst case is a delivery that gets
// retried one more time than strictly necessary — the reclaim scan will
// eventually pick it back up since it's still sitting at 'pending' (never
// got the DLQ mark). The alternative order risks the opposite: a row
// permanently marked 'dead' with no corresponding DLQ record ever
// published, silently losing the failure information for good. Neither
// order is truly atomic with the other (that's the same dual-write
// problem as everywhere else two different systems are involved) -- this
// is a deliberate choice about which failure mode is less bad, not a
// guarantee.
func (p *Processor) deadLetter(ctx context.Context, event models.Event, channel string, claim db.Claim, cause error) {
	record := models.DeadLetter{
		EventID:    event.EventID,
		UserID:     event.UserID,
		Channel:    channel,
		Type:       event.Type,
		Payload:    event.Payload,
		Attempts:   claim.Attempts,
		FinalError: cause.Error(),
		FailedAt:   time.Now().UTC(),
	}

	if err := p.DLQ.PublishValue(ctx, event.UserID, record); err != nil {
		log.Printf("delivery: DLQ publish failed event=%s channel=%s: %v -- leaving retryable, will be retried again", event.EventID, channel, err)
		// Leave it retryable rather than silently dropping it: worst case
		// is one extra attempt later, which is safe (idempotent) either way.
		delay := p.Policy.BackoffFor(claim.Attempts)
		_, _ = p.Store.MarkRetryable(ctx, claim, delay, cause)
		return
	}

	applied, err := p.Store.MarkDead(ctx, claim, cause)
	logMarkOutcome("dead", event, channel, claim, applied, err)
	if applied {
		log.Printf("delivery: DEAD-LETTERED event=%s user=%s channel=%s attempts=%d final_error=%q", event.EventID, event.UserID, channel, claim.Attempts, cause.Error())
	}
}

func logMarkOutcome(status string, event models.Event, channel string, claim db.Claim, applied bool, err error) {
	if err != nil {
		log.Printf("delivery: mark %s failed event=%s channel=%s: %v", status, event.EventID, channel, err)
		return
	}
	if !applied {
		log.Printf("delivery: mark %s discarded event=%s channel=%s attempt=%d: claim superseded by another worker", status, event.EventID, channel, claim.Attempts)
		return
	}
	log.Printf("delivery: %s event=%s user=%s channel=%s attempt=%d", status, event.EventID, event.UserID, channel, claim.Attempts)
}
