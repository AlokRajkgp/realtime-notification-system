-- Phase C: turns delivery_status into a real state machine (pending /
-- retryable / sent / skipped / dead) instead of a one-shot pending/sent/
-- failed/skipped flag.
--
-- claimed_at doubles as both a "when was this claimed" timestamp AND a
-- fencing token: any Mark* call must present the exact claimed_at value it
-- was handed, or its write is silently discarded because someone else has
-- since reclaimed the row (see internal/db/delivery.go).
--
-- next_attempt_at is when a 'retryable' row becomes eligible for reclaim.
--
-- event_json holds the original event body. It has to live somewhere
-- queryable outside Kafka, because by the time a retry is due, the Kafka
-- offset for the original message has long since been committed -- Kafka
-- itself will never hand us this message again.

-- Any pre-Phase-C 'failed' rows become 'dead' before the stricter
-- constraint below would otherwise reject them.
UPDATE delivery_status SET status = 'dead' WHERE status = 'failed';

ALTER TABLE delivery_status
    ADD COLUMN claimed_at      TIMESTAMPTZ,
    ADD COLUMN next_attempt_at TIMESTAMPTZ,
    ADD COLUMN event_json      JSONB;

ALTER TABLE delivery_status
    DROP CONSTRAINT delivery_status_status_check,
    ADD CONSTRAINT delivery_status_status_check
        CHECK (status IN ('pending', 'retryable', 'sent', 'skipped', 'dead'));

-- The reclaim scan's two lookups (retryable rows past due, pending rows
-- gone stale) each get a small partial index -- cheap to maintain since
-- they only cover rows in that one status.
CREATE INDEX idx_delivery_status_retry_due
    ON delivery_status (next_attempt_at)
    WHERE status = 'retryable';

CREATE INDEX idx_delivery_status_stale_pending
    ON delivery_status (claimed_at)
    WHERE status = 'pending';
