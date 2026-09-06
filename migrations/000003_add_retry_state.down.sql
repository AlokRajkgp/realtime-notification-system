DROP INDEX IF EXISTS idx_delivery_status_stale_pending;
DROP INDEX IF EXISTS idx_delivery_status_retry_due;

ALTER TABLE delivery_status
    DROP CONSTRAINT delivery_status_status_check,
    ADD CONSTRAINT delivery_status_status_check
        CHECK (status IN ('pending', 'sent', 'failed', 'skipped'));

UPDATE delivery_status SET status = 'failed' WHERE status IN ('retryable', 'dead');

ALTER TABLE delivery_status
    DROP COLUMN claimed_at,
    DROP COLUMN next_attempt_at,
    DROP COLUMN event_json;
