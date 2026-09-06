-- delivery_status is both the delivery-status query table AND the
-- idempotency mechanism: the unique constraint on (event_id, channel) is
-- what turns Kafka's at-least-once delivery into effectively-once
-- processing per channel. A worker "claims" a delivery by trying to insert
-- a row; if the row already exists (duplicate delivery of the same Kafka
-- message, or two workers racing on a rebalance), the insert is a no-op and
-- the worker knows to skip it instead of delivering twice.
CREATE TABLE delivery_status (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT NOT NULL,
    user_id     TEXT NOT NULL,
    channel     TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'sent', 'failed', 'skipped')),
    attempts    INT NOT NULL DEFAULT 1,
    last_error  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (event_id, channel)
);

-- Every status lookup we do is "all channel outcomes for one event"
-- (GET /api/v1/events/:id/status) or "everything for one user" (a later
-- admin/user-facing query), so index both.
CREATE INDEX idx_delivery_status_event_id ON delivery_status (event_id);
CREATE INDEX idx_delivery_status_user_id ON delivery_status (user_id);
