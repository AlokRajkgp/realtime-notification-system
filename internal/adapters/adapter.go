// Package adapters implements one Adapter per delivery channel (in-app,
// email, push/SMS...). The worker (internal/delivery) treats every channel
// identically: claim it idempotently, call Send, record the outcome.
package adapters

import (
	"context"
	"errors"

	"realtime-notification-system/internal/models"
)

// Adapter delivers one event to one channel.
type Adapter interface {
	// Name identifies this channel in the delivery_status table, e.g. "in-app".
	Name() string
	// Send attempts delivery once. A non-nil error means this attempt
	// failed. By default that's treated as transient/retryable; an adapter
	// that knows a failure will never succeed on retry (e.g. the event
	// itself is malformed) should wrap it in PermanentError instead.
	Send(ctx context.Context, event models.Event) error
}

// PermanentError marks a Send failure as not worth retrying — the same
// input will fail identically every time (e.g. it can never be JSON-
// encoded), as opposed to a transient failure (e.g. Redis briefly
// unreachable) where trying again later stands a real chance of working.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// IsPermanent reports whether err (or something it wraps) is a PermanentError.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}
