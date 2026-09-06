// Package adapters implements one Adapter per delivery channel (in-app,
// email, push/SMS...). The worker treats every channel identically: claim
// it idempotently, call Send, record the outcome — see cmd/worker.
package adapters

import (
	"context"

	"realtime-notification-system/internal/models"
)

// Adapter delivers one event to one channel.
type Adapter interface {
	// Name identifies this channel in the delivery_status table, e.g. "in-app".
	Name() string
	// Send attempts delivery once. A non-nil error means this attempt
	// failed — the worker records it as such; retrying is a later step.
	Send(ctx context.Context, event models.Event) error
}
