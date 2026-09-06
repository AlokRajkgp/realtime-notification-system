// Package models holds structs shared between the api and worker binaries.
package models

import "time"

// Event is what a client POSTs to the producer API, and what gets published
// to Kafka as-is (JSON-encoded). EventID is the idempotency key used later
// for dedupe — if the caller doesn't supply one, the API generates a UUID.
type Event struct {
	EventID   string         `json:"event_id"`
	UserID    string         `json:"user_id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}
