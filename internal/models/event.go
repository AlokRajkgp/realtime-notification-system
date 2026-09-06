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

// DeadLetter is published to the DLQ topic when a delivery permanently
// fails or exhausts its retries (see internal/delivery). It carries the
// original event plus enough failure context to diagnose what happened
// without needing to query Postgres.
type DeadLetter struct {
	EventID    string         `json:"event_id"`
	UserID     string         `json:"user_id"`
	Channel    string         `json:"channel"`
	Type       string         `json:"type"`
	Payload    map[string]any `json:"payload,omitempty"`
	Attempts   int            `json:"attempts"`
	FinalError string         `json:"final_error"`
	FailedAt   time.Time      `json:"failed_at"`
}
