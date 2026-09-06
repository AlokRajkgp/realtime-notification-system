// Package db owns the Postgres connection and every query against it.
package db

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"

	// Registers the "pgx" database/sql driver via its init(); we drive it
	// through sqlx rather than raw database/sql for convenient struct scans.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Connect opens a Postgres connection pool and verifies it's reachable.
func Connect(dsn string) (*sqlx.DB, error) {
	conn, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// DeliveryStatus mirrors one row of the delivery_status table, as exposed
// by GET /api/v1/events/:event_id/status. event_json is deliberately not
// selected here -- it's an internal implementation detail of retries, not
// something the status API needs to expose.
type DeliveryStatus struct {
	ID            int64      `db:"id" json:"id"`
	EventID       string     `db:"event_id" json:"event_id"`
	UserID        string     `db:"user_id" json:"user_id"`
	Channel       string     `db:"channel" json:"channel"`
	Status        string     `db:"status" json:"status"`
	Attempts      int        `db:"attempts" json:"attempts"`
	LastError     *string    `db:"last_error" json:"last_error,omitempty"`
	ClaimedAt     *time.Time `db:"claimed_at" json:"claimed_at,omitempty"`
	NextAttemptAt *time.Time `db:"next_attempt_at" json:"next_attempt_at,omitempty"`
	CreatedAt     time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at" json:"updated_at"`
}

// Store wraps every delivery_status query behind Go methods, so callers
// never write SQL directly.
type Store struct {
	db *sqlx.DB
}

func NewStore(conn *sqlx.DB) *Store {
	return &Store{db: conn}
}

// GetByEventID returns the delivery outcome for every channel a given event
// was (or is being) delivered on — this is what GET /api/v1/events/:id/status
// serves.
func (s *Store) GetByEventID(ctx context.Context, eventID string) ([]DeliveryStatus, error) {
	var rows []DeliveryStatus
	err := s.db.SelectContext(ctx, &rows, `
		SELECT id, event_id, user_id, channel, status, attempts, last_error,
		       claimed_at, next_attempt_at, created_at, updated_at
		FROM delivery_status
		WHERE event_id = $1
		ORDER BY channel
	`, eventID)
	return rows, err
}
