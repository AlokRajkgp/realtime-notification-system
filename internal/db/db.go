// Package db owns the Postgres connection and every query against it.
package db

import (
	"context"
	"database/sql"
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

// DeliveryStatus mirrors one row of the delivery_status table.
type DeliveryStatus struct {
	ID        int64     `db:"id" json:"id"`
	EventID   string    `db:"event_id" json:"event_id"`
	UserID    string    `db:"user_id" json:"user_id"`
	Channel   string    `db:"channel" json:"channel"`
	Status    string    `db:"status" json:"status"`
	Attempts  int       `db:"attempts" json:"attempts"`
	LastError *string   `db:"last_error" json:"last_error,omitempty"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

// Store wraps every delivery_status query behind Go methods, so callers
// never write SQL directly.
type Store struct {
	db *sqlx.DB
}

func NewStore(conn *sqlx.DB) *Store {
	return &Store{db: conn}
}

// ClaimDelivery tries to create the delivery_status row for (eventID, channel).
// claimed=true means this call created it, i.e. this worker owns delivering
// this event on this channel and should proceed. claimed=false means a row
// already existed — a duplicate delivery of the same Kafka message (redelivery
// after a crash/rebalance) or a race with another worker instance — and the
// caller should skip sending again.
func (s *Store) ClaimDelivery(ctx context.Context, eventID, userID, channel string) (claimed bool, err error) {
	var id int64
	err = s.db.QueryRowxContext(ctx, `
		INSERT INTO delivery_status (event_id, user_id, channel, status, attempts)
		VALUES ($1, $2, $3, 'pending', 1)
		ON CONFLICT (event_id, channel) DO NOTHING
		RETURNING id
	`, eventID, userID, channel).Scan(&id)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

// MarkStatus records the outcome of a delivery attempt this worker claimed.
// lastErr may be nil (e.g. status "sent").
func (s *Store) MarkStatus(ctx context.Context, eventID, channel, status string, lastErr error) error {
	var errMsg *string
	if lastErr != nil {
		msg := lastErr.Error()
		errMsg = &msg
	}

	_, err := s.db.ExecContext(ctx, `
		UPDATE delivery_status
		SET status = $3, last_error = $4, updated_at = now()
		WHERE event_id = $1 AND channel = $2
	`, eventID, channel, status, errMsg)
	return err
}

// GetByEventID returns the delivery outcome for every channel a given event
// was (or is being) delivered on — this is what GET /api/v1/events/:id/status
// serves.
func (s *Store) GetByEventID(ctx context.Context, eventID string) ([]DeliveryStatus, error) {
	var rows []DeliveryStatus
	err := s.db.SelectContext(ctx, &rows, `
		SELECT id, event_id, user_id, channel, status, attempts, last_error, created_at, updated_at
		FROM delivery_status
		WHERE event_id = $1
		ORDER BY channel
	`, eventID)
	return rows, err
}
