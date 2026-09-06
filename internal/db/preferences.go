package db

import (
	"context"
	"database/sql"
)

// ChannelPreference mirrors one row of user_preferences.
type ChannelPreference struct {
	Channel string `db:"channel" json:"channel"`
	Enabled bool   `db:"enabled" json:"enabled"`
}

// DNDWindow mirrors one row of user_dnd_windows. Start/End are
// minutes-since-midnight in Timezone; see the migration for why.
type DNDWindow struct {
	StartMin int    `db:"start_min" json:"start_min"`
	EndMin   int    `db:"end_min" json:"end_min"`
	Timezone string `db:"timezone" json:"timezone"`
}

// ChannelEnabled reports whether userID currently has channel enabled.
// No row for (userID, channel) means enabled — see the migration comment.
func (s *Store) ChannelEnabled(ctx context.Context, userID, channel string) (bool, error) {
	var enabled bool
	err := s.db.GetContext(ctx, &enabled, `
		SELECT enabled FROM user_preferences WHERE user_id = $1 AND channel = $2
	`, userID, channel)

	switch {
	case err == sql.ErrNoRows:
		return true, nil
	case err != nil:
		return false, err
	default:
		return enabled, nil
	}
}

// SetChannelEnabled opts userID in or out of channel.
func (s *Store) SetChannelEnabled(ctx context.Context, userID, channel string, enabled bool) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO user_preferences (user_id, channel, enabled, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (user_id, channel) DO UPDATE SET enabled = $3, updated_at = now()
	`, userID, channel, enabled)
	return err
}

// ListPreferences returns every channel userID has an explicit row for.
// A channel absent from this list is implicitly enabled.
func (s *Store) ListPreferences(ctx context.Context, userID string) ([]ChannelPreference, error) {
	var rows []ChannelPreference
	err := s.db.SelectContext(ctx, &rows, `
		SELECT channel, enabled FROM user_preferences WHERE user_id = $1 ORDER BY channel
	`, userID)
	return rows, err
}

// GetDNDWindow returns userID's DND window, or nil if they don't have one
// configured (meaning: never in DND).
func (s *Store) GetDNDWindow(ctx context.Context, userID string) (*DNDWindow, error) {
	var win DNDWindow
	err := s.db.GetContext(ctx, &win, `
		SELECT start_min, end_min, timezone FROM user_dnd_windows WHERE user_id = $1
	`, userID)

	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	default:
		return &win, nil
	}
}

// SetDNDWindow sets (or replaces) userID's DND window.
func (s *Store) SetDNDWindow(ctx context.Context, userID string, startMin, endMin int, timezone string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO user_dnd_windows (user_id, start_min, end_min, timezone, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (user_id) DO UPDATE SET start_min = $2, end_min = $3, timezone = $4, updated_at = now()
	`, userID, startMin, endMin, timezone)
	return err
}

// ClearDNDWindow removes userID's DND window entirely.
func (s *Store) ClearDNDWindow(ctx context.Context, userID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM user_dnd_windows WHERE user_id = $1`, userID)
	return err
}
