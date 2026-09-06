// Package preferences decides whether a user should currently receive a
// delivery on a given channel, combining per-channel opt-in/out with a
// do-not-disturb time window. The API package does plain CRUD against
// internal/db directly; this package is where the two settings are
// actually applied together, so only the worker needs to import it.
package preferences

import (
	"context"
	"time"

	"realtime-notification-system/internal/db"
)

// Checker wraps a db.Store with the combined opt-in/out + DND decision.
type Checker struct {
	store *db.Store
}

func NewChecker(store *db.Store) *Checker {
	return &Checker{store: store}
}

// Allowed reports whether userID should be delivered to on channel right
// now: the channel must be enabled, and the current time (in the user's
// configured DND timezone) must fall outside their DND window, if they
// have one.
func (c *Checker) Allowed(ctx context.Context, userID, channel string) (bool, error) {
	enabled, err := c.store.ChannelEnabled(ctx, userID, channel)
	if err != nil {
		return false, err
	}
	if !enabled {
		return false, nil
	}

	win, err := c.store.GetDNDWindow(ctx, userID)
	if err != nil {
		return false, err
	}
	if win == nil {
		return true, nil
	}

	return !inDNDWindow(time.Now(), *win), nil
}

// inDNDWindow reports whether now, converted into win's timezone, falls
// within [win.StartMin, win.EndMin). EndMin may be less than StartMin,
// meaning the window wraps past midnight (e.g. 22:00-07:00).
func inDNDWindow(now time.Time, win db.DNDWindow) bool {
	loc, err := time.LoadLocation(win.Timezone)
	if err != nil {
		// An invalid/unknown timezone was somehow stored — fail safe by
		// treating it as "not in DND" (deliver) rather than silently
		// suppressing every delivery for this user until it's fixed.
		return false
	}

	local := now.In(loc)
	nowMin := local.Hour()*60 + local.Minute()

	if win.StartMin == win.EndMin {
		return false // zero-width window means "no DND"
	}
	if win.StartMin < win.EndMin {
		return nowMin >= win.StartMin && nowMin < win.EndMin
	}
	// Wraps past midnight.
	return nowMin >= win.StartMin || nowMin < win.EndMin
}
