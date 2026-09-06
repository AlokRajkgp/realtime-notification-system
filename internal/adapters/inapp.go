package adapters

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"realtime-notification-system/internal/models"
)

// inAppChannel is the Redis Pub/Sub channel a user's live WebSocket
// connection subscribes to (see internal/ws.Hub). Using one channel per
// user means only the API instance actually holding that user's connection
// receives the message, instead of every instance seeing every event.
func inAppChannel(userID string) string {
	return "notify:inapp:" + userID
}

// InApp delivers an event by publishing it to the user's Redis Pub/Sub
// channel. Whichever API instance holds an open WebSocket for that user
// (internal/ws.Hub) is subscribed to the same channel and forwards the
// message to the browser.
//
// Redis Pub/Sub does not queue messages for offline subscribers: if the
// user has no open WebSocket anywhere right now, this publish reaches zero
// subscribers and the notification is simply not seen in-app. That's a
// deliberate limitation of this channel, not a bug — "reach the user even
// if they're offline" is what the email/push channels are for.
type InApp struct {
	rdb *redis.Client
}

func NewInApp(rdb *redis.Client) *InApp {
	return &InApp{rdb: rdb}
}

func (a *InApp) Name() string { return "in-app" }

func (a *InApp) Send(ctx context.Context, event models.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		// A malformed event would fail to marshal identically on every
		// retry -- there's no point trying again, so this is permanent.
		return &PermanentError{Err: fmt.Errorf("marshal event: %w", err)}
	}

	if err := a.rdb.Publish(ctx, inAppChannel(event.UserID), body).Err(); err != nil {
		// Redis being briefly unreachable is exactly the kind of failure
		// that might succeed on the next try -- transient (the default).
		return fmt.Errorf("publish to redis: %w", err)
	}
	return nil
}
