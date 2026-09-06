// Package ws serves the in-app notification bell: a WebSocket endpoint that
// bridges each connected browser to the Redis Pub/Sub channel the InApp
// adapter (internal/adapters) publishes to.
package ws

import (
	"context"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	// Dev/portfolio default: accept any origin. Before this ever serves
	// real traffic, CheckOrigin needs to validate against an allowlist.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Hub upgrades incoming requests to WebSocket connections. It holds no
// per-connection state itself beyond the connection's lifetime — routing
// is entirely delegated to Redis Pub/Sub, so any number of API instances
// can each run a Hub without coordinating with each other.
type Hub struct {
	rdb *redis.Client
}

func NewHub(rdb *redis.Client) *Hub {
	return &Hub{rdb: rdb}
}

// ServeWS handles GET /ws?user_id=... . There's no auth system yet, so
// user_id is trusted as given in the query string — fine for local
// development and demos, not for anything real. Wiring this to actual
// session/token auth is a later step.
func (h *Hub) ServeWS(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id query param required"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("ws: upgrade failed user=%s: %v", userID, err)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()

	sub := h.rdb.Subscribe(ctx, "notify:inapp:"+userID)
	defer sub.Close()

	// gorilla/websocket requires the connection to be read continuously to
	// process control frames (ping/pong/close) and notice the browser
	// closing the tab — even though this endpoint never expects an
	// application message from the client. Reading in a goroutine and
	// cancelling ctx on any read error is the standard way to detect that.
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				cancel()
				return
			}
		}
	}()

	log.Printf("ws: user=%s connected", userID)
	defer log.Printf("ws: user=%s disconnected", userID)

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg.Payload)); err != nil {
				log.Printf("ws: write failed user=%s: %v", userID, err)
				return
			}
		}
	}
}
