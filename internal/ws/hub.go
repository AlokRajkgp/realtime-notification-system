// Package ws serves the in-app notification bell: a WebSocket endpoint that
// bridges each connected browser to the Redis Pub/Sub channel the InApp
// adapter (internal/adapters) publishes to.
package ws

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	// pongWait is how long we'll wait for a pong (or any other read) before
	// deciding the connection is dead. writeWait bounds how long a single
	// write may take. pingPeriod must be comfortably less than pongWait, so
	// there's at least one full round trip of slack before the deadline.
	pongWait   = 20 * time.Second
	pingPeriod = 15 * time.Second
	writeWait  = 5 * time.Second
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

	// Keepalive: TCP staying "up" only means the OS-level connection hasn't
	// been torn down — it says nothing about whether the other process is
	// still there and reading (a frozen tab, a laptop that went to sleep
	// without closing sockets, a NAT box that silently drops idle mappings
	// all look identical to TCP: still connected). Ping/pong is an
	// application-level heartbeat that actually answers that question.
	//
	// The read deadline starts at pongWait. A browser's WebSocket client
	// answers a Ping automatically at the protocol level (invisible to
	// page JS) — SetPongHandler is how gorilla surfaces that reply to us,
	// and every pong received pushes the deadline forward. If pongWait
	// elapses with no pong, the blocked ReadMessage call below returns a
	// timeout error — which the existing error handling already treats as
	// "connection is gone" via cancel(). No new failure path needed.
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// gorilla/websocket requires the connection to be read continuously to
	// process control frames (ping/pong/close) and notice the browser
	// closing the tab — even though this endpoint never expects an
	// application message from the client. Reading in a goroutine and
	// cancelling ctx on any read error (including a deadline timeout) is
	// the standard way to detect that.
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

	pingTicker := time.NewTicker(pingPeriod)
	defer pingTicker.Stop()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pingTicker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("ws: ping failed user=%s: %v", userID, err)
				return
			}
		case msg, ok := <-ch:
			if !ok {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg.Payload)); err != nil {
				log.Printf("ws: write failed user=%s: %v", userID, err)
				return
			}
		}
	}
}
