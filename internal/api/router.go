package api

import (
	"github.com/gin-gonic/gin"

	"realtime-notification-system/internal/ws"
)

// NewRouter wires up every route the api binary serves.
func NewRouter(s *Server, hub *ws.Hub) *gin.Engine {
	r := gin.Default()

	r.GET("/healthz", s.Healthz)
	r.GET("/ws", hub.ServeWS)

	v1 := r.Group("/api/v1")
	{
		v1.POST("/events", s.CreateEvent)
		v1.GET("/events/:event_id/status", s.GetEventStatus)

		v1.GET("/users/:user_id/preferences", s.GetPreferences)
		v1.PUT("/users/:user_id/preferences/:channel", s.SetChannelPreference)
		v1.PUT("/users/:user_id/dnd", s.SetDND)
		v1.DELETE("/users/:user_id/dnd", s.ClearDND)
	}

	return r
}
