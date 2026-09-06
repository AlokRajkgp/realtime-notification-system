package api

import "github.com/gin-gonic/gin"

// NewRouter wires up every route the api binary serves.
func NewRouter(s *Server) *gin.Engine {
	r := gin.Default()

	r.GET("/healthz", s.Healthz)

	v1 := r.Group("/api/v1")
	{
		v1.POST("/events", s.CreateEvent)
	}

	return r
}
