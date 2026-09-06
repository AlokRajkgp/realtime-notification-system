package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"realtime-notification-system/internal/db"
	"realtime-notification-system/internal/kafkaclient"
	"realtime-notification-system/internal/models"
)

// Server bundles the dependencies HTTP handlers need.
type Server struct {
	producer *kafkaclient.Producer
	store    *db.Store
}

// NewServer builds a Server.
func NewServer(producer *kafkaclient.Producer, store *db.Store) *Server {
	return &Server{producer: producer, store: store}
}

// createEventRequest is the shape a client POSTs to /api/v1/events.
// EventID is optional — the caller can supply their own for idempotency
// (e.g. an ID derived from an upstream business event), or leave it blank
// and let the API generate one.
type createEventRequest struct {
	EventID string         `json:"event_id"`
	UserID  string         `json:"user_id" binding:"required"`
	Type    string         `json:"type" binding:"required"`
	Payload map[string]any `json:"payload"`
}

// CreateEvent validates an incoming event and publishes it to Kafka,
// partitioned by user_id. It responds 202 Accepted: publishing to Kafka is
// not the same as the event being delivered to a channel — that happens
// asynchronously in the consumer/worker (a later step).
func (s *Server) CreateEvent(c *gin.Context) {
	var req createEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	eventID := req.EventID
	if eventID == "" {
		eventID = uuid.NewString()
	}

	event := models.Event{
		EventID:   eventID,
		UserID:    req.UserID,
		Type:      req.Type,
		Payload:   req.Payload,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.producer.Publish(c.Request.Context(), event); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish event"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"event_id": event.EventID, "status": "accepted"})
}

// GetEventStatus returns the delivery outcome for every channel a given
// event was (or is being) delivered on. An event that was just published
// and hasn't reached the worker yet legitimately returns an empty list —
// this endpoint doesn't distinguish "not delivered yet" from "unknown event
// ID" (there's no events table to check against yet).
func (s *Server) GetEventStatus(c *gin.Context) {
	eventID := c.Param("event_id")

	rows, err := s.store.GetByEventID(c.Request.Context(), eventID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query delivery status"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"event_id": eventID, "deliveries": rows})
}

// Healthz is a liveness probe for the API process itself. It deliberately
// does not check downstream dependencies (Kafka/Postgres/Redis) — that's
// what a readiness probe would do, added alongside those integrations later.
func (s *Server) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
