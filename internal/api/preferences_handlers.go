package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"realtime-notification-system/internal/db"
)

// GetPreferences returns every channel the user has an explicit opt-in/out
// row for (any channel not listed is implicitly enabled — see the
// migration) plus their DND window, if they have one.
func (s *Server) GetPreferences(c *gin.Context) {
	ctx := c.Request.Context()
	userID := c.Param("user_id")

	prefs, err := s.store.ListPreferences(ctx, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load preferences"})
		return
	}

	win, err := s.store.GetDNDWindow(ctx, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load dnd window"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id":  userID,
		"channels": prefs,
		"dnd":      dndResponse(win),
	})
}

type setChannelRequest struct {
	Enabled bool `json:"enabled"`
}

// SetChannelPreference opts a user in or out of one channel.
func (s *Server) SetChannelPreference(c *gin.Context) {
	userID := c.Param("user_id")
	channel := c.Param("channel")

	var req setChannelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.store.SetChannelEnabled(c.Request.Context(), userID, channel, req.Enabled); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save preference"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"user_id": userID, "channel": channel, "enabled": req.Enabled})
}

type setDNDRequest struct {
	Start    string `json:"start" binding:"required"` // "HH:MM", 24h
	End      string `json:"end" binding:"required"`
	Timezone string `json:"timezone"` // IANA name, e.g. "Asia/Kolkata"; defaults to UTC
}

// SetDND sets (or replaces) a user's do-not-disturb window: no channel is
// delivered while the current time, in Timezone, falls between Start and
// End. End may be earlier than Start (e.g. "22:00"-"07:00") to mean a
// window that wraps past midnight.
func (s *Server) SetDND(c *gin.Context) {
	userID := c.Param("user_id")

	var req setDNDRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	startMin, err := parseHHMM(req.Start)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid start: " + err.Error()})
		return
	}
	endMin, err := parseHHMM(req.End)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid end: " + err.Error()})
		return
	}

	tz := req.Timezone
	if tz == "" {
		tz = "UTC"
	}
	if _, err := time.LoadLocation(tz); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timezone: " + err.Error()})
		return
	}

	if err := s.store.SetDNDWindow(c.Request.Context(), userID, startMin, endMin, tz); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save dnd window"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"user_id": userID, "start": req.Start, "end": req.End, "timezone": tz})
}

// ClearDND removes a user's DND window entirely (no quiet hours).
func (s *Server) ClearDND(c *gin.Context) {
	userID := c.Param("user_id")

	if err := s.store.ClearDNDWindow(c.Request.Context(), userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to clear dnd window"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"user_id": userID, "dnd": nil})
}

func parseHHMM(s string) (int, error) {
	t, err := time.Parse("15:04", s)
	if err != nil {
		return 0, err
	}
	return t.Hour()*60 + t.Minute(), nil
}

func dndResponse(win *db.DNDWindow) any {
	if win == nil {
		return nil
	}
	return gin.H{
		"start":    fmtHHMM(win.StartMin),
		"end":      fmtHHMM(win.EndMin),
		"timezone": win.Timezone,
	}
}

func fmtHHMM(mins int) string {
	return fmt.Sprintf("%02d:%02d", mins/60, mins%60)
}
