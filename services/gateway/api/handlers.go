package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server holds API dependencies
type Server struct {
	DB          *pgxpool.Pool
	AsynqClient *asynq.Client
	wsClients   map[*websocket.Conn]bool
	wsMutex     sync.RWMutex
	upgrader    websocket.Upgrader
}

// NewServer creates a new API server
func NewServer(db *pgxpool.Pool, redisAddr string) *Server {
	return &Server{
		DB:          db,
		AsynqClient: asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr}),
		wsClients:   make(map[*websocket.Conn]bool),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// SetupRoutes configures all API routes
func (s *Server) SetupRoutes(r *gin.Engine) {
	r.GET("/health", s.Health)
	r.GET("/events", s.GetEvents)
	r.GET("/ws/events", s.WebSocketHandler)
	r.POST("/query", s.EnqueueAIQuery)
}

// Health returns service health status
func (s *Server) Health(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	if err := s.DB.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"db":     "disconnected",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
		"db":     "connected",
	})
}

// EventResponse represents an event for API responses
type EventResponse struct {
	ID          int64           `json:"id"`
	SourceType  string          `json:"source_type"`
	ExternalID  string          `json:"external_id"`
	EventTime   *time.Time      `json:"event_time"`
	Lat         *float64        `json:"lat"`
	Lon         *float64        `json:"lon"`
	Category    string          `json:"category"`
	Description string          `json:"description"`
	Payload     json.RawMessage `json:"payload"`
	IngestedAt  time.Time       `json:"ingested_at"`
}

// GetEvents returns recent events within radius of lat/lon
func (s *Server) GetEvents(c *gin.Context) {
	lat, _ := strconv.ParseFloat(c.DefaultQuery("lat", "36.1627"), 64)
	lon, _ := strconv.ParseFloat(c.DefaultQuery("lon", "-86.7816"), 64)
	radiusMeters, _ := strconv.ParseFloat(c.DefaultQuery("radius", "5000"), 64)
	hours, _ := strconv.Atoi(c.DefaultQuery("hours", "24"))
	sourceType := c.Query("source")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	query := `
		SELECT id, source_type, external_id, event_time, lat, lon, category, description, payload, ingested_at
		FROM events
		WHERE event_time > NOW() - INTERVAL '1 hour' * $1
		AND ($4 = '' OR source_type = $4)
		AND (
			(lat IS NOT NULL AND lon IS NOT NULL AND 
			 ST_DWithin(
				geom::geography,
				ST_SetSRID(ST_MakePoint($3, $2), 4326)::geography,
				$5
			 ))
			OR (lat IS NULL AND lon IS NULL)
		)
		ORDER BY event_time DESC
		LIMIT $6`

	rows, err := s.DB.Query(ctx, query, hours, lat, lon, sourceType, radiusMeters, limit)
	if err != nil {
		log.Printf("Query error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database query failed"})
		return
	}
	defer rows.Close()

	events := make([]EventResponse, 0)
	for rows.Next() {
		var e EventResponse
		err := rows.Scan(&e.ID, &e.SourceType, &e.ExternalID, &e.EventTime,
			&e.Lat, &e.Lon, &e.Category, &e.Description, &e.Payload, &e.IngestedAt)
		if err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		events = append(events, e)
	}

	c.JSON(http.StatusOK, gin.H{
		"count":  len(events),
		"events": events,
	})
}

// WebSocketHandler handles WebSocket connections for real-time events
func (s *Server) WebSocketHandler(c *gin.Context) {
	conn, err := s.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	s.wsMutex.Lock()
	s.wsClients[conn] = true
	s.wsMutex.Unlock()

	log.Printf("WebSocket client connected, total: %d", len(s.wsClients))

	conn.WriteJSON(gin.H{"type": "connected", "message": "Subscribed to events"})

	defer func() {
		s.wsMutex.Lock()
		delete(s.wsClients, conn)
		s.wsMutex.Unlock()
		conn.Close()
		log.Printf("WebSocket client disconnected, total: %d", len(s.wsClients))
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// BroadcastEvent sends an event to all WebSocket clients
func (s *Server) BroadcastEvent(event interface{}) {
	s.wsMutex.RLock()
	defer s.wsMutex.RUnlock()

	msg, _ := json.Marshal(gin.H{
		"type":  "new-event",
		"event": event,
	})

	for client := range s.wsClients {
		err := client.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			log.Printf("WebSocket write error: %v", err)
		}
	}
}

// AIQueryRequest represents a natural language query
type AIQueryRequest struct {
	Query string `json:"query" binding:"required"`
}

// EnqueueAIQuery queues a query for the Python AI agent
func (s *Server) EnqueueAIQuery(c *gin.Context) {
	var req AIQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "query is required"})
		return
	}

	payload, _ := json.Marshal(req)
	task := asynq.NewTask("ai:query", payload)

	info, err := s.AsynqClient.Enqueue(task, asynq.Queue("ai"), asynq.MaxRetry(3))
	if err != nil {
		log.Printf("Failed to enqueue AI query: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue query"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"status":  "queued",
		"task_id": info.ID,
		"queue":   info.Queue,
	})
}

// Close cleans up resources
func (s *Server) Close() {
	if s.AsynqClient != nil {
		s.AsynqClient.Close()
	}
}
