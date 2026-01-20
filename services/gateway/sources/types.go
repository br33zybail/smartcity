package sources

import (
	"encoding/json"
	"time"
)

// Event represents a unified event from any source
type Event struct {
	SourceType  string          `json:"source_type"`
	ExternalID  string          `json:"external_id"`
	EventTime   *time.Time      `json:"event_time"`
	Lat         *float64        `json:"lat"`
	Lon         *float64        `json:"lon"`
	Category    string          `json:"category"`
	Description string          `json:"description"`
	Payload     json.RawMessage `json:"payload"`
}

// Parser interface for different API types
type Parser interface {
	Parse(data []byte) ([]Event, error)
}
