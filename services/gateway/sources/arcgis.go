package sources

import (
	"encoding/json"
	"fmt"
	"time"
)

// ArcGIS response structures
type ArcGISResponse struct {
	Features []ArcGISFeature `json:"features"`
}

type ArcGISFeature struct {
	Attributes map[string]interface{} `json:"attributes"`
	Geometry   *ArcGISGeometry        `json:"geometry"`
}

type ArcGISGeometry struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// ArcGISParser parses ArcGIS FeatureServer responses
type ArcGISParser struct {
	SourceType string
}

func NewArcGISParser(sourceType string) *ArcGISParser {
	return &ArcGISParser{SourceType: sourceType}
}

func (p *ArcGISParser) Parse(data []byte) ([]Event, error) {
	var resp ArcGISResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse ArcGIS response: %w", err)
	}

	events := make([]Event, 0, len(resp.Features))
	for _, f := range resp.Features {
		event := p.featureToEvent(f)
		events = append(events, event)
	}
	return events, nil
}

func (p *ArcGISParser) featureToEvent(f ArcGISFeature) Event {
	event := Event{
		SourceType: p.SourceType,
	}

	// Extract geometry
	if f.Geometry != nil {
		lat := f.Geometry.Y
		lon := f.Geometry.X
		event.Lat = &lat
		event.Lon = &lon
	}

	// Source-specific field mapping
	switch p.SourceType {
	case "accident":
		event.ExternalID = getStringOrFloat(f.Attributes, "Accident_Number")
		event.EventTime = getEpochTime(f.Attributes, "Date_and_Time")
		event.Category = getStringField(f.Attributes, "Collision_Type_Description")
		event.Description = getStringField(f.Attributes, "Weather_Description")

	case "police_dispatch":
		event.ExternalID = fmt.Sprintf("%v", f.Attributes["ObjectId"])
		event.EventTime = getEpochTime(f.Attributes, "CallReceivedTime")
		event.Category = getStringField(f.Attributes, "IncidentTypeCode")
		event.Description = getStringField(f.Attributes, "IncidentTypeName")
		// Use location for lat/lon if geometry missing
		if event.Lat == nil {
			event.Description = fmt.Sprintf("%s - %s", event.Description, getStringField(f.Attributes, "Location"))
		}

	case "fire_incident":
		event.ExternalID = fmt.Sprintf("%v", f.Attributes["ObjectId"])
		event.EventTime = getEpochTime(f.Attributes, "DispatchDateTime")
		event.Category = getStringField(f.Attributes, "incident_type_id")
		event.Description = fmt.Sprintf("Unit: %s, Event: %s",
			getStringField(f.Attributes, "Unit_ID"),
			getStringField(f.Attributes, "event_number"))
	}

	// Store full payload as JSON
	if payload, err := json.Marshal(f.Attributes); err == nil {
		event.Payload = payload
	}

	return event
}

// Helper functions for extracting typed values from map[string]interface{}

func getStringField(attrs map[string]interface{}, key string) string {
	if v, ok := attrs[key]; ok && v != nil {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func getStringOrFloat(attrs map[string]interface{}, key string) string {
	if v, ok := attrs[key]; ok && v != nil {
		switch val := v.(type) {
		case float64:
			return fmt.Sprintf("%.0f", val)
		case string:
			return val
		default:
			return fmt.Sprintf("%v", val)
		}
	}
	return ""
}

func getEpochTime(attrs map[string]interface{}, key string) *time.Time {
	if v, ok := attrs[key]; ok && v != nil {
		switch val := v.(type) {
		case float64:
			t := time.UnixMilli(int64(val))
			return &t
		case int64:
			t := time.UnixMilli(val)
			return &t
		}
	}
	return nil
}
