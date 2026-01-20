package sources

import (
	"encoding/json"
	"fmt"
	"time"
)

// Open-Meteo response structure
type WeatherResponse struct {
	Latitude  float64        `json:"latitude"`
	Longitude float64        `json:"longitude"`
	Timezone  string         `json:"timezone"`
	Current   WeatherCurrent `json:"current"`
}

type WeatherCurrent struct {
	Time          string  `json:"time"`
	Temperature   float64 `json:"temperature_2m"`
	Precipitation float64 `json:"precipitation"`
	WeatherCode   int     `json:"weather_code"`
	WindSpeed     float64 `json:"wind_speed_10m"`
}

// WeatherParser parses Open-Meteo weather API responses
type WeatherParser struct{}

func NewWeatherParser() *WeatherParser {
	return &WeatherParser{}
}

func (p *WeatherParser) Parse(data []byte) ([]Event, error) {
	var resp WeatherResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse weather response: %w", err)
	}

	// Parse the time
	eventTime, _ := time.Parse("2006-01-02T15:04", resp.Current.Time)

	// Create unique ID from timestamp
	externalID := fmt.Sprintf("weather-%s", resp.Current.Time)

	event := Event{
		SourceType:  "weather",
		ExternalID:  externalID,
		EventTime:   &eventTime,
		Lat:         &resp.Latitude,
		Lon:         &resp.Longitude,
		Category:    weatherCodeToCategory(resp.Current.WeatherCode),
		Description: fmt.Sprintf("Temp: %.1f°C, Precip: %.1fmm, Wind: %.1f km/h",
			resp.Current.Temperature,
			resp.Current.Precipitation,
			resp.Current.WindSpeed),
		Payload: data,
	}

	return []Event{event}, nil
}

// weatherCodeToCategory converts WMO weather codes to readable categories
func weatherCodeToCategory(code int) string {
	switch {
	case code == 0:
		return "Clear"
	case code >= 1 && code <= 3:
		return "Partly Cloudy"
	case code >= 45 && code <= 48:
		return "Fog"
	case code >= 51 && code <= 57:
		return "Drizzle"
	case code >= 61 && code <= 67:
		return "Rain"
	case code >= 71 && code <= 77:
		return "Snow"
	case code >= 80 && code <= 82:
		return "Rain Showers"
	case code >= 85 && code <= 86:
		return "Snow Showers"
	case code >= 95 && code <= 99:
		return "Thunderstorm"
	default:
		return fmt.Sprintf("Code %d", code)
	}
}
