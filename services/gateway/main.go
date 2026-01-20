package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/joho/godotenv"
)

type ArcGISResponse struct {
	Features []Feature `json:"features"`
}

type Feature struct {
	Attributes Accident `json:"attributes"`
	Geometry   Geometry `json:"geometry"`
}

type Accident struct {
	AccidentNumber float64 `json:"Accident_Number"`
	DateTime       int64   `json:"Date_and_Time"`
}

type Geometry struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

func main() {
	// Load env (API keys later for LLMs, etc)
	if err := godotenv.Load("../../.env"); err != nil {
		log.Println("No .env file found (ok for now)")
	}

	client := resty.New().
		SetRetryCount(3).
		SetRetryWaitTime(2 * time.Second)

	ticker := time.NewTicker(5 * time.Minute) // Polling every 5 minutes, can adjust as needed
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	fmt.Println("Nashville Traffic Ingester starting...")

	// Fetch immediately on startup
	fetchRecentAccidents(client)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutting down ingester")
			return
		case <-ticker.C:
			fetchRecentAccidents(client)
		}
	}
}

func fetchRecentAccidents(client *resty.Client) {
	const baseURL = "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Traffic_Accidents_2/FeatureServer/0/query"

	resp, err := client.R().
		SetQueryParams(map[string]string{
			"where":     "1=1",
			"outFields": "*",
			"outSR":     "4326",
			"f":         "json",
			"resultRecordCount": "50",
			"orderByFields":     "Date_and_Time DESC",
		}).
		SetResult(&ArcGISResponse{}).
		Get(baseURL)

	if err != nil {
		log.Printf("Error fetching accidents: %v", err)
		return
	}

	if resp.StatusCode() != 200 {
		log.Printf("Bad status: %d - %s", resp.StatusCode(), resp.String())
		return
	}

	result, ok := resp.Result().(*ArcGISResponse)
	if !ok {
		log.Println("Failed to parse response")
		return
	}

	fmt.Printf("Fetched %d accidents\n", len(result.Features))
	for _, f := range result.Features {
		// DateTime is epoch milliseconds
		t := time.UnixMilli(f.Attributes.DateTime)
		fmt.Printf("- %.0f @ %s (%.6f, %.6f)\n", f.Attributes.AccidentNumber, t.Format(time.RFC3339), f.Geometry.Y, f.Geometry.X)
	}
}
