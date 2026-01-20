package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ============== API Response Types ==============

type AccidentsResponse struct {
	Features []AccidentFeature `json:"features"`
}

type AccidentFeature struct {
	Attributes AccidentAttributes `json:"attributes"`
	Geometry   Geometry           `json:"geometry"`
}

type AccidentAttributes struct {
	AccidentNumber   float64 `json:"Accident_Number"`
	DateTime         int64   `json:"Date_and_Time"`
	CollisionType    *string `json:"Collision_Type_Description"`
	WeatherCondition *string `json:"Weather_Description"`
}

type Geometry struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type DispatchResponse struct {
	Features []DispatchFeature `json:"features"`
}

type DispatchFeature struct {
	Attributes DispatchAttributes `json:"attributes"`
}

type DispatchAttributes struct {
	ObjectId            int64   `json:"ObjectId"`
	IncidentTypeCode    *string `json:"IncidentTypeCode"`
	IncidentTypeName    *string `json:"IncidentTypeName"`
	CallReceivedTime    *int64  `json:"CallReceivedTime"`
	Location            *string `json:"Location"`
	LocationDescription *string `json:"LocationDescription"`
	CityName            *string `json:"CityName"`
	LastUpdated         *int64  `json:"LastUpdated"`
}

type FireResponse struct {
	Features []FireFeature `json:"features"`
}

type FireFeature struct {
	Attributes FireAttributes `json:"attributes"`
}

type FireAttributes struct {
	ObjectId         int64   `json:"ObjectId"`
	EventNumber      *string `json:"event_number"`
	UnitID           *string `json:"Unit_ID"`
	IncidentTypeID   *string `json:"incident_type_id"`
	DispatchDateTime *int64  `json:"DispatchDateTime"`
	PostalCode       *string `json:"PostalCode"`
}

// ============== Global State ==============

var (
	dbPool *pgxpool.Pool
	client *resty.Client
)

// ============== Main ==============

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize database pool
	var err error
	dbPool, err = pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	log.Println("Connected to PostGIS")

	// Initialize HTTP client
	client = resty.New().
		SetRetryCount(3).
		SetRetryWaitTime(2 * time.Second).
		SetTimeout(30 * time.Second)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigChan; cancel() }()

	log.Println("Nashville Data Ingester started")

	// Fetch immediately on startup
	fetchAll(ctx)

	// Poll every 10 minutes
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down")
			return
		case <-ticker.C:
			fetchAll(ctx)
		}
	}
}

// ============== Fetch Orchestration ==============

func fetchAll(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		fetchAccidents(ctx)
	}()

	go func() {
		defer wg.Done()
		fetchDispatch(ctx)
	}()

	go func() {
		defer wg.Done()
		fetchFire(ctx)
	}()

	wg.Wait()
	log.Println("All data sources fetched")
}

// ============== Accidents ==============

func fetchAccidents(ctx context.Context) {
	const url = "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Traffic_Accidents_2/FeatureServer/0/query"

	var result AccidentsResponse
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"where":             "1=1",
			"outFields":         "*",
			"outSR":             "4326",
			"f":                 "json",
			"resultRecordCount": "100",
			"orderByFields":     "Date_and_Time DESC",
		}).
		SetResult(&result).
		Get(url)

	if err != nil {
		log.Printf("[Accidents] Fetch error: %v", err)
		return
	}
	if resp.StatusCode() != 200 {
		log.Printf("[Accidents] Bad status: %d", resp.StatusCode())
		return
	}

	if len(result.Features) == 0 {
		log.Println("[Accidents] No records fetched")
		return
	}

	log.Printf("[Accidents] Fetched %d records", len(result.Features))

	// Batch insert
	batch := &pgx.Batch{}
	const query = `INSERT INTO accidents (accident_number, date_time, geom, latitude, longitude, collision_type, weather_condition)
		VALUES ($1, $2, ST_SetSRID(ST_MakePoint($4, $3), 4326), $3, $4, $5, $6)
		ON CONFLICT (accident_number) DO NOTHING`

	for _, f := range result.Features {
		batch.Queue(query,
			fmt.Sprintf("%.0f", f.Attributes.AccidentNumber),
			time.UnixMilli(f.Attributes.DateTime),
			f.Geometry.Y,
			f.Geometry.X,
			f.Attributes.CollisionType,
			f.Attributes.WeatherCondition,
		)
	}

	inserted := executeBatch(ctx, batch, "[Accidents]")
	log.Printf("[Accidents] Inserted %d new records", inserted)
}

// ============== Dispatch ==============

func fetchDispatch(ctx context.Context) {
	const url = "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Metro_Nashville_Police_Department_Active_Dispatch_Table_view/FeatureServer/0/query"

	var result DispatchResponse
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"where":     "1=1",
			"outFields": "*",
			"outSR":     "4326",
			"f":         "json",
		}).
		SetResult(&result).
		Get(url)

	if err != nil {
		log.Printf("[Dispatch] Fetch error: %v", err)
		return
	}
	if resp.StatusCode() != 200 {
		log.Printf("[Dispatch] Bad status: %d", resp.StatusCode())
		return
	}

	if len(result.Features) == 0 {
		log.Println("[Dispatch] No records fetched")
		return
	}

	log.Printf("[Dispatch] Fetched %d records", len(result.Features))

	// Batch upsert
	batch := &pgx.Batch{}
	const query = `INSERT INTO dispatch (object_id, incident_type_code, incident_type_name, call_received_time, location, location_description, city_name, last_updated)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (object_id) DO UPDATE SET
			incident_type_code = EXCLUDED.incident_type_code,
			incident_type_name = EXCLUDED.incident_type_name,
			call_received_time = EXCLUDED.call_received_time,
			location = EXCLUDED.location,
			location_description = EXCLUDED.location_description,
			city_name = EXCLUDED.city_name,
			last_updated = EXCLUDED.last_updated`

	for _, f := range result.Features {
		batch.Queue(query,
			f.Attributes.ObjectId,
			f.Attributes.IncidentTypeCode,
			f.Attributes.IncidentTypeName,
			epochToTime(f.Attributes.CallReceivedTime),
			f.Attributes.Location,
			f.Attributes.LocationDescription,
			f.Attributes.CityName,
			epochToTime(f.Attributes.LastUpdated),
		)
	}

	upserted := executeBatch(ctx, batch, "[Dispatch]")
	log.Printf("[Dispatch] Upserted %d records", upserted)
}

// ============== Fire ==============

func fetchFire(ctx context.Context) {
	const url = "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Nashville_Fire_Department_Active_Incidents_view/FeatureServer/0/query"

	var result FireResponse
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"where":     "1=1",
			"outFields": "*",
			"outSR":     "4326",
			"f":         "json",
		}).
		SetResult(&result).
		Get(url)

	if err != nil {
		log.Printf("[Fire] Fetch error: %v", err)
		return
	}
	if resp.StatusCode() != 200 {
		log.Printf("[Fire] Bad status: %d", resp.StatusCode())
		return
	}

	if len(result.Features) == 0 {
		log.Println("[Fire] No records fetched")
		return
	}

	log.Printf("[Fire] Fetched %d records", len(result.Features))

	// Batch upsert
	batch := &pgx.Batch{}
	const query = `INSERT INTO fire_incidents (object_id, event_number, unit_id, incident_type_id, dispatch_time, postal_code)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (object_id) DO UPDATE SET
			event_number = EXCLUDED.event_number,
			unit_id = EXCLUDED.unit_id,
			incident_type_id = EXCLUDED.incident_type_id,
			dispatch_time = EXCLUDED.dispatch_time,
			postal_code = EXCLUDED.postal_code`

	for _, f := range result.Features {
		batch.Queue(query,
			f.Attributes.ObjectId,
			f.Attributes.EventNumber,
			f.Attributes.UnitID,
			f.Attributes.IncidentTypeID,
			epochToTime(f.Attributes.DispatchDateTime),
			f.Attributes.PostalCode,
		)
	}

	upserted := executeBatch(ctx, batch, "[Fire]")
	log.Printf("[Fire] Upserted %d records", upserted)
}

// ============== Helpers ==============

// epochToTime converts millisecond epoch to *time.Time (nil-safe)
func epochToTime(epoch *int64) *time.Time {
	if epoch == nil {
		return nil
	}
	t := time.UnixMilli(*epoch)
	return &t
}

// executeBatch sends a batch to the database and returns rows affected
func executeBatch(ctx context.Context, batch *pgx.Batch, tag string) int64 {
	results := dbPool.SendBatch(ctx, batch)
	defer results.Close()

	var totalAffected int64
	for i := 0; i < batch.Len(); i++ {
		ct, err := results.Exec()
		if err != nil {
			log.Printf("%s Batch exec error at index %d: %v", tag, i, err)
			continue
		}
		totalAffected += ct.RowsAffected()
	}
	return totalAffected
}
