package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/br33zybail/smartcity/services/gateway/api"
	"github.com/br33zybail/smartcity/services/gateway/sources"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"
)

// ============== Config Types ==============

type Config struct {
	City     string            `yaml:"city"`
	Timezone string            `yaml:"timezone"`
	BBox     BBox              `yaml:"bbox"`
	Sources  map[string]Source `yaml:"sources"`
}

type BBox struct {
	MinLat float64 `yaml:"min_lat"`
	MinLon float64 `yaml:"min_lon"`
	MaxLat float64 `yaml:"max_lat"`
	MaxLon float64 `yaml:"max_lon"`
}

type Source struct {
	URL      string            `yaml:"url"`
	Type     string            `yaml:"type"`
	Interval string            `yaml:"interval"`
	Params   map[string]string `yaml:"params"`
}

// ============== Global State ==============

var (
	dbPool    *pgxpool.Pool
	client    *resty.Client
	apiServer *api.Server
)

// ============== Main ==============

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load config
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/app/config/nashville.yaml"
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Printf("Loaded config for city: %s", cfg.City)

	// Initialize database pool
	dbPool, err = pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	log.Println("Connected to PostGIS")

	// Initialize HTTP client for API calls
	client = resty.New().
		SetRetryCount(3).
		SetRetryWaitTime(2 * time.Second).
		SetTimeout(30 * time.Second)

	// Initialize API server
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	apiServer = api.NewServer(dbPool, redisAddr)
	defer apiServer.Close()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// Start PostgreSQL LISTEN for real-time events
	wg.Add(1)
	go func() {
		defer wg.Done()
		listenForEvents(ctx)
	}()

	// Start per-source ingester goroutines
	for name, src := range cfg.Sources {
		wg.Add(1)
		go func(name string, src Source) {
			defer wg.Done()
			runSourceIngester(ctx, name, src)
		}(name, src)
	}

	// Start HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		runHTTPServer(ctx)
	}()

	log.Printf("%s Data Ingester + API Gateway started", cfg.City)

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutdown signal received")
	cancel()

	// Wait for all goroutines to finish
	wg.Wait()
	log.Println("Shutdown complete")
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ============== HTTP Server ==============

func runHTTPServer(ctx context.Context) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.LoggerWithConfig(gin.LoggerConfig{
		SkipPaths: []string{"/health"},
	}))

	// Setup routes
	apiServer.SetupRoutes(r)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Start server in goroutine
	go func() {
		log.Println("HTTP server listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	log.Println("HTTP server stopped")
}

// ============== PostgreSQL LISTEN ==============

func listenForEvents(ctx context.Context) {
	// Acquire a dedicated connection for LISTEN
	conn, err := dbPool.Acquire(ctx)
	if err != nil {
		log.Printf("Failed to acquire connection for LISTEN: %v", err)
		return
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN new_event")
	if err != nil {
		log.Printf("Failed to LISTEN: %v", err)
		return
	}

	log.Println("Listening for new_event notifications")

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // Context cancelled, shutdown
			}
			log.Printf("LISTEN error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Parse and broadcast the event
		var event map[string]interface{}
		if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
			log.Printf("Failed to parse notification: %v", err)
			continue
		}

		apiServer.BroadcastEvent(event)
	}
}

// ============== Source Ingester ==============

func runSourceIngester(ctx context.Context, name string, src Source) {
	interval, err := time.ParseDuration(src.Interval)
	if err != nil {
		log.Printf("[%s] Invalid interval %q, using 10m", name, src.Interval)
		interval = 10 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("[%s] Starting ingester (interval: %s)", name, interval)

	// Fetch immediately on startup
	fetchAndStore(ctx, name, src)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Stopping", name)
			return
		case <-ticker.C:
			fetchAndStore(ctx, name, src)
		}
	}
}

func fetchAndStore(ctx context.Context, name string, src Source) {
	resp, err := client.R().
		SetQueryParams(src.Params).
		Get(src.URL)

	if err != nil {
		log.Printf("[%s] Fetch error: %v", name, err)
		return
	}
	if resp.StatusCode() != 200 {
		log.Printf("[%s] Bad status: %d", name, resp.StatusCode())
		return
	}

	parser := getParser(name, src.Type)
	events, err := parser.Parse(resp.Body())
	if err != nil {
		log.Printf("[%s] Parse error: %v", name, err)
		return
	}

	if len(events) == 0 {
		log.Printf("[%s] No events fetched", name)
		return
	}

	log.Printf("[%s] Fetched %d events", name, len(events))

	inserted := batchInsertEvents(ctx, events, name)
	log.Printf("[%s] Upserted %d events", name, inserted)
}

func getParser(name, sourceType string) sources.Parser {
	switch sourceType {
	case "arcgis":
		eventType := name
		switch name {
		case "accidents":
			eventType = "accident"
		case "police_dispatch":
			eventType = "police_dispatch"
		case "fire_incidents":
			eventType = "fire_incident"
		}
		return sources.NewArcGISParser(eventType)
	case "weather":
		return sources.NewWeatherParser()
	default:
		log.Printf("[%s] Unknown source type %q, using ArcGIS", name, sourceType)
		return sources.NewArcGISParser(name)
	}
}

func batchInsertEvents(ctx context.Context, events []sources.Event, tag string) int64 {
	batch := &pgx.Batch{}

	const query = `
		INSERT INTO events (source_type, external_id, event_time, geom, lat, lon, category, description, payload)
		VALUES ($1, $2, $3,
			CASE WHEN $4::float IS NOT NULL AND $5::float IS NOT NULL
				THEN ST_SetSRID(ST_MakePoint($5, $4), 4326)
				ELSE NULL
			END,
			$4, $5, $6, $7, $8)
		ON CONFLICT (source_type, external_id) DO UPDATE SET
			event_time = EXCLUDED.event_time,
			geom = EXCLUDED.geom,
			lat = EXCLUDED.lat,
			lon = EXCLUDED.lon,
			category = EXCLUDED.category,
			description = EXCLUDED.description,
			payload = EXCLUDED.payload`

	for _, e := range events {
		batch.Queue(query,
			e.SourceType,
			e.ExternalID,
			e.EventTime,
			e.Lat,
			e.Lon,
			e.Category,
			e.Description,
			e.Payload,
		)
	}

	results := dbPool.SendBatch(ctx, batch)
	defer results.Close()

	var totalAffected int64
	for i := 0; i < batch.Len(); i++ {
		ct, err := results.Exec()
		if err != nil {
			log.Printf("[%s] Batch exec error at index %d: %v", tag, i, err)
			continue
		}
		totalAffected += ct.RowsAffected()
	}
	return totalAffected
}
