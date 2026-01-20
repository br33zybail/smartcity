-- Unified events table for all data sources
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,           -- 'accident', 'police_dispatch', 'fire_incident', 'weather'
    external_id TEXT,                    -- Original ID from source (accident_number, object_id, etc.)
    event_time TIMESTAMP WITH TIME ZONE, -- When the event occurred
    geom GEOMETRY(POINT, 4326),          -- PostGIS point geometry
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    category TEXT,                       -- Incident type, weather code, etc.
    description TEXT,                    -- Human-readable description
    payload JSONB,                       -- Full raw data from source
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (source_type, external_id)    -- Prevent duplicates per source
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_events_geom ON events USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_events_time ON events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_source ON events (source_type);
CREATE INDEX IF NOT EXISTS idx_events_category ON events (category);
CREATE INDEX IF NOT EXISTS idx_events_payload ON events USING GIN (payload);

-- Keep the old tables for now (can drop later after migration)
-- DROP TABLE IF EXISTS accidents, dispatch, fire_incidents;
