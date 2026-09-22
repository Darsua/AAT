-- Migration 001_init.sql: Core Aggregator schema and volcano reference seed data
-- Database target: PostgreSQL 16 Alpine

-- 1. Hazards Table
CREATE TABLE IF NOT EXISTS hazards (
    hazard_id VARCHAR(64) PRIMARY KEY,
    source VARCHAR(32) NOT NULL,
    source_ref_id VARCHAR(64) NOT NULL,
    hazard_type VARCHAR(32) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    area_name TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_source_ref UNIQUE (source, source_ref_id)
);

-- Indexes for hazards query performance
CREATE INDEX IF NOT EXISTS idx_hazards_occurred_at ON hazards(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hazards_type_occurred ON hazards(hazard_type, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hazards_severity_occurred ON hazards(severity, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hazards_attributes_gin ON hazards USING gin(attributes);

-- 2. Volcano References Table
CREATE TABLE IF NOT EXISTS volcano_references (
    volcano_id VARCHAR(32) PRIMARY KEY,
    volcano_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

-- Seed data for 6 Indonesian volcanoes
INSERT INTO volcano_references (volcano_id, volcano_name, latitude, longitude) VALUES
    ('VLC-001', 'Gunung Merapi', -7.5407, 110.4456),
    ('VLC-002', 'Gunung Anak Krakatau', -6.1020, 105.4230),
    ('VLC-003', 'Gunung Semeru', -8.1077, 112.9224),
    ('VLC-004', 'Gunung Marapi', -0.3811, 100.4739),
    ('VLC-005', 'Gunung Sinabung', 3.1694, 98.3925),
    ('VLC-006', 'Gunung Ruang', 2.3014, 125.3697)
ON CONFLICT (volcano_id) DO UPDATE SET
    volcano_name = EXCLUDED.volcano_name,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;

-- 3. Transactional Outbox Table
CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

-- Partial index for high-throughput polling of unpublished events
CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON outbox_events(id) WHERE published = FALSE;
