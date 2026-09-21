package domain

import (
	"encoding/json"
	"time"
)

// Source represents the upstream monitoring agency.
type Source string

const (
	SourceBMKG  Source = "BMKG"
	SourcePVMBG Source = "PVMBG"
)

// HazardType represents the domain classification of the natural disaster.
type HazardType string

const (
	HazardTypeSeismic  HazardType = "SEISMIC"
	HazardTypeVolcanic HazardType = "VOLCANIC"
)

// Severity represents the standardized national alert level.
type Severity string

const (
	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

// HazardEvent represents the canonical disaster event model inside the BNPB ecosystem.
// Classified into Summary Fields (accessible by Media) and Raw Fields (rejected with 403 for Media).
type HazardEvent struct {
	// Summary Fields (Accessible by Media)
	HazardID   string     `json:"hazard_id" db:"hazard_id"`
	Source     Source     `json:"source" db:"source"`
	HazardType HazardType `json:"hazard_type" db:"hazard_type"`
	Severity   Severity   `json:"severity" db:"severity"`
	AreaName   string     `json:"area_name" db:"area_name"`
	OccurredAt time.Time  `json:"occurred_at" db:"occurred_at"`
	IngestedAt time.Time  `json:"ingested_at" db:"ingested_at"`

	// Raw Fields (Restricted to Field Responder and Internal roles)
	SourceRefID string         `json:"source_ref_id" db:"source_ref_id"`
	Latitude    *float64       `json:"latitude,omitempty" db:"latitude"`
	Longitude   *float64       `json:"longitude,omitempty" db:"longitude"`
	Attributes  map[string]any `json:"attributes" db:"attributes"` // Dynamic schemaless bag (e.g. confidence_level, depth_km, magnitude)
}

// VolcanoReference represents the static Indonesian volcano catalog for geographic enrichment.
type VolcanoReference struct {
	VolcanoID   string  `json:"volcano_id" db:"volcano_id"`
	VolcanoName string  `json:"volcano_name" db:"volcano_name"`
	Latitude    float64 `json:"latitude" db:"latitude"`
	Longitude   float64 `json:"longitude" db:"longitude"`
}

// OutboxEvent represents an entry in the transactional outbox table
// to guarantee atomic persistence and at-least-once relay to Redpanda.
type OutboxEvent struct {
	ID          int64           `json:"id" db:"id"`
	EventID     string          `json:"event_id" db:"event_id"`
	EventType   string          `json:"event_type" db:"event_type"`
	Payload     json.RawMessage `json:"payload" db:"payload"`
	Published   bool            `json:"published" db:"published"`
	CreatedAt   time.Time       `json:"created_at" db:"created_at"`
	PublishedAt *time.Time      `json:"published_at,omitempty" db:"published_at"`
}
