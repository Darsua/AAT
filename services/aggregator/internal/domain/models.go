package domain

import (
	"encoding/json"
	"time"
)

// Source represents the upstream monitoring agency.
type Source string

const (
	SourceBMKG         Source = "BMKG"
	SourcePVMBG        Source = "PVMBG"
	SourceSatelit      Source = "SATELIT"
	SourceSensorPantai Source = "SENSOR_PANTAI"
)

// IsValidSource checks whether s is a valid canonical HazardEvent source.
func IsValidSource(s Source) bool {
	switch s {
	case SourceBMKG, SourcePVMBG, SourceSatelit, SourceSensorPantai:
		return true
	default:
		return false
	}
}

// HazardType represents the domain classification of the natural disaster.
type HazardType string

const (
	HazardTypeSeismic  HazardType = "SEISMIC"
	HazardTypeVolcanic HazardType = "VOLCANIC"
	HazardTypeWildfire HazardType = "WILDFIRE"
	HazardTypeCoastal  HazardType = "COASTAL"
)

// IsValidHazardType checks whether ht is a valid canonical HazardType.
func IsValidHazardType(ht HazardType) bool {
	switch ht {
	case HazardTypeSeismic, HazardTypeVolcanic, HazardTypeWildfire, HazardTypeCoastal:
		return true
	default:
		return false
	}
}

// Severity represents the standardized national alert level.
type Severity string

const (
	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

// IsValidSeverity checks whether sev is a valid canonical Severity level.
func IsValidSeverity(sev Severity) bool {
	switch sev {
	case SeverityNormal, SeverityWaspada, SeveritySiaga, SeverityAwas:
		return true
	default:
		return false
	}
}

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
	SourceRefID  string         `json:"source_ref_id" db:"source_ref_id"`
	Latitude     *float64       `json:"latitude,omitempty" db:"latitude"`
	Longitude    *float64       `json:"longitude,omitempty" db:"longitude"`
	Attributes   map[string]any `json:"attributes" db:"attributes"` // Dynamic schemaless bag (e.g. confidence_level, depth_km, magnitude)
	FencingToken int64          `json:"-" db:"fencing_token"`
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
	Payload       json.RawMessage `json:"payload" db:"payload"`
	CorrelationID string          `json:"correlation_id" db:"correlation_id"`
	Published     bool            `json:"published" db:"published"`
	CreatedAt     time.Time       `json:"created_at" db:"created_at"`
	PublishedAt   *time.Time      `json:"published_at,omitempty" db:"published_at"`
}

// StorageRole describes whether a storage unit serves as primary or replica for a shard.
type StorageRole string

const (
	StorageRolePrimary StorageRole = "primary"
	StorageRoleReplica StorageRole = "replica"
)

// StorageLocation reports the cluster node, storage unit ID, and replication role holding a hazard record.
type StorageLocation struct {
	Node string      `json:"node"`
	Unit string      `json:"unit"`
	Role StorageRole `json:"role"`
}

// UnprocessedStatus represents the lifecycle state of a message parked in the Dead Letter Queue.
type UnprocessedStatus string

const (
	UnprocessedStatusRetrying  UnprocessedStatus = "retrying"
	UnprocessedStatusParked    UnprocessedStatus = "parked"
	UnprocessedStatusRecovered UnprocessedStatus = "recovered"
)

// UnprocessedMessage represents an unprocessable or retrying message stored in dlq_messages.
type UnprocessedMessage struct {
	MessageID  string            `json:"message_id" db:"message_id"`
	Source     Source            `json:"source" db:"source"`
	ReceivedAt time.Time         `json:"received_at" db:"received_at"`
	Reason     string            `json:"reason" db:"reason"`
	Retryable  bool              `json:"retryable" db:"retryable"`
	Attempts   int               `json:"attempts" db:"attempts"`
	Status     UnprocessedStatus `json:"status" db:"status"`
	RawPayload []byte            `json:"-" db:"raw_payload"`
}

// LeaderLease represents a time-bounded distributed leadership lease backed by storage coordinator time.
type LeaderLease struct {
	LeaseKey     string    `json:"lease_key" db:"lease_key"`
	HolderID     string    `json:"holder_id" db:"holder_id"`
	FencingToken int64     `json:"fencing_token" db:"fencing_token"`
	AcquiredAt   time.Time `json:"acquired_at" db:"acquired_at"`
	RenewedAt    time.Time `json:"renewed_at" db:"renewed_at"`
	ExpiresAt    time.Time `json:"expires_at" db:"expires_at"`
}

// SourceCheckpoint stores durable source collection progress paired with the leader's fencing token.
type SourceCheckpoint struct {
	Source          Source    `json:"source" db:"source"`
	LastOccurredAt  time.Time `json:"last_occurred_at" db:"last_occurred_at"`
	LastSourceRefID string    `json:"last_source_ref_id" db:"last_source_ref_id"`
	FencingToken    int64     `json:"fencing_token" db:"fencing_token"`
	HolderID        string    `json:"holder_id" db:"holder_id"`
	UpdatedAt       time.Time `json:"updated_at" db:"updated_at"`
}

// WriteFence carries the monotonic fencing token and holder identity validated by storage before committing writes.
type WriteFence struct {
	LeaseKey     string
	Source       Source
	HolderID     string
	FencingToken int64
}
