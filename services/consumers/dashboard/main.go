package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Fallback to localhost:19092 if BROKERS is not set.
const (
	defaultBrokers = "localhost:19092"
	defaultTopic   = "bnpb.hazard-events"
	defaultGroupID = "bnpb-dashboard-group"
)

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

// HazardEvent mirrors the canonical event in contracts/hazard_event.json.
type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    *float64       `json:"latitude"`
	Longitude   *float64       `json:"longitude"`
	Attributes  map[string]any `json:"attributes"` // Attribute may change --> to Latitude and Longitude
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
}

// At least one of TypeCount, SeverityCount, or Duplicate will be non-zero.
type ProcessResult struct {
	Duplicate     bool
	TypeCount     int
	SeverityCount int
}

// Dashboard owns the in-memory counters and processed event IDs.
type Dashboard struct {
	mu         sync.Mutex
	processed  map[string]struct{}
	byType     map[string]int // counts by hazard type
	bySeverity map[string]int // counts by severity
}

func NewDashboard() *Dashboard {
	return &Dashboard{
		processed:  make(map[string]struct{}),
		byType:     make(map[string]int),
		bySeverity: make(map[string]int),
	}
}

// Process counts each hazard ID at most once during this process lifetime.
func (d *Dashboard) Process(event HazardEvent) ProcessResult {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.processed[event.HazardID]; exists {
		return ProcessResult{Duplicate: true}
	}

	d.byType[event.HazardType]++
	d.bySeverity[event.Severity]++
	d.processed[event.HazardID] = struct{}{}

	return ProcessResult{
		TypeCount:     d.byType[event.HazardType],
		SeverityCount: d.bySeverity[event.Severity],
	}
}

func loadConfig() Config {
	return Config{
		Brokers: splitAndTrim(getEnv("REDPANDA_BROKERS", defaultBrokers)),
		Topic:   getEnv("TOPIC", defaultTopic),
		GroupID: getEnv("CONSUMER_GROUP", defaultGroupID),
	}
}

// Topic for querying hazard events.
func getEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

// splitAndTrim splits a comma-separated string into a slice of trimmed strings, for parsing broker addresses.
// Cause of partitioning.
func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

// decodeEvent decodes a hazard event from a byte slice, see if it is valid.
func decodeEvent(data []byte) (HazardEvent, error) {
	var event HazardEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return HazardEvent{}, fmt.Errorf("decode hazard event: %w", err)
	}
	if event.HazardID == "" || event.HazardType == "" || event.Severity == "" {
		return HazardEvent{}, errors.New("hazard_id, hazard_type, and severity are required")
	}
	return event, nil
}

// consume reads hazard events from the Kafka topic and processes them.
func consume(ctx context.Context, reader *kafka.Reader, dashboard *Dashboard) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		event, err := decodeEvent(message.Value)
		if err != nil {
			return err
		}

		result := dashboard.Process(event)
		if result.Duplicate {
			log.Printf("[dashboard] skipped duplicate hazard_id=%s", event.HazardID)
		} else {
			log.Printf(
				"[dashboard] processed hazard_id=%s type=%s severity=%s type_count=%d severity_count=%d",
				event.HazardID,
				event.HazardType,
				event.Severity,
				result.TypeCount,
				result.SeverityCount,
			)
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit message: %w", err)
		}
	}
}

func main() {
	config := loadConfig()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     config.Brokers,
		Topic:       config.Topic,
		GroupID:     config.GroupID,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf(
		"[dashboard] consuming topic=%s group=%s brokers=%s",
		config.Topic,
		config.GroupID,
		strings.Join(config.Brokers, ","),
	)

	if err := consume(ctx, reader, NewDashboard()); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[dashboard] stopped: %v", err)
		return
	}
	log.Println("[dashboard] stopped")
}
