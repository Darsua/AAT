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

const (
	defaultBrokers    = "localhost:19092"
	defaultTopic      = "bnpb.hazard-events"
	defaultGroupID    = "bnpb-notifier-group"
	correlationHeader = "X-Correlation-ID"
)

// Config contains the connection settings needed by this consumer.
type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

// HazardEvent mirrors the canonical event in contracts/hazard_event.json.
// Attributes remains dynamic so source-specific fields can evolve independently.
type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    *float64       `json:"latitude"`
	Longitude   *float64       `json:"longitude"`
	Attributes  map[string]any `json:"attributes"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
}

// NotificationResult describes the action taken for one event.
type NotificationResult struct {
	Duplicate bool
	Alert     bool
	Reason    string
}

// Notifier prevents duplicate notifications during this process lifetime.
type Notifier struct {
	mu        sync.Mutex
	processed map[string]struct{}
}

func NewNotifier() *Notifier {
	return &Notifier{processed: make(map[string]struct{})}
}

// Process evaluates a new event once and returns whether it requires an alert.
func (n *Notifier) Process(event HazardEvent) NotificationResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.processed[event.HazardID]; exists {
		return NotificationResult{Duplicate: true}
	}

	reason, alert := alertReason(event)
	n.processed[event.HazardID] = struct{}{}
	return NotificationResult{Alert: alert, Reason: reason}
}

func alertReason(event HazardEvent) (string, bool) {
	reasons := make([]string, 0, 2)

	if event.HazardType == "SEISMIC" {
		if magnitude, ok := numberAttribute(event.Attributes, "magnitude"); ok && magnitude >= 6.0 {
			reasons = append(reasons, fmt.Sprintf("earthquake magnitude %.1f", magnitude))
		}

		if level, ok := tsunamiThreatLevel(event.Attributes); ok && (level == "SIAGA" || level == "AWAS") {
			reasons = append(reasons, "tsunami warning "+level)
		}
	}

	if event.HazardType == "VOLCANIC" && strings.EqualFold(event.Severity, "AWAS") {
		reasons = append(reasons, "volcano alert AWAS")
	}

	return strings.Join(reasons, "; "), len(reasons) > 0
}

func numberAttribute(attributes map[string]any, name string) (float64, bool) {
	value, exists := attributes[name]
	if !exists {
		return 0, false
	}
	number, ok := value.(float64)
	return number, ok
}

func tsunamiThreatLevel(attributes map[string]any) (string, bool) {
	warning, ok := attributes["tsunami_warning"].(map[string]any)
	if !ok {
		return "", false
	}
	level, ok := warning["threat_level"].(string)
	return strings.ToUpper(level), ok
}

func loadConfig() Config {
	return Config{
		Brokers: splitAndTrim(getEnv("REDPANDA_BROKERS", defaultBrokers)),
		Topic:   getEnv("TOPIC", defaultTopic),
		GroupID: getEnv("CONSUMER_GROUP", defaultGroupID),
	}
}

func getEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			result = append(result, broker)
		}
	}
	return result
}

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

func messageHeader(message kafka.Message, name string) string {
	for _, header := range message.Headers {
		if strings.EqualFold(header.Key, name) {
			return string(header.Value)
		}
	}
	return ""
}

func consume(ctx context.Context, reader messageReader, notifier *Notifier) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		event, err := decodeEvent(message.Value)
		if err != nil {
			log.Printf(
				"[notifier] correlation_id=%q skipped invalid message partition=%d offset=%d error=%q",
				messageHeader(message, correlationHeader),
				message.Partition,
				message.Offset,
				err,
			)
			if err := reader.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf("commit invalid message: %w", err)
			}
			continue
		}

		correlationID := messageHeader(message, correlationHeader)
		result := notifier.Process(event)
		switch {
		case result.Duplicate:
			log.Printf("[notifier] correlation_id=%q skipped duplicate hazard_id=%s", correlationID, event.HazardID)
		case result.Alert:
			log.Printf(
				"[notifier] correlation_id=%q ALERT hazard_id=%s area=%q reason=%q",
				correlationID,
				event.HazardID,
				event.AreaName,
				result.Reason,
			)
		default:
			log.Printf("[notifier] correlation_id=%q no alert hazard_id=%s", correlationID, event.HazardID)
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
		"[notifier] consuming topic=%s group=%s brokers=%s",
		config.Topic,
		config.GroupID,
		strings.Join(config.Brokers, ","),
	)

	if err := consume(ctx, reader, NewNotifier()); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[notifier] stopped: %v", err)
		return
	}
	log.Println("[notifier] stopped")
}
