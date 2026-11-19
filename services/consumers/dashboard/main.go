package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Fallback to localhost:19092 if BROKERS is not set.
const (
	defaultBrokers      = "localhost:19092"
	defaultTopic        = "bnpb.hazard-events"
	defaultGroupID      = "bnpb-dashboard-group"
	defaultAggregator   = "http://aggregator:8083"
	defaultSyncInterval = 5 * time.Second
	correlationHeader   = "X-Correlation-ID"
)

type Config struct {
	Brokers       []string
	Topic         string
	GroupID       string
	AggregatorURL string
	SyncInterval  time.Duration
}

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
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
	Attributes  map[string]any `json:"attributes"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
}

// At least one of TypeCount, SeverityCount, Duplicate, Updated, or Retracted will be set.
type ProcessResult struct {
	Duplicate     bool
	Updated       bool
	Retracted     bool
	TypeCount     int
	SeverityCount int
}

// Dashboard owns the update-aware hazard projection, aggregates, and sync state.
type Dashboard struct {
	mu                  sync.RWMutex
	events              map[string]HazardEvent
	byType              map[string]int // counts by hazard type
	bySeverity          map[string]int // counts by severity
	degraded            bool
	partialData         bool
	unavailableUnits    []string
	partialReason       string
	staleSince          *time.Time
	lastSyncAt          *time.Time
	lastKafkaEventAt    *time.Time
	syncIntervalSeconds int
}

func NewDashboard() *Dashboard {
	return &Dashboard{
		events:              make(map[string]HazardEvent),
		byType:              make(map[string]int),
		bySeverity:          make(map[string]int),
		syncIntervalSeconds: int(defaultSyncInterval.Seconds()),
	}
}

func (d *Dashboard) SetSyncInterval(interval time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.syncIntervalSeconds = int(interval.Seconds())
}

// Retract removes a hazard by ID (e.g. on Kafka tombstone or retraction message).
func (d *Dashboard) Retract(hazardID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.retractLocked(hazardID)
}

func (d *Dashboard) retractLocked(hazardID string) bool {
	existing, exists := d.events[hazardID]
	if !exists {
		return false
	}
	delete(d.events, hazardID)
	if d.byType[existing.HazardType] > 0 {
		d.byType[existing.HazardType]--
	}
	if d.bySeverity[existing.Severity] > 0 {
		d.bySeverity[existing.Severity]--
	}
	return true
}

func isRetracted(event HazardEvent) bool {
	if strings.EqualFold(event.Severity, "RETRACTED") || strings.EqualFold(event.HazardType, "RETRACTED") {
		return true
	}
	if event.Attributes != nil {
		if ret, ok := event.Attributes["retracted"].(bool); ok && ret {
			return true
		}
		if del, ok := event.Attributes["deleted"].(bool); ok && del {
			return true
		}
		if status, ok := event.Attributes["status"].(string); ok && strings.EqualFold(status, "RETRACTED") {
			return true
		}
	}
	return false
}

// Process upserts a canonical hazard event, updating counts if an existing hazard changed.
func (d *Dashboard) Process(event HazardEvent) ProcessResult {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now().UTC()
	d.lastKafkaEventAt = &now

	if isRetracted(event) {
		removed := d.retractLocked(event.HazardID)
		return ProcessResult{Retracted: true, Updated: removed}
	}

	if existing, exists := d.events[event.HazardID]; exists {
		if shouldSkipUpdate(existing, event) {
			return ProcessResult{Duplicate: true}
		}
		if d.byType[existing.HazardType] > 0 {
			d.byType[existing.HazardType]--
		}
		if d.bySeverity[existing.Severity] > 0 {
			d.bySeverity[existing.Severity]--
		}
		d.events[event.HazardID] = cloneHazardEvent(event)
		d.byType[event.HazardType]++
		d.bySeverity[event.Severity]++
		return ProcessResult{
			Updated:       true,
			TypeCount:     d.byType[event.HazardType],
			SeverityCount: d.bySeverity[event.Severity],
		}
	}

	d.events[event.HazardID] = cloneHazardEvent(event)
	d.byType[event.HazardType]++
	d.bySeverity[event.Severity]++

	return ProcessResult{
		TypeCount:     d.byType[event.HazardType],
		SeverityCount: d.bySeverity[event.Severity],
	}
}

func shouldSkipUpdate(existing, incoming HazardEvent) bool {
	if !incoming.IngestedAt.IsZero() && !existing.IngestedAt.IsZero() {
		if incoming.IngestedAt.Before(existing.IngestedAt) {
			return true
		}
		if incoming.IngestedAt.After(existing.IngestedAt) {
			return false
		}
	} else if incoming.IngestedAt.IsZero() && existing.IngestedAt.IsZero() &&
		!incoming.OccurredAt.IsZero() && !existing.OccurredAt.IsZero() {
		if incoming.OccurredAt.Before(existing.OccurredAt) {
			return true
		}
	}
	return hazardEventsEqual(existing, incoming)
}

func hazardEventsEqual(a, b HazardEvent) bool {
	if a.HazardID != b.HazardID ||
		a.Source != b.Source ||
		a.SourceRefID != b.SourceRefID ||
		a.HazardType != b.HazardType ||
		a.Severity != b.Severity ||
		a.AreaName != b.AreaName ||
		!a.OccurredAt.Equal(b.OccurredAt) ||
		!a.IngestedAt.Equal(b.IngestedAt) {
		return false
	}
	if !floatPtrEqual(a.Latitude, b.Latitude) || !floatPtrEqual(a.Longitude, b.Longitude) {
		return false
	}
	return reflect.DeepEqual(a.Attributes, b.Attributes)
}

func floatPtrEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func cloneHazardEvent(e HazardEvent) HazardEvent {
	cp := e
	if e.Latitude != nil {
		v := *e.Latitude
		cp.Latitude = &v
	}
	if e.Longitude != nil {
		v := *e.Longitude
		cp.Longitude = &v
	}
	if len(e.Attributes) > 0 {
		attrs := make(map[string]any, len(e.Attributes))
		for k, v := range e.Attributes {
			attrs[k] = v
		}
		cp.Attributes = attrs
	}
	return cp
}

func (d *Dashboard) rebuildCountersLocked() {
	d.byType = make(map[string]int)
	d.bySeverity = make(map[string]int)
	for _, ev := range d.events {
		d.byType[ev.HazardType]++
		d.bySeverity[ev.Severity]++
	}
}

func loadConfig() Config {
	syncInterval := defaultSyncInterval
	if raw := strings.TrimSpace(os.Getenv("DASHBOARD_SYNC_INTERVAL_SECS")); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
			syncInterval = time.Duration(secs) * time.Second
		}
	}
	return Config{
		Brokers:       splitAndTrim(getEnv("REDPANDA_BROKERS", defaultBrokers)),
		Topic:         getEnv("TOPIC", defaultTopic),
		GroupID:       getEnv("CONSUMER_GROUP", defaultGroupID),
		AggregatorURL: strings.TrimRight(getEnv("AGGREGATOR_URL", defaultAggregator), "/"),
		SyncInterval:  syncInterval,
	}
}

// getEnv retrieves an environment variable or returns a fallback value.
func getEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

// splitAndTrim splits a comma-separated string into a slice of non-empty trimmed strings.
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

// decodeEvent decodes and validates a canonical hazard event from a byte slice.
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

// consume reads hazard events from the Kafka topic and processes them.
func consume(ctx context.Context, reader messageReader, dashboard *Dashboard) error {
	logger := newJSONLogger("dashboard-consumer")
	return consumeWithTelemetry(ctx, reader, dashboard, newRegistry(), newTracer("dashboard-consumer", logger), logger)
}

func consumeWithTelemetry(ctx context.Context, reader messageReader, dashboard *Dashboard, reg *Registry, tracer *Tracer, logger *slog.Logger) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		started := time.Now()
		correlationID := messageHeader(message, correlationHeader)
		msgCtx := ctx
		if tp := messageHeader(message, traceparentHeader); tp != "" {
			msgCtx = contextWithTraceparent(msgCtx, tp)
		}
		msgCtx, span := tracer.StartSpan(msgCtx, "kafka.consume "+message.Topic, spanKindConsumer,
			SpanAttr{Key: "messaging.system", Value: "kafka"},
			SpanAttr{Key: "messaging.destination", Value: message.Topic},
			SpanAttr{Key: "correlation_id", Value: correlationID},
		)
		st := traceFromContext(msgCtx)

		// Support Kafka tombstone messages (empty payload with key indicating retraction)
		if len(message.Value) == 0 && len(message.Key) > 0 {
			key := string(message.Key)
			if dashboard.Retract(key) {
				logger.Info("retracted hazard via tombstone",
					slog.String("correlation_id", correlationID),
					slog.String("trace_id", st.TraceID),
					slog.String("span_id", st.SpanID),
					slog.String("hazard_id", key),
				)
			}
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "dashboard-consumer", "topic": message.Topic, "status": "processed"}, 1)
			reg.ObserveHistogram("kafka_message_processing_duration_seconds", map[string]string{"service": "dashboard-consumer", "topic": message.Topic}, time.Since(started).Seconds())
			span.End(200, "")
			if err := reader.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf("commit tombstone message: %w", err)
			}
			continue
		}

		event, err := decodeEvent(message.Value)
		if err != nil {
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "dashboard-consumer", "topic": message.Topic, "status": "invalid"}, 1)
			reg.ObserveHistogram("kafka_message_processing_duration_seconds", map[string]string{"service": "dashboard-consumer", "topic": message.Topic}, time.Since(started).Seconds())
			span.End(400, err.Error())
			logger.Warn("skipped invalid message",
				slog.String("correlation_id", correlationID),
				slog.String("trace_id", st.TraceID),
				slog.String("span_id", st.SpanID),
				slog.Int("partition", message.Partition),
				slog.Int64("offset", message.Offset),
				slog.String("error", err.Error()),
			)
			if err := reader.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf("commit invalid message: %w", err)
			}
			continue
		}

		result := dashboard.Process(event)
		if result.Retracted {
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "dashboard-consumer", "topic": message.Topic, "status": "processed"}, 1)
			logger.Info("retracted hazard event",
				slog.String("correlation_id", correlationID),
				slog.String("trace_id", st.TraceID),
				slog.String("span_id", st.SpanID),
				slog.String("hazard_id", event.HazardID),
			)
		} else if result.Duplicate {
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "dashboard-consumer", "topic": message.Topic, "status": "duplicate"}, 1)
			reg.IncCounter("kafka_consumer_dedup_skipped_total", map[string]string{"service": "dashboard-consumer"}, 1)
			logger.Info("skipped duplicate hazard event",
				slog.String("correlation_id", correlationID),
				slog.String("trace_id", st.TraceID),
				slog.String("span_id", st.SpanID),
				slog.String("hazard_id", event.HazardID),
			)
		} else {
			msg := "processed hazard event"
			if result.Updated {
				msg = "updated hazard event"
			}
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "dashboard-consumer", "topic": message.Topic, "status": "processed"}, 1)
			logger.Info(msg,
				slog.String("correlation_id", correlationID),
				slog.String("trace_id", st.TraceID),
				slog.String("span_id", st.SpanID),
				slog.String("hazard_id", event.HazardID),
				slog.String("hazard_type", event.HazardType),
				slog.String("severity", event.Severity),
				slog.Int("type_count", result.TypeCount),
				slog.Int("severity_count", result.SeverityCount),
			)
		}
		reg.ObserveHistogram("kafka_message_processing_duration_seconds", map[string]string{"service": "dashboard-consumer", "topic": message.Topic}, time.Since(started).Seconds())
		span.End(200, "")

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

	dashboard := NewDashboard()
	dashboard.SetSyncInterval(config.SyncInterval)
	logger := newJSONLogger("dashboard-consumer")
	reg := newRegistry()
	tracer := newTracer("dashboard-consumer", logger)
	startDashboardServerWithTelemetry(ctx, getEnv("PORT", "8085"), dashboard, reg)
	startAggregatorSyncLoop(ctx, config.AggregatorURL, config.SyncInterval, &http.Client{Timeout: 4 * time.Second}, dashboard)

	logger.Info("consuming hazard events",
		slog.String("topic", config.Topic),
		slog.String("group", config.GroupID),
		slog.String("brokers", strings.Join(config.Brokers, ",")),
		slog.String("aggregator", config.AggregatorURL),
	)

	if err := consumeWithTelemetry(ctx, reader, dashboard, reg, tracer, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("consumer stopped with error", slog.String("error", err.Error()))
		return
	}
	logger.Info("consumer stopped")
}
