package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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

// NotificationDelivery represents an acknowledged Field Team alert receipt.
type NotificationDelivery struct {
	NotificationID string    `json:"notification_id"`
	HazardID       string    `json:"hazard_id"`
	Source         string    `json:"source"`
	HazardType     string    `json:"hazard_type"`
	Severity       string    `json:"severity"`
	AreaName       string    `json:"area_name"`
	Reason         string    `json:"reason"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	OccurredAt     time.Time `json:"occurred_at"`
	ReceivedAt     time.Time `json:"received_at"`
	PayloadHash    string    `json:"payload_hash"`
}

// NotificationResult describes the action taken for one event.
type NotificationResult struct {
	Duplicate bool
	Alert     bool
	Reason    string
}

// Notifier performs update-aware deduplication and records Field Team alert deliveries.
type Notifier struct {
	mu          sync.RWMutex
	processed   map[string]string
	deliveries  []NotificationDelivery
	receiptPath string
	receiverURL string
	httpClient  *http.Client
}

func NewNotifier() *Notifier {
	return NewNotifierWithOptions("", "")
}

func NewNotifierWithOptions(receiptPath, receiverURL string) *Notifier {
	n := &Notifier{
		processed:   make(map[string]string),
		receiptPath: strings.TrimSpace(receiptPath),
		receiverURL: strings.TrimSpace(receiverURL),
		httpClient:  &http.Client{Timeout: 2 * time.Second},
	}
	if n.receiptPath != "" {
		n.loadReceipts()
	}
	return n
}

func (n *Notifier) loadReceipts() {
	f, err := os.Open(n.receiptPath)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var d NotificationDelivery
		if err := json.Unmarshal(scanner.Bytes(), &d); err == nil && d.HazardID != "" {
			n.deliveries = append(n.deliveries, d)
			if d.PayloadHash != "" {
				n.processed[d.HazardID] = d.PayloadHash
			}
		}
	}
}

// Process evaluates an event with update-aware deduplication:
// identical payloads for the same hazard_id are suppressed, while updated payloads
// (e.g. severity escalation or critical=true flip) are re-evaluated and notified.
func (n *Notifier) Process(event HazardEvent) NotificationResult {
	hash := eventFingerprint(event)

	n.mu.Lock()
	defer n.mu.Unlock()

	if prevHash, exists := n.processed[event.HazardID]; exists && prevHash == hash {
		return NotificationResult{Duplicate: true}
	}

	reason, alert := alertReason(event)
	n.processed[event.HazardID] = hash
	return NotificationResult{Alert: alert, Reason: reason}
}

// Deliver persists the Field Team alert receipt and optionally forwards to FIELD_TEAM_RECEIVER_URL.
func (n *Notifier) Deliver(ctx context.Context, event HazardEvent, reason, correlationID string) (NotificationDelivery, error) {
	hash := eventFingerprint(event)
	delivery := NotificationDelivery{
		NotificationID: fmt.Sprintf("notif-%s-%s", event.HazardID, hash[:8]),
		HazardID:       event.HazardID,
		Source:         event.Source,
		HazardType:     event.HazardType,
		Severity:       event.Severity,
		AreaName:       event.AreaName,
		Reason:         reason,
		CorrelationID:  correlationID,
		OccurredAt:     event.OccurredAt,
		ReceivedAt:     time.Now().UTC(),
		PayloadHash:    hash,
	}

	if n.receiverURL != "" {
		if err := n.postToReceiver(ctx, delivery); err != nil {
			return NotificationDelivery{}, err
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.receiptPath != "" {
		line, err := json.Marshal(delivery)
		if err != nil {
			return NotificationDelivery{}, err
		}
		f, err := os.OpenFile(n.receiptPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return NotificationDelivery{}, fmt.Errorf("open receipt log: %w", err)
		}
		_, writeErr := f.Write(append(line, '\n'))
		closeErr := f.Close()
		if writeErr != nil {
			return NotificationDelivery{}, writeErr
		}
		if closeErr != nil {
			return NotificationDelivery{}, closeErr
		}
	}
	n.deliveries = append(n.deliveries, delivery)
	return delivery, nil
}

func (n *Notifier) postToReceiver(ctx context.Context, delivery NotificationDelivery) error {
	body, err := json.Marshal(delivery)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.receiverURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if delivery.CorrelationID != "" {
			req.Header.Set(correlationHeader, delivery.CorrelationID)
		}
		resp, err := n.httpClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("receiver returned HTTP %d", resp.StatusCode)
		} else {
			lastErr = err
		}
	}
	return lastErr
}

// Deliveries returns a snapshot of acknowledged Field Team notifications.
func (n *Notifier) Deliveries() []NotificationDelivery {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]NotificationDelivery, len(n.deliveries))
	copy(out, n.deliveries)
	return out
}

func eventFingerprint(event HazardEvent) string {
	payload, _ := json.Marshal(struct {
		HazardID   string         `json:"hazard_id"`
		Severity   string         `json:"severity"`
		Attributes map[string]any `json:"attributes"`
		OccurredAt string         `json:"occurred_at"`
	}{
		HazardID:   event.HazardID,
		Severity:   strings.ToUpper(event.Severity),
		Attributes: event.Attributes,
		OccurredAt: event.OccurredAt.UTC().Format(time.RFC3339Nano),
	})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
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

	if event.HazardType == "COASTAL" {
		if crit, ok := event.Attributes["critical"].(bool); ok && crit {
			reasons = append(reasons, "coastal critical observation")
		} else if strings.EqualFold(event.Severity, "AWAS") {
			reasons = append(reasons, "coastal alert AWAS")
		}
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
	logger := newJSONLogger("notifier-consumer")
	return consumeWithTelemetry(ctx, reader, notifier, newRegistry(), newTracer("notifier-consumer", logger), logger)
}

func consumeWithTelemetry(ctx context.Context, reader messageReader, notifier *Notifier, reg *Registry, tracer *Tracer, logger *slog.Logger) error {
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

		event, err := decodeEvent(message.Value)
		if err != nil {
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "notifier-consumer", "topic": message.Topic, "status": "invalid"}, 1)
			reg.ObserveHistogram("kafka_message_processing_duration_seconds", map[string]string{"service": "notifier-consumer", "topic": message.Topic}, time.Since(started).Seconds())
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

		result := notifier.Process(event)
		switch {
		case result.Duplicate:
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "notifier-consumer", "topic": message.Topic, "status": "duplicate"}, 1)
			reg.IncCounter("kafka_consumer_dedup_skipped_total", map[string]string{"service": "notifier-consumer"}, 1)
			logger.Info("skipped duplicate hazard event",
				slog.String("correlation_id", correlationID),
				slog.String("trace_id", st.TraceID),
				slog.String("span_id", st.SpanID),
				slog.String("hazard_id", event.HazardID),
			)
		case result.Alert:
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "notifier-consumer", "topic": message.Topic, "status": "alert"}, 1)
			delivery, err := notifier.Deliver(ctx, event, result.Reason, correlationID)
			if err != nil {
				reg.IncCounter("notification_delivery_failures_total", map[string]string{"service": "notifier-consumer"}, 1)
				logger.Error("notification delivery failed",
					slog.String("correlation_id", correlationID),
					slog.String("trace_id", st.TraceID),
					slog.String("span_id", st.SpanID),
					slog.String("hazard_id", event.HazardID),
					slog.String("error", err.Error()),
				)
				continue
			}
			logger.Info("ALERT triggered",
				slog.String("correlation_id", correlationID),
				slog.String("trace_id", st.TraceID),
				slog.String("span_id", st.SpanID),
				slog.String("notification_id", delivery.NotificationID),
				slog.String("hazard_id", event.HazardID),
				slog.String("area", event.AreaName),
				slog.String("reason", result.Reason),
			)
		default:
			reg.IncCounter("kafka_messages_consumed_total", map[string]string{"service": "notifier-consumer", "topic": message.Topic, "status": "processed"}, 1)
			logger.Info("processed hazard event without alert",
				slog.String("correlation_id", correlationID),
				slog.String("trace_id", st.TraceID),
				slog.String("span_id", st.SpanID),
				slog.String("hazard_id", event.HazardID),
			)
		}
		reg.ObserveHistogram("kafka_message_processing_duration_seconds", map[string]string{"service": "notifier-consumer", "topic": message.Topic}, time.Since(started).Seconds())
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

	logger := newJSONLogger("notifier-consumer")
	reg := newRegistry()
	tracer := newTracer("notifier-consumer", logger)
	notifier := NewNotifierWithOptions(os.Getenv("NOTIFIER_RECEIPT_LOG"), os.Getenv("FIELD_TEAM_RECEIVER_URL"))
	startHTTPServer(ctx, getEnv("PORT", "8086"), reg, notifier)

	logger.Info("consuming hazard events",
		slog.String("topic", config.Topic),
		slog.String("group", config.GroupID),
		slog.String("brokers", strings.Join(config.Brokers, ",")),
	)

	if err := consumeWithTelemetry(ctx, reader, notifier, reg, tracer, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("consumer stopped with error", slog.String("error", err.Error()))
		return
	}
	logger.Info("consumer stopped")
}
