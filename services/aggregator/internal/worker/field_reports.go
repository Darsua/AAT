package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/telemetry"

	"github.com/segmentio/kafka-go"
)

const (
	defaultFieldReportsTopicName = "bnpb.field-reports"
	fieldReportErrorBackoff      = 500 * time.Millisecond
	maxFieldReportHazardIDLen    = 128
	maxFieldReportDLQPayload     = 1 << 20 // 1 MiB matching gateway limit
)

// FieldReportMessageReader abstracts kafka.Reader for field-report consumption and unit testing.
type FieldReportMessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// StagedFieldReport mirrors the durable staging payload published to bnpb.field-reports by client-api.
type StagedFieldReport struct {
	ReportID       string         `json:"report_id"`
	ReporterID     string         `json:"reporter_id"`
	SubmittedBy    string         `json:"submitted_by"`
	HazardType     string         `json:"hazard_type"`
	Severity       string         `json:"severity"`
	AreaName       string         `json:"area_name"`
	Latitude       *float64       `json:"latitude,omitempty"`
	Longitude      *float64       `json:"longitude,omitempty"`
	OccurredAt     string         `json:"occurred_at"`
	Description    string         `json:"description,omitempty"`
	Details        map[string]any `json:"details,omitempty"`
	ReceivedAt     string         `json:"received_at"`
	CorrelationID  string         `json:"correlation_id,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	PayloadSHA256  string         `json:"payload_sha256"`
}

// MapFieldReportHazardTypeToSource maps a validated hazard_type to an existing canonical Source value
// in frozen contracts/hazard_event.json without altering contract enums.
func MapFieldReportHazardTypeToSource(ht domain.HazardType) (domain.Source, bool) {
	switch ht {
	case domain.HazardTypeSeismic:
		return domain.SourceBMKG, true
	case domain.HazardTypeVolcanic:
		return domain.SourcePVMBG, true
	case domain.HazardTypeWildfire:
		return domain.SourceSatelit, true
	case domain.HazardTypeCoastal:
		return domain.SourceSensorPantai, true
	default:
		return "", false
	}
}

// NormalizeFieldReport decodes, validates, and converts a staged field report into a canonical HazardEvent.
func NormalizeFieldReport(raw []byte, fallbackKey string) (*domain.HazardEvent, StagedFieldReport, error) {
	var report StagedFieldReport
	if err := json.Unmarshal(raw, &report); err != nil {
		return nil, StagedFieldReport{}, fmt.Errorf("invalid staged JSON: %w", err)
	}

	report.ReportID = strings.TrimSpace(report.ReportID)
	if report.ReportID == "" {
		report.ReportID = strings.TrimSpace(fallbackKey)
	}
	if report.ReportID == "" {
		return nil, report, errors.New("report_id is required")
	}
	hazardID := "haz-field-" + report.ReportID
	if len(hazardID) > maxFieldReportHazardIDLen {
		return nil, report, fmt.Errorf("report_id exceeds maximum canonical length (%d)", maxFieldReportHazardIDLen-len("haz-field-"))
	}

	report.ReporterID = strings.TrimSpace(report.ReporterID)
	if report.ReporterID == "" {
		return nil, report, errors.New("reporter_id is required")
	}

	report.AreaName = strings.TrimSpace(report.AreaName)
	if report.AreaName == "" {
		return nil, report, errors.New("area_name is required")
	}

	ht := domain.HazardType(strings.ToUpper(strings.TrimSpace(report.HazardType)))
	if !domain.IsValidHazardType(ht) {
		return nil, report, fmt.Errorf("unsupported hazard_type %q", report.HazardType)
	}
	source, ok := MapFieldReportHazardTypeToSource(ht)
	if !ok {
		return nil, report, fmt.Errorf("no canonical source mapping for hazard_type %q", ht)
	}

	sev := domain.Severity(strings.ToUpper(strings.TrimSpace(report.Severity)))
	if !domain.IsValidSeverity(sev) {
		return nil, report, fmt.Errorf("unsupported severity %q", report.Severity)
	}

	occurredAt, err := parseUTCReportTime(report.OccurredAt)
	if err != nil {
		return nil, report, fmt.Errorf("invalid occurred_at: %w", err)
	}

	if report.Latitude != nil {
		if math.IsNaN(*report.Latitude) || math.IsInf(*report.Latitude, 0) || *report.Latitude < -90 || *report.Latitude > 90 {
			return nil, report, errors.New("latitude must be between -90 and 90")
		}
	}
	if report.Longitude != nil {
		if math.IsNaN(*report.Longitude) || math.IsInf(*report.Longitude, 0) || *report.Longitude < -180 || *report.Longitude > 180 {
			return nil, report, errors.New("longitude must be between -180 and 180")
		}
	}

	ingestedAt := time.Now().UTC()
	if parsed, err := parseUTCReportTime(report.ReceivedAt); err == nil {
		ingestedAt = parsed
	}

	attrs := make(map[string]any, len(report.Details)+8)
	for k, v := range report.Details {
		attrs[k] = v
	}
	attrs["submission_channel"] = "FIELD_REPORT"
	attrs["report_id"] = report.ReportID
	attrs["reporter_id"] = report.ReporterID
	setIfNonEmpty(attrs, "submitted_by", strings.TrimSpace(report.SubmittedBy))
	setIfNonEmpty(attrs, "description", strings.TrimSpace(report.Description))
	setIfNonEmpty(attrs, "received_at", strings.TrimSpace(report.ReceivedAt))
	setIfNonEmpty(attrs, "idempotency_key", strings.TrimSpace(report.IdempotencyKey))
	setIfNonEmpty(attrs, "payload_sha256", strings.TrimSpace(report.PayloadSHA256))

	var latPtr, lonPtr *float64
	if report.Latitude != nil {
		v := *report.Latitude
		latPtr = &v
	}
	if report.Longitude != nil {
		v := *report.Longitude
		lonPtr = &v
	}

	return &domain.HazardEvent{
		HazardID:    hazardID,
		Source:      source,
		SourceRefID: report.ReportID,
		HazardType:  ht,
		Severity:    sev,
		AreaName:    report.AreaName,
		Latitude:    latPtr,
		Longitude:   lonPtr,
		Attributes:  attrs,
		OccurredAt:  occurredAt,
		IngestedAt:  ingestedAt,
	}, report, nil
}

func parseUTCReportTime(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, errors.New("timestamp is empty")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UTC(), nil
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func setIfNonEmpty(attrs map[string]any, key, value string) {
	if value != "" {
		attrs[key] = value
	}
}

// FieldReportConsumer asynchronously persists staged field reports from bnpb.field-reports
// into canonical sharded storage + transactional outbox and commits offsets only after durable persistence.
type FieldReportConsumer struct {
	reader FieldReportMessageReader
	repo   HazardSaver
	log    *slog.Logger
	reg    *telemetry.Registry
	tracer *telemetry.Tracer
	now    func() time.Time
}

// NewFieldReportConsumer creates a FieldReportConsumer worker.
func NewFieldReportConsumer(reader FieldReportMessageReader, repo HazardSaver, log *slog.Logger) *FieldReportConsumer {
	if log == nil {
		log = slog.Default()
	}
	return &FieldReportConsumer{
		reader: reader,
		repo:   repo,
		log:    log.With(slog.String("worker", "field-report-consumer")),
		now:    func() time.Time { return time.Now().UTC() },
	}
}

// WithTelemetry attaches Prometheus metrics and OTLP tracing to the field report consumer.
func (c *FieldReportConsumer) WithTelemetry(reg *telemetry.Registry, tracer *telemetry.Tracer) *FieldReportConsumer {
	c.reg = reg
	c.tracer = tracer
	return c
}

// Start runs the consumer loop until ctx is canceled.
func (c *FieldReportConsumer) Start(ctx context.Context) {
	c.log.Info("starting field-report staging consumer")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.log.Info("stopping field-report staging consumer")
				return
			}
			c.log.Warn("field_report_fetch_failed", slog.String("error", err.Error()))
			if !sleepOrDone(ctx, fieldReportErrorBackoff) {
				return
			}
			continue
		}

		if err := c.ProcessMessage(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Warn("field_report_process_failed",
				slog.String("key", string(msg.Key)),
				slog.Int("partition", msg.Partition),
				slog.Int64("offset", msg.Offset),
				slog.String("error", err.Error()),
			)
			if !sleepOrDone(ctx, fieldReportErrorBackoff) {
				return
			}
		}
	}
}

// ProcessMessage handles a single Kafka message from bnpb.field-reports:
// invalid payloads are parked in dlq_messages before committing; valid reports
// are deduplicated by (source, source_ref_id) and persisted with outbox before committing.
func (c *FieldReportConsumer) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	started := time.Now()
	topic := msg.Topic
	if topic == "" {
		topic = defaultFieldReportsTopicName
	}

	correlationID := kafkaMessageHeader(msg, telemetry.CorrelationHeader)
	msgCtx := ctx
	if tp := kafkaMessageHeader(msg, telemetry.TraceparentHeader); tp != "" {
		msgCtx = telemetry.ContextWithTraceparent(msgCtx, tp)
	}
	var span *telemetry.Span
	if c.tracer != nil {
		msgCtx, span = c.tracer.StartSpan(msgCtx, "kafka.consume "+topic, telemetry.SpanKindConsumer,
			telemetry.SpanAttr{Key: "messaging.system", Value: "kafka"},
			telemetry.SpanAttr{Key: "messaging.destination", Value: topic},
			telemetry.SpanAttr{Key: "correlation_id", Value: correlationID},
		)
	}

	event, staged, normErr := NormalizeFieldReport(msg.Value, string(msg.Key))
	if normErr != nil {
		if err := c.parkInvalidMessage(ctx, msg, staged, normErr); err != nil {
			c.recordOutcome(topic, "error", started)
			if span != nil {
				span.End(503, err.Error())
			}
			return err
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.recordOutcome(topic, "error", started)
			if span != nil {
				span.End(500, err.Error())
			}
			return fmt.Errorf("commit parked field report offset: %w", err)
		}
		c.recordOutcome(topic, "invalid", started)
		if span != nil {
			span.End(400, normErr.Error())
		}
		return nil
	}

	if correlationID == "" {
		correlationID = strings.TrimSpace(staged.CorrelationID)
	}

	if existing, lookupErr := c.repo.GetHazardBySourceRef(msgCtx, event.Source, event.SourceRefID); lookupErr == nil && existing != nil {
		if isDuplicateFieldReport(existing, staged.PayloadSHA256) {
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.recordOutcome(topic, "error", started)
				if span != nil {
					span.End(500, err.Error())
				}
				return fmt.Errorf("commit duplicate field report offset: %w", err)
			}
			if c.reg != nil {
				c.reg.IncCounter("kafka_consumer_dedup_skipped_total", map[string]string{"service": "aggregator"}, 1)
			}
			c.recordOutcome(topic, "duplicate", started)
			if span != nil {
				span.End(200, "")
			}
			c.log.Info("skipped duplicate field report",
				slog.String("correlation_id", correlationID),
				slog.String("hazard_id", event.HazardID),
				slog.String("source", string(event.Source)),
				slog.String("report_id", staged.ReportID),
			)
			return nil
		}
	}

	if err := c.repo.SaveHazardWithOutbox(msgCtx, event, correlationID); err != nil {
		c.recordOutcome(topic, "error", started)
		if span != nil {
			span.End(503, err.Error())
		}
		return fmt.Errorf("save field report hazard %s: %w", event.HazardID, err)
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.recordOutcome(topic, "error", started)
		if span != nil {
			span.End(500, err.Error())
		}
		return fmt.Errorf("commit field report offset: %w", err)
	}

	c.recordOutcome(topic, "processed", started)
	if span != nil {
		span.End(200, "")
	}
	c.log.Info("persisted field report hazard",
		slog.String("correlation_id", correlationID),
		slog.String("hazard_id", event.HazardID),
		slog.String("source", string(event.Source)),
		slog.String("hazard_type", string(event.HazardType)),
		slog.String("severity", string(event.Severity)),
	)
	return nil
}

func (c *FieldReportConsumer) parkInvalidMessage(ctx context.Context, msg kafka.Message, staged StagedFieldReport, normErr error) error {
	msgID := formatFieldReportDLQID(staged.ReportID, string(msg.Key), msg.Value)
	c.log.Warn("field_report_invalid_parked",
		slog.String("message_id", msgID),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
		slog.String("error", normErr.Error()),
	)

	store, ok := c.repo.(UnprocessedStore)
	if !ok {
		return nil
	}

	source := domain.SourceBMKG
	if mapped, ok := MapFieldReportHazardTypeToSource(domain.HazardType(strings.ToUpper(strings.TrimSpace(staged.HazardType)))); ok {
		source = mapped
	}

	raw := slices.Clone(msg.Value)
	if raw == nil {
		raw = []byte{}
	}
	if len(raw) > maxFieldReportDLQPayload {
		raw = raw[:maxFieldReportDLQPayload]
	}
	receivedAt := c.now().UTC()
	if parsed, err := parseUTCReportTime(staged.ReceivedAt); err == nil {
		receivedAt = parsed
	}

	return store.SaveUnprocessedMessage(ctx, domain.UnprocessedMessage{
		MessageID:  msgID,
		Source:     source,
		ReceivedAt: receivedAt,
		Reason:     "field_report_invalid: " + normErr.Error(),
		Retryable:  false,
		Attempts:   1,
		Status:     domain.UnprocessedStatusParked,
		RawPayload: raw,
	})
}

func (c *FieldReportConsumer) recordOutcome(topic, status string, started time.Time) {
	if c.reg == nil {
		return
	}
	c.reg.IncCounter("kafka_messages_consumed_total", map[string]string{
		"service": "aggregator",
		"topic":   topic,
		"status":  status,
	}, 1)
	c.reg.ObserveHistogram("kafka_message_processing_duration_seconds", map[string]string{
		"service": "aggregator",
		"topic":   topic,
	}, time.Since(started).Seconds())
}

func isDuplicateFieldReport(existing *domain.HazardEvent, incomingSHA string) bool {
	if existing == nil {
		return false
	}
	incoming := strings.TrimSpace(incomingSHA)
	if incoming == "" {
		return true
	}
	stored, _ := existing.Attributes["payload_sha256"].(string)
	return strings.TrimSpace(stored) == incoming
}

func formatFieldReportDLQID(reportID, key string, raw []byte) string {
	const prefix = "field-report:"
	candidate := strings.TrimSpace(reportID)
	if candidate == "" {
		candidate = strings.TrimSpace(key)
	}
	if candidate != "" && len(prefix)+len(candidate) <= maxFieldReportHazardIDLen {
		return prefix + candidate
	}
	sum := sha256.Sum256(raw)
	return prefix + hex.EncodeToString(sum[:20])
}

func kafkaMessageHeader(msg kafka.Message, name string) string {
	for _, h := range msg.Headers {
		if strings.EqualFold(h.Key, name) {
			return string(h.Value)
		}
	}
	return ""
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
