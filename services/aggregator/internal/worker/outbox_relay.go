package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/telemetry"

	"github.com/segmentio/kafka-go"
)

// OutboxStore defines database interactions required by the outbox relay.
type OutboxStore interface {
	RelayOutboxBatch(
		ctx context.Context,
		limit int,
		relayFn func(records []domain.OutboxEvent) (published []domain.OutboxEvent, quarantined []domain.OutboxEvent, err error),
	) error
}

// KafkaMessageWriter abstracts the kafka.Writer for testability.
type KafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// OutboxRelay periodically polls unpublished outbox records and dispatches them to Redpanda/Kafka.
type OutboxRelay struct {
	store    OutboxStore
	writer   KafkaMessageWriter
	interval time.Duration
	log      *slog.Logger
	reg      *telemetry.Registry
	tracer   *telemetry.Tracer
}

// WithTelemetry attaches optional Prometheus metrics and OTLP tracing to the relay.
func (r *OutboxRelay) WithTelemetry(reg *telemetry.Registry, tracer *telemetry.Tracer) {
	r.reg = reg
	r.tracer = tracer
}

// NewOutboxRelay creates a new OutboxRelay instance.
func NewOutboxRelay(
	store OutboxStore,
	writer KafkaMessageWriter,
	interval time.Duration,
	log *slog.Logger,
) *OutboxRelay {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &OutboxRelay{
		store:    store,
		writer:   writer,
		interval: interval,
		log:      log.With(slog.String("worker", "outbox-relay")),
	}
}

// Start runs the outbox relay loop until the context is canceled.
func (r *OutboxRelay) Start(ctx context.Context) {
	r.log.Info("starting transactional outbox relay worker", slog.Duration("interval", r.interval))

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("stopping transactional outbox relay worker")
			return
		case <-ticker.C:
			r.relayBatch(ctx)
		}
	}
}

func (r *OutboxRelay) relayBatch(ctx context.Context) {
	batchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err := r.store.RelayOutboxBatch(batchCtx, 50, func(records []domain.OutboxEvent) ([]domain.OutboxEvent, []domain.OutboxEvent, error) {
		messages := make([]kafka.Message, 0, len(records))
		published := make([]domain.OutboxEvent, 0, len(records))
		quarantined := make([]domain.OutboxEvent, 0)
		spans := make([]*telemetry.Span, 0, len(records))

		for _, rec := range records {
			// Extract hazard_id from payload for partition key
			var hazard struct {
				HazardID string `json:"hazard_id"`
			}
			if err := json.Unmarshal(rec.Payload, &hazard); err != nil {
				r.log.Error("malformed outbox payload, quarantining record", "id", rec.ID, "error", err)
				quarantined = append(quarantined, rec)
				continue
			}

			if hazard.HazardID == "" {
				r.log.Error("outbox record missing required hazard_id in payload, quarantining invalid event", "id", rec.ID, "event_id", rec.EventID)
				quarantined = append(quarantined, rec)
				continue
			}

			corrID := rec.CorrelationID
			if corrID == "" {
				corrID = fmt.Sprintf("outbox-relay-%d", rec.ID)
			}

			headers := []kafka.Header{
				{Key: "X-Correlation-ID", Value: []byte(corrID)},
			}
			if r.tracer != nil {
				msgCtx, span := r.tracer.StartSpan(batchCtx, "kafka.publish outbox", telemetry.SpanKindProducer,
					telemetry.SpanAttr{Key: "messaging.system", Value: "kafka"},
					telemetry.SpanAttr{Key: "hazard_id", Value: hazard.HazardID},
					telemetry.SpanAttr{Key: "correlation_id", Value: corrID},
				)
				if tp := telemetry.TraceparentFromContext(msgCtx); tp != "" {
					headers = append(headers, kafka.Header{Key: telemetry.TraceparentHeader, Value: []byte(tp)})
				}
				spans = append(spans, span)
			}

			messages = append(messages, kafka.Message{
				Key:     []byte(hazard.HazardID),
				Value:   rec.Payload,
				Headers: headers,
				Time:    rec.CreatedAt,
			})
			published = append(published, rec)
		}

		if len(messages) > 0 {
			if err := r.writer.WriteMessages(batchCtx, messages...); err != nil {
				for _, span := range spans {
					span.End(500, err.Error())
				}
				r.reg.IncCounter("outbox_errors_total", map[string]string{"service": "aggregator"}, 1)
				r.log.Warn("failed to publish outbox batch to kafka broker, will retry next tick", "error", err, "count", len(messages))
				return nil, nil, err
			}
			for _, span := range spans {
				span.End(200, "")
			}
			r.reg.IncCounter("outbox_published_total", map[string]string{"service": "aggregator"}, float64(len(messages)))
			r.log.Info("successfully dispatched outbox batch to redpanda", slog.Int("dispatched_count", len(messages)))
		}

		return published, quarantined, nil
	})

	if err != nil {
		r.log.Warn("outbox relay batch failed", "error", err)
	}
}
