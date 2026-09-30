package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"bnpb/aggregator/internal/domain"

	"github.com/segmentio/kafka-go"
)

// OutboxStore defines database interactions required by the outbox relay.
type OutboxStore interface {
	FetchUnprocessedOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, ids []int64) error
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

	records, err := r.store.FetchUnprocessedOutbox(batchCtx, 50)
	if err != nil {
		r.log.Warn("failed to fetch unprocessed outbox records", "error", err)
		return
	}

	if len(records) == 0 {
		return
	}

	messages := make([]kafka.Message, 0, len(records))
	publishedIDs := make([]int64, 0, len(records))

	for _, rec := range records {
		// Extract hazard_id from payload for partition key
		var hazard struct {
			HazardID string `json:"hazard_id"`
		}
		if err := json.Unmarshal(rec.Payload, &hazard); err != nil {
			r.log.Error("malformed outbox payload, skipping", "id", rec.ID, "error", err)
			continue
		}

		key := hazard.HazardID
		if key == "" {
			key = rec.EventID
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(key),
			Value: rec.Payload,
			Headers: []kafka.Header{
				{Key: "X-Correlation-ID", Value: []byte(fmt.Sprintf("outbox-relay-%d", rec.ID))},
			},
			Time: rec.CreatedAt,
		})
		publishedIDs = append(publishedIDs, rec.ID)
	}

	if len(messages) == 0 {
		return
	}

	// Write batch to Kafka / Redpanda
	if err := r.writer.WriteMessages(batchCtx, messages...); err != nil {
		r.log.Warn("failed to publish outbox batch to kafka broker, will retry next tick", "error", err, "count", len(messages))
		return
	}

	// Mark batch as published in PostgreSQL
	if err := r.store.MarkOutboxPublished(batchCtx, publishedIDs); err != nil {
		r.log.Error("failed to mark outbox records as published in database", "error", err, "count", len(publishedIDs))
		return
	}

	r.log.Info("successfully dispatched outbox batch to redpanda", slog.Int("dispatched_count", len(messages)))
}
