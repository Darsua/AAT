package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"

	"github.com/segmentio/kafka-go"
)

type mockOutboxStore struct {
	mu               sync.Mutex
	records          []domain.OutboxEvent
	publishedCalls   []domain.OutboxEvent
	quarantinedCalls []int64
	fetchErr         error
	publishErr       error
}

func (m *mockOutboxStore) RelayOutboxBatch(
	ctx context.Context,
	limit int,
	relayFn func(records []domain.OutboxEvent) (published []domain.OutboxEvent, quarantined []domain.OutboxEvent, err error),
) error {
	m.mu.Lock()
	if m.fetchErr != nil {
		m.mu.Unlock()
		return m.fetchErr
	}
	records := m.records
	m.mu.Unlock()

	if len(records) == 0 {
		return nil
	}

	published, quarantined, err := relayFn(records)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.publishedCalls = append(m.publishedCalls, published...)
	for _, q := range quarantined {
		m.quarantinedCalls = append(m.quarantinedCalls, q.ID)
	}
	m.mu.Unlock()
	return nil
}

type mockKafkaWriter struct {
	mu       sync.Mutex
	written  []kafka.Message
	writeErr error
	closed   bool
}

func (m *mockKafkaWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	m.written = append(m.written, msgs...)
	return nil
}

func (m *mockKafkaWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func TestOutboxRelay_Success(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	payload1, _ := json.Marshal(map[string]any{"hazard_id": "haz-1", "source": "BMKG"})
	payload2, _ := json.Marshal(map[string]any{"hazard_id": "haz-2", "source": "PVMBG"})

	store := &mockOutboxStore{
		records: []domain.OutboxEvent{
			{ID: 101, EventID: "evt-1", Payload: payload1, CorrelationID: "trace-bmkg-101", CreatedAt: time.Now()},
			{ID: 102, EventID: "evt-2", Payload: payload2, CreatedAt: time.Now()},
		},
	}

	writer := &mockKafkaWriter{}
	relay := NewOutboxRelay(store, writer, 50*time.Millisecond, discardLogger)

	// Run relay for one tick
	relay.relayBatch(context.Background())

	writer.mu.Lock()
	writtenCount := len(writer.written)
	writer.mu.Unlock()

	if writtenCount != 2 {
		t.Fatalf("expected 2 messages written to kafka, got %d", writtenCount)
	}

	if string(writer.written[0].Key) != "haz-1" || string(writer.written[1].Key) != "haz-2" {
		t.Errorf("unexpected partition keys: %s, %s", writer.written[0].Key, writer.written[1].Key)
	}

	// Verify correlation ID propagation
	if string(writer.written[0].Headers[0].Value) != "trace-bmkg-101" {
		t.Errorf("expected propagated correlation ID 'trace-bmkg-101', got '%s'", writer.written[0].Headers[0].Value)
	}
	if string(writer.written[1].Headers[0].Value) != "outbox-relay-102" {
		t.Errorf("expected fallback correlation ID 'outbox-relay-102', got '%s'", writer.written[1].Headers[0].Value)
	}

	store.mu.Lock()
	publishedCount := len(store.publishedCalls)
	store.mu.Unlock()

	if publishedCount != 2 {
		t.Fatalf("expected 2 records marked published, got %d", publishedCount)
	}
}

func TestOutboxRelay_QuarantinesMissingHazardID(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Payload with missing hazard_id must be skipped/quarantined rather than published with fallback key
	invalidPayload, _ := json.Marshal(map[string]any{"source": "BMKG", "severity": "WASPADA"})
	store := &mockOutboxStore{
		records: []domain.OutboxEvent{
			{ID: 301, EventID: "evt-corrupt", Payload: invalidPayload, CreatedAt: time.Now()},
		},
	}

	writer := &mockKafkaWriter{}
	relay := NewOutboxRelay(store, writer, 50*time.Millisecond, discardLogger)

	relay.relayBatch(context.Background())

	writer.mu.Lock()
	writtenCount := len(writer.written)
	writer.mu.Unlock()

	if writtenCount != 0 {
		t.Fatalf("expected 0 messages written to kafka for invalid payload missing hazard_id, got %d", writtenCount)
	}

	store.mu.Lock()
	quarantinedCount := len(store.quarantinedCalls)
	quarantinedID := int64(0)
	if quarantinedCount > 0 {
		quarantinedID = store.quarantinedCalls[0]
	}
	store.mu.Unlock()

	if quarantinedCount != 1 || quarantinedID != 301 {
		t.Fatalf("expected record 301 to be quarantined, got count=%d, id=%d", quarantinedCount, quarantinedID)
	}
}

func TestOutboxRelay_BrokerFailureRetainsRecords(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	payload, _ := json.Marshal(map[string]any{"hazard_id": "haz-1"})
	store := &mockOutboxStore{
		records: []domain.OutboxEvent{
			{ID: 201, EventID: "evt-201", Payload: payload, CreatedAt: time.Now()},
		},
	}

	writer := &mockKafkaWriter{
		writeErr: errors.New("kafka broker connection refused"),
	}

	relay := NewOutboxRelay(store, writer, 50*time.Millisecond, discardLogger)
	relay.relayBatch(context.Background())

	store.mu.Lock()
	publishedCount := len(store.publishedCalls)
	store.mu.Unlock()

	if publishedCount != 0 {
		t.Fatalf("records should not be marked published when broker write fails, got %d", publishedCount)
	}
}

type mockHazardSaver struct {
	mu      sync.Mutex
	saved   []*domain.HazardEvent
	saveErr error
}

func (m *mockHazardSaver) SaveHazardWithOutbox(_ context.Context, h *domain.HazardEvent, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.saved = append(m.saved, h)
	return nil
}

func (m *mockHazardSaver) GetHazardBySourceRef(_ context.Context, _ domain.Source, _ string) (*domain.HazardEvent, error) {
	return nil, nil
}

func (m *mockHazardSaver) GetLatestOccurredAt(_ context.Context, _ domain.Source) (*time.Time, error) {
	return nil, nil
}

func TestPoller_GracefulCancellation(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	saver := &mockHazardSaver{}

	ctx, cancel := context.WithCancel(context.Background())
	poller := NewBMKGPoller(nil, nil, saver, nil, 100*time.Millisecond, discardLogger)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		// Start with an already canceled or shortly canceled context
		cancel()
		poller.Start(ctx)
	}()

	select {
	case <-stopped:
		// Clean exit
	case <-time.After(1 * time.Second):
		t.Fatal("poller failed to stop cleanly on context cancellation")
	}
}
