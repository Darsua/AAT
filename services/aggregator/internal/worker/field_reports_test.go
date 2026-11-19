package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/telemetry"

	"github.com/segmentio/kafka-go"
)

type stubFieldReportReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	idx       int
	committed []kafka.Message
	closed    bool
}

func (r *stubFieldReportReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if r.idx < len(r.msgs) {
		msg := r.msgs[r.idx]
		r.idx++
		r.mu.Unlock()
		return msg, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *stubFieldReportReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.committed = append(r.committed, msgs...)
	return nil
}

func (r *stubFieldReportReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *stubFieldReportReader) committedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.committed)
}

type fieldReportTestStore struct {
	*dlqTestStore
	mu          sync.Mutex
	saveCalls   int
	correlations []string
	failSaveErr error
	failDLQErr  error
}

func newFieldReportTestStore() *fieldReportTestStore {
	return &fieldReportTestStore{dlqTestStore: newDLQTestStore()}
}

func (s *fieldReportTestStore) SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, correlationID string) error {
	s.mu.Lock()
	if s.failSaveErr != nil {
		err := s.failSaveErr
		s.mu.Unlock()
		return err
	}
	s.saveCalls++
	s.correlations = append(s.correlations, correlationID)
	s.mu.Unlock()
	return s.dlqTestStore.SaveHazardWithOutbox(ctx, h, correlationID)
}

func (s *fieldReportTestStore) SaveUnprocessedMessage(ctx context.Context, m domain.UnprocessedMessage) error {
	s.mu.Lock()
	if s.failDLQErr != nil {
		err := s.failDLQErr
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	return s.dlqTestStore.SaveUnprocessedMessage(ctx, m)
}

func metricsBody(t *testing.T, reg *telemetry.Registry) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	reg.Handler()(rec, req)
	return rec.Body.String()
}

func TestFieldReportConsumer_MappingsAttributesAndCommit(t *testing.T) {
	cases := []struct {
		hazardType string
		wantSource domain.Source
	}{
		{"SEISMIC", domain.SourceBMKG},
		{"VOLCANIC", domain.SourcePVMBG},
		{"WILDFIRE", domain.SourceSatelit},
		{"COASTAL", domain.SourceSensorPantai},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	lat := -7.5407
	lon := 110.4456

	for _, tc := range cases {
		t.Run(tc.hazardType, func(t *testing.T) {
			store := newFieldReportTestStore()
			reader := &stubFieldReportReader{}
			reg := telemetry.NewRegistry()
			tracer := telemetry.NewTracer("aggregator")
			consumer := NewFieldReportConsumer(reader, store, logger).WithTelemetry(reg, tracer)

			reportID := "rep-" + strings.ToLower(tc.hazardType) + "-001"
			payload, err := json.Marshal(StagedFieldReport{
				ReportID:       reportID,
				ReporterID:     "petugas-07",
				SubmittedBy:    "test-field-client",
				HazardType:     tc.hazardType,
				Severity:       "SIAGA",
				AreaName:       "Kabupaten Sleman, DIY",
				Latitude:       &lat,
				Longitude:      &lon,
				OccurredAt:     "2026-11-19T12:00:00Z",
				Description:    "Asap kawah terlihat tebal",
				Details:        map[string]any{"wind_dir": "NW"},
				ReceivedAt:     "2026-11-19T12:00:05Z",
				IdempotencyKey: "idem-" + reportID,
				PayloadSHA256:  "sha-" + reportID,
			})
			if err != nil {
				t.Fatalf("marshal staged report: %v", err)
			}

			msg := kafka.Message{
				Topic: "bnpb.field-reports",
				Key:   []byte(reportID),
				Value: payload,
				Headers: []kafka.Header{
					{Key: telemetry.CorrelationHeader, Value: []byte("corr-" + reportID)},
					{Key: telemetry.TraceparentHeader, Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
				},
			}
			if err := consumer.ProcessMessage(context.Background(), msg); err != nil {
				t.Fatalf("ProcessMessage() error = %v", err)
			}

			if reader.committedCount() != 1 {
				t.Fatalf("expected 1 committed offset, got %d", reader.committedCount())
			}

			saved, ok := store.hazards[reportID]
			if !ok {
				t.Fatalf("expected hazard with source_ref_id %q in store", reportID)
			}
			if saved.HazardID != "haz-field-"+reportID {
				t.Fatalf("expected HazardID %q, got %q", "haz-field-"+reportID, saved.HazardID)
			}
			if saved.Source != tc.wantSource {
				t.Fatalf("expected Source %s, got %s", tc.wantSource, saved.Source)
			}
			if saved.Attributes["submission_channel"] != "FIELD_REPORT" {
				t.Fatalf("expected submission_channel=FIELD_REPORT, got %v", saved.Attributes["submission_channel"])
			}
			if saved.Attributes["wind_dir"] != "NW" || saved.Attributes["reporter_id"] != "petugas-07" {
				t.Fatalf("expected details and reporter metadata in attributes, got %+v", saved.Attributes)
			}
			if len(store.correlations) != 1 || store.correlations[0] != "corr-"+reportID {
				t.Fatalf("expected correlation ID propagated to SaveHazardWithOutbox, got %+v", store.correlations)
			}

			body := metricsBody(t, reg)
			if !strings.Contains(body, `kafka_messages_consumed_total{service="aggregator",status="processed",topic="bnpb.field-reports"} 1`) {
				t.Fatalf("expected processed counter metric, got:\n%s", body)
			}
		})
	}
}

func TestFieldReportConsumer_DuplicateAndUpdateRecovery(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := newFieldReportTestStore()
	reader := &stubFieldReportReader{}
	reg := telemetry.NewRegistry()
	consumer := NewFieldReportConsumer(reader, store, logger).WithTelemetry(reg, nil)

	staged := StagedFieldReport{
		ReportID:      "rep-crash-recovery-1",
		ReporterID:    "petugas-01",
		HazardType:    "COASTAL",
		Severity:      "WASPADA",
		AreaName:      "Pelabuhan Ratu",
		OccurredAt:    "2026-11-19T13:00:00Z",
		ReceivedAt:    "2026-11-19T13:00:02Z",
		PayloadSHA256: "abc123",
	}
	payload, _ := json.Marshal(staged)
	msg := kafka.Message{Topic: "bnpb.field-reports", Key: []byte(staged.ReportID), Value: payload}

	if err := consumer.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("first ProcessMessage() failed: %v", err)
	}
	if store.saveCalls != 1 {
		t.Fatalf("expected 1 save call, got %d", store.saveCalls)
	}

	// Simulate broker redelivery (e.g., crash after SaveHazardWithOutbox before offset commit).
	if err := consumer.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("duplicate ProcessMessage() failed: %v", err)
	}
	if store.saveCalls != 1 {
		t.Fatalf("expected duplicate redelivery not to re-save, got saveCalls=%d", store.saveCalls)
	}
	if reader.committedCount() != 2 {
		t.Fatalf("expected both deliveries committed after persistence, got %d", reader.committedCount())
	}

	// Modified payload with same report_id (different PayloadSHA256) should upsert an update.
	staged.Severity = "AWAS"
	staged.PayloadSHA256 = "def456"
	updatedPayload, _ := json.Marshal(staged)
	if err := consumer.ProcessMessage(context.Background(), kafka.Message{Topic: "bnpb.field-reports", Key: []byte(staged.ReportID), Value: updatedPayload}); err != nil {
		t.Fatalf("updated ProcessMessage() failed: %v", err)
	}
	if store.saveCalls != 2 || store.hazards[staged.ReportID].Severity != domain.SeverityAwas {
		t.Fatalf("expected updated report to save with AWAS, saveCalls=%d saved=%+v", store.saveCalls, store.hazards[staged.ReportID])
	}

	body := metricsBody(t, reg)
	if !strings.Contains(body, `kafka_consumer_dedup_skipped_total{service="aggregator"} 1`) {
		t.Fatalf("expected dedup skipped metric, got:\n%s", body)
	}
}

func TestFieldReportConsumer_TransientStorageFailureDoesNotCommitOffset(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := newFieldReportTestStore()
	store.failSaveErr = errors.New("primary shard temporarily unreachable")
	reader := &stubFieldReportReader{}
	consumer := NewFieldReportConsumer(reader, store, logger)

	payload, _ := json.Marshal(StagedFieldReport{
		ReportID:      "rep-retry-01",
		ReporterID:    "petugas-09",
		HazardType:    "SEISMIC",
		Severity:      "SIAGA",
		AreaName:      "Bantul, DIY",
		OccurredAt:    "2026-11-19T14:00:00Z",
		PayloadSHA256: "hash-retry",
	})
	msg := kafka.Message{Topic: "bnpb.field-reports", Key: []byte("rep-retry-01"), Value: payload}

	if err := consumer.ProcessMessage(context.Background(), msg); err == nil {
		t.Fatal("expected error when storage fails")
	}
	if reader.committedCount() != 0 {
		t.Fatalf("expected 0 committed offsets on storage failure, got %d", reader.committedCount())
	}

	store.mu.Lock()
	store.failSaveErr = nil
	store.mu.Unlock()

	if err := consumer.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("retry after storage recovery failed: %v", err)
	}
	if store.saveCalls != 1 || reader.committedCount() != 1 {
		t.Fatalf("expected recovery retry to persist once and commit offset, saves=%d commits=%d", store.saveCalls, reader.committedCount())
	}
}

func TestFieldReportConsumer_InvalidPayloadParksInDLQAndCommits(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := newFieldReportTestStore()
	reader := &stubFieldReportReader{}
	reg := telemetry.NewRegistry()
	consumer := NewFieldReportConsumer(reader, store, logger).WithTelemetry(reg, nil)

	// Invalid JSON must park in dlq_messages before committing offset.
	badJSON := kafka.Message{
		Topic:     "bnpb.field-reports",
		Partition: 0,
		Offset:    4,
		Key:       []byte("rep-bad-json"),
		Value:     []byte(`{"report_id":"rep-bad-json",`),
	}
	if err := consumer.ProcessMessage(context.Background(), badJSON); err != nil {
		t.Fatalf("expected malformed payload to be parked and committed, got error = %v", err)
	}
	if reader.committedCount() != 1 {
		t.Fatalf("expected offset committed after DLQ park, got %d", reader.committedCount())
	}

	active := store.active()
	if len(active) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(active))
	}
	if active[0].MessageID != "field-report:rep-bad-json" || active[0].Status != domain.UnprocessedStatusParked || active[0].Retryable {
		t.Fatalf("unexpected DLQ record: %+v", active[0])
	}

	// If DLQ persistence fails, offset must NOT be committed.
	store.mu.Lock()
	store.failDLQErr = errors.New("dlq shard down")
	store.mu.Unlock()
	badFields := kafka.Message{
		Topic: "bnpb.field-reports",
		Key:   []byte("rep-bad-type"),
		Value: []byte(`{"report_id":"rep-bad-type","reporter_id":"p1","hazard_type":"METEOR","severity":"SIAGA","area_name":"X","occurred_at":"2026-11-19T12:00:00Z"}`),
	}
	if err := consumer.ProcessMessage(context.Background(), badFields); err == nil {
		t.Fatal("expected error when DLQ persistence fails")
	}
	if reader.committedCount() != 1 {
		t.Fatalf("expected offset not to advance when DLQ save fails, commits=%d", reader.committedCount())
	}

	// Ensure unused time import warning never happens by exercising timestamp ordering check cleanly.
	if active[0].ReceivedAt.After(time.Now().UTC().Add(time.Minute)) {
		t.Fatalf("unexpected future DLQ received_at: %v", active[0].ReceivedAt)
	}
}
