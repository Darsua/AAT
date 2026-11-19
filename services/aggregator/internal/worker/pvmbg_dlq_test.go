package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/normalizer"
	"bnpb/aggregator/internal/repository"
	"bnpb/aggregator/internal/status"
)

type dlqTestStore struct {
	mu       sync.Mutex
	hazards  map[string]domain.HazardEvent // by source_ref_id
	dlq      map[string]domain.UnprocessedMessage
	dlqSaves int
}

func newDLQTestStore() *dlqTestStore {
	return &dlqTestStore{hazards: map[string]domain.HazardEvent{}, dlq: map[string]domain.UnprocessedMessage{}}
}

func (s *dlqTestStore) SaveHazardWithOutbox(_ context.Context, h *domain.HazardEvent, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hazards[h.SourceRefID] = *h
	return nil
}

func (s *dlqTestStore) GetHazardBySourceRef(_ context.Context, _ domain.Source, ref string) (*domain.HazardEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h, ok := s.hazards[ref]; ok {
		return &h, nil
	}
	return nil, nil
}

func (s *dlqTestStore) GetLatestOccurredAt(context.Context, domain.Source) (*time.Time, error) {
	return nil, nil
}

func (s *dlqTestStore) SaveUnprocessedMessage(_ context.Context, m domain.UnprocessedMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dlq[m.MessageID] = m
	s.dlqSaves++
	return nil
}

func (s *dlqTestStore) QueryAuditUnprocessed(_ context.Context, f repository.AuditUnprocessedFilter) (repository.AuditUnprocessedResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []domain.UnprocessedMessage
	for _, m := range s.dlq {
		if m.Status == domain.UnprocessedStatusRecovered || (f.Source != nil && m.Source != *f.Source) {
			continue
		}
		items = append(items, m)
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].ReceivedAt.Equal(items[j].ReceivedAt) {
			return items[i].ReceivedAt.Before(items[j].ReceivedAt)
		}
		return items[i].MessageID < items[j].MessageID
	})
	return repository.AuditUnprocessedResult{Items: items}, nil
}

func (s *dlqTestStore) active() []domain.UnprocessedMessage {
	res, _ := s.QueryAuditUnprocessed(context.Background(), repository.AuditUnprocessedFilter{})
	return res.Items
}

type pvmbgTestFeed struct {
	mu   sync.Mutex
	body string
}

func (f *pvmbgTestFeed) set(body string) { f.mu.Lock(); f.body = body; f.mu.Unlock() }

func (f *pvmbgTestFeed) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, _ = io.WriteString(w, f.body)
}

func newDLQTestPoller(t *testing.T, feed *pvmbgTestFeed, store *dlqTestStore) *PVMBGPoller {
	t.Helper()
	srv := httptest.NewServer(feed)
	t.Cleanup(srv.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := NewPVMBGPoller(adapter.NewPVMBGAdapter(srv.URL, "token", time.Second), normalizer.NewNormalizer(nil), store, status.NewTracker(), time.Second, log).
		WithDLQRetryBudget(3)
	clock := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	p.dlq.now = func() time.Time { clock = clock.Add(time.Minute); return clock }
	return p
}

const (
	reportA       = `{"report_id":"rpt-a","volcano_id":"VLC-001","alert_level":"Siaga","eruption_count_24h":3,"ash_column_height_m":500,"reported_at":"2026-11-17T01:00:00Z"}`
	renamedB      = `{"event_id":"rpt-b","volcano_id":"VLC-002","alert_level":"Waspada","eruption_count_24h":1,"ash_column_height_m":200,"reported_at":"2026-11-17T01:05:00Z"}`
	staleRenamedA = `{"event_id":"rpt-a","volcano_id":"VLC-001","alert_level":"Normal","eruption_count_24h":0,"ash_column_height_m":0,"reported_at":"2026-11-17T00:30:00Z"}`
)

func TestPVMBGIncompatibleReportParksOnceThenReplays(t *testing.T) {
	feed := &pvmbgTestFeed{body: "[" + reportA + "," + renamedB + "," + staleRenamedA + "]"}
	store := newDLQTestStore()
	p := newDLQTestPoller(t, feed, store)
	ctx := context.Background()

	if err := p.poll(ctx); err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if _, ok := store.hazards["rpt-a"]; !ok || len(store.hazards) != 1 {
		t.Fatalf("valid report should be stored alone: %v", store.hazards)
	}
	parked := store.active()
	if len(parked) != 2 || parked[0].Status != domain.UnprocessedStatusParked || parked[0].Retryable || parked[0].Attempts != 1 {
		t.Fatalf("parked = %+v", parked)
	}

	saves := store.dlqSaves
	if err := p.poll(ctx); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if store.dlqSaves != saves {
		t.Fatalf("re-polling the same incompatible payload rewrote the DLQ (%d -> %d saves)", saves, store.dlqSaves)
	}

	// System change: accept the renamed field, then replay.
	p.adapter.WithSchemaPolicy(adapter.PVMBGSchemaPolicy{FieldAliases: map[string]string{"event_id": "report_id"}})
	p.dlq.lastReplay = time.Time{}
	feed.set("[]")
	if err := p.poll(ctx); err != nil {
		t.Fatalf("replay poll: %v", err)
	}
	if len(store.active()) != 0 {
		t.Fatalf("recovered messages must leave the audit view: %+v", store.active())
	}
	if h, ok := store.hazards["rpt-b"]; !ok || h.Severity != domain.SeverityWaspada {
		t.Fatalf("replayed rpt-b = %+v", h)
	}
	// The parked older version of rpt-a must not overwrite the newer stored hazard.
	if h := store.hazards["rpt-a"]; !h.OccurredAt.Equal(time.Date(2026, 11, 17, 1, 0, 0, 0, time.UTC)) {
		t.Fatalf("rpt-a rolled back to %s", h.OccurredAt)
	}
	for _, m := range store.dlq {
		if m.Status != domain.UnprocessedStatusRecovered || len(m.RawPayload) == 0 {
			t.Fatalf("message %s = %s, raw kept = %v", m.MessageID, m.Status, len(m.RawPayload) > 0)
		}
	}
}

func TestPVMBGRetryableRejectionUsesBudgetThenParks(t *testing.T) {
	feed := &pvmbgTestFeed{body: "[" + reportA + `,{"report_id":"cut`}
	store := newDLQTestStore()
	p := newDLQTestPoller(t, feed, store)
	ctx := context.Background()

	for attempt := 1; attempt <= 2; attempt++ {
		if err := p.poll(ctx); !errors.Is(err, errRetryableRejection) {
			t.Fatalf("attempt %d err = %v, want retryable rejection", attempt, err)
		}
		m := store.active()[0]
		if m.Status != domain.UnprocessedStatusRetrying || m.Attempts != attempt || !m.Retryable {
			t.Fatalf("attempt %d message = %+v", attempt, m)
		}
	}
	if err := p.poll(ctx); err != nil {
		t.Fatalf("budget exhausted poll err = %v, want nil (parked)", err)
	}
	m := store.active()[0]
	if m.Status != domain.UnprocessedStatusParked || m.Attempts != 3 {
		t.Fatalf("after budget message = %+v", m)
	}
	first := m.ReceivedAt
	saves := store.dlqSaves
	if err := p.poll(ctx); err != nil || store.dlqSaves != saves {
		t.Fatalf("parked retryable payload kept looping: err=%v saves %d -> %d", err, saves, store.dlqSaves)
	}
	if !store.active()[0].ReceivedAt.Equal(first) {
		t.Fatal("received_at must stay the first receipt time")
	}

	// The upstream recovers: the next poll succeeds and replay recovers the parked body.
	feed.set("[" + reportA + "]")
	p.dlq.lastReplay = time.Time{}
	if err := p.poll(ctx); err != nil {
		t.Fatalf("recovered poll: %v", err)
	}
	if len(store.active()) != 1 {
		t.Fatalf("truncated body cannot decode yet, so it stays parked: %+v", store.active())
	}
}
