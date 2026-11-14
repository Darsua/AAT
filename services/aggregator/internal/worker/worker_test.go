package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/normalizer"
	"bnpb/aggregator/internal/repository"
	"bnpb/aggregator/internal/status"

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

func TestComputeBackoff_PreventsShiftOverflow(t *testing.T) {
	interval := 5 * time.Second
	maxBackoff := 30 * time.Second

	// Test boundary and high retry counts that would otherwise overflow int / time.Duration shifts
	testCases := []int{0, 1, 2, 5, 6, 7, 10, 31, 32, 62, 63, 64, 100, 1000, 10000}
	for _, count := range testCases {
		d := computeBackoff(interval, count, maxBackoff)
		if d < interval {
			t.Errorf("retryCount=%d: backoff %v is less than base interval %v", count, d, interval)
		}
		// Max allowed delay is maxBackoff + maxJitter (1s)
		if d > maxBackoff+1*time.Second {
			t.Errorf("retryCount=%d: backoff %v exceeded maximum threshold %v", count, d, maxBackoff+1*time.Second)
		}
	}
}

func TestMultiReplicaBMKGPoller_ExclusiveCollectionAndZeroGapFailover(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	backends := map[string]*repository.MemoryUnitBackend{
		"shard-1":    repository.NewMemoryUnitBackend(),
		"shard-1-r1": repository.NewMemoryUnitBackend(),
		"shard-2":    repository.NewMemoryUnitBackend(),
		"shard-2-r1": repository.NewMemoryUnitBackend(),
	}
	groups := []repository.ShardGroup{
		{
			ShardID: "shard-1",
			Units: []repository.StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: backends["shard-1"]},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: backends["shard-1-r1"]},
			},
		},
		{
			ShardID: "shard-2",
			Units: []repository.StorageUnit{
				{Unit: "shard-2", Node: "ruby", Role: domain.StorageRolePrimary, Backend: backends["shard-2"]},
				{Unit: "shard-2-r1", Node: "kana", Role: domain.StorageRoleReplica, Backend: backends["shard-2-r1"]},
			},
		},
	}
	repo, err := repository.NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("failed to create sharded repo: %v", err)
	}

	baseTime := time.Date(2026, 11, 14, 14, 0, 0, 0, time.UTC)
	phase1Events := []adapter.SeismicEventDTO{
		{EventID: "eq-001", Magnitude: 5.2, DepthKM: 10, EpicenterLat: -7.5, EpicenterLon: 110.4, RegionName: "Yogyakarta", OccurredAt: baseTime.Add(1 * time.Second)},
		{EventID: "eq-002", Magnitude: 5.8, DepthKM: 12, EpicenterLat: -8.1, EpicenterLon: 112.9, RegionName: "Malang", OccurredAt: baseTime.Add(2 * time.Second)},
	}
	phase2Events := []adapter.SeismicEventDTO{
		phase1Events[0],
		phase1Events[1],
		{EventID: "eq-003", Magnitude: 6.1, DepthKM: 15, EpicenterLat: -6.2, EpicenterLon: 105.4, RegionName: "Sunda Strait", OccurredAt: baseTime.Add(3 * time.Second)},
		{EventID: "eq-004", Magnitude: 4.7, DepthKM: 20, EpicenterLat: -2.5, EpicenterLon: 113.8, RegionName: "Palu", OccurredAt: baseTime.Add(4 * time.Second)},
	}

	var mu sync.Mutex
	includePhase2 := false
	var inFlight atomic.Int32
	var maxConcurrent atomic.Int32
	var totalSeismicCalls atomic.Int32

	mockBMKG := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/seismic-events":
			totalSeismicCalls.Add(1)
			cur := inFlight.Add(1)
			for {
				prev := maxConcurrent.Load()
				if cur <= prev || maxConcurrent.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			inFlight.Add(-1)

			mu.Lock()
			list := phase1Events
			if includePhase2 {
				list = phase2Events
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(list)
		case "/tsunami-warnings":
			_ = json.NewEncoder(w).Encode([]adapter.TsunamiWarningDTO{})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mockBMKG.Close()

	norm := normalizer.NewNormalizer(nil)
	pollInterval := 35 * time.Millisecond
	leaseTTL := 250 * time.Millisecond
	renewInterval := 60 * time.Millisecond

	replicas := []struct {
		id     string
		poller *BMKGPoller
		cancel context.CancelFunc
		done   chan struct{}
	}{
		{id: "agg-1"},
		{id: "agg-2"},
		{id: "agg-3"},
	}

	ctx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()

	for i := range replicas {
		bmkgAdapter := adapter.NewBMKGAdapter(mockBMKG.URL, "", 2*time.Second)
		p := NewBMKGPoller(bmkgAdapter, norm, repo, status.NewTracker(), pollInterval, discardLogger).
			WithLeaseConfig(replicas[i].id, leaseTTL, renewInterval)
		rCtx, rCancel := context.WithCancel(ctx)
		done := make(chan struct{})
		replicas[i].poller = p
		replicas[i].cancel = rCancel
		replicas[i].done = done
		go func() {
			defer close(done)
			p.Start(rCtx)
		}()
	}

	// Wait until leader has ingested Phase 1 (2 events) and checkpoint is persisted.
	deadline := time.Now().Add(5 * time.Second)
	var firstLeader string
	var firstToken int64
	for time.Now().Before(deadline) {
		_, total, _ := repo.QueryHazards(ctx, repository.HazardFilter{Limit: 10})
		cp, _ := repo.GetSourceCheckpoint(ctx, domain.SourceBMKG)
		if total == 2 && cp != nil && cp.HolderID != "" {
			firstLeader = cp.HolderID
			firstToken = cp.FencingToken
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if firstLeader == "" {
		t.Fatalf("leader did not complete phase 1 ingestion and checkpoint within timeout")
	}

	// Advance to phase 2 and shut down the active leader so standby takes over cleanly.
	mu.Lock()
	includePhase2 = true
	mu.Unlock()

	for i := range replicas {
		if replicas[i].id == firstLeader {
			replicas[i].cancel()
			<-replicas[i].done
		}
	}

	// Verify standby takes over, advances checkpoint, and ingests all 4 events with 0 gaps / 0 duplicates.
	deadline = time.Now().Add(5 * time.Second)
	var secondLeader string
	for time.Now().Before(deadline) {
		_, total, _ := repo.QueryHazards(ctx, repository.HazardFilter{Limit: 20})
		cp, _ := repo.GetSourceCheckpoint(ctx, domain.SourceBMKG)
		if total == 4 && cp != nil && cp.HolderID != firstLeader && cp.FencingToken > firstToken {
			secondLeader = cp.HolderID
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if secondLeader == "" {
		t.Fatalf("standby did not take over from %s and complete phase 2 ingestion", firstLeader)
	}

	if maxConcurrent.Load() > 1 {
		t.Errorf("expected single-leader exclusive upstream polling (maxConcurrent<=1), got %d", maxConcurrent.Load())
	}

	// Confirm former leader's stale fencing token is rejected at storage.
	staleEv := domain.HazardEvent{
		HazardID:    "haz-bmkg-stale-zombie",
		Source:      domain.SourceBMKG,
		SourceRefID: "eq-stale-zombie",
		HazardType:  domain.HazardTypeSeismic,
		Severity:    domain.SeverityNormal,
		AreaName:    "ZombieTest",
		OccurredAt:  baseTime.Add(10 * time.Second),
	}
	if err := repo.SaveHazardWithFence(ctx, &staleEv, "corr-zombie", domain.WriteFence{
		Source:       domain.SourceBMKG,
		HolderID:     firstLeader,
		FencingToken: firstToken,
	}); !errors.Is(err, repository.ErrStaleFencingToken) {
		t.Fatalf("expected ErrStaleFencingToken from killed leader token %d, got %v", firstToken, err)
	}

	cancelAll()
	for i := range replicas {
		<-replicas[i].done
	}
}
