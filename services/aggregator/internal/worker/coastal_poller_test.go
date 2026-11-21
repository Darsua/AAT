package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/status"
)

type stubCoastalClient struct {
	stations map[string]adapter.CoastalStation
	batch    *adapter.CoastalObservationBatch
	err      error
	calls    int
}

func (s *stubCoastalClient) FetchStations(context.Context) (map[string]adapter.CoastalStation, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.stations, nil
}

func (s *stubCoastalClient) FetchObservations(context.Context, int, string, map[string]adapter.CoastalStation, time.Time) (*adapter.CoastalObservationBatch, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.batch, nil
}

type recordingHazardSaver struct {
	saved       []*domain.HazardEvent
	unprocessed []domain.UnprocessedMessage
	saveDelay   time.Duration
}

func (r *recordingHazardSaver) SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, _ string) error {
	if r.saveDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.saveDelay):
		}
	}
	r.saved = append(r.saved, h)
	return nil
}

func (r *recordingHazardSaver) GetHazardBySourceRef(context.Context, domain.Source, string) (*domain.HazardEvent, error) {
	return nil, nil
}

func (r *recordingHazardSaver) GetLatestOccurredAt(context.Context, domain.Source) (*time.Time, error) {
	return nil, nil
}

func (r *recordingHazardSaver) SaveUnprocessedMessage(_ context.Context, msg domain.UnprocessedMessage) error {
	r.unprocessed = append(r.unprocessed, msg)
	return nil
}

func TestCoastalPoller_CircuitBreakerAndAutonomousRecovery(t *testing.T) {
	now := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	cb := NewCircuitBreaker(2, 5*time.Second)
	cb.nowFn = func() time.Time { return now }

	if !cb.Allow() {
		t.Fatal("closed breaker must allow")
	}
	cb.RecordFailure()
	if cb.State() != BreakerClosed {
		t.Fatalf("expected closed after 1 failure, got %s", cb.State())
	}
	cb.RecordFailure()
	if cb.State() != BreakerOpen {
		t.Fatalf("expected open after 2 failures, got %s", cb.State())
	}
	if cb.Allow() {
		t.Fatal("open breaker must block before cooldown")
	}

	now = now.Add(6 * time.Second)
	if !cb.Allow() {
		t.Fatal("half-open probe must be allowed after cooldown")
	}
	if cb.Allow() {
		t.Fatal("only one concurrent half-open probe should be allowed")
	}
	cb.RecordSuccess()
	if cb.State() != BreakerClosed {
		t.Fatalf("expected closed after probe success, got %s", cb.State())
	}
}

func TestCoastalPoller_PersistsCriticalQuarantinesInvalidAndCachesFallback(t *testing.T) {
	client := &stubCoastalClient{
		stations: map[string]adapter.CoastalStation{
			"stn-padang-01": {StationID: "stn-padang-01", Name: "Padang", Province: "Sumatera Barat", Latitude: -0.95, Longitude: 100.35},
		},
		batch: &adapter.CoastalObservationBatch{
			GeneratedAt: time.Now().UTC(),
			Valid: []adapter.CoastalObservation{
				{
					ObservationID:    "obs-crit-1",
					StationID:        "stn-padang-01",
					ObservedAt:       "2026-11-12T02:43:00Z",
					SeaLevelAnomalyM: 0.9,
					WaveHeightM:      2.0,
					Quality:          "ok",
					Critical:         true,
				},
			},
			Rejected: []adapter.RejectedObservation{
				{
					ObservationID: "obs-bad-1",
					StationID:     "stn-padang-01",
					Reason:        "invalid wave_height_m -1",
					RawPayload:    []byte(`{"observation_id":"obs-bad-1"}`),
				},
			},
		},
	}

	repo := &recordingHazardSaver{}
	tracker := status.NewTracker()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	poller := NewCoastalPoller(client, repo, tracker, time.Second, time.Second, 30*time.Second, log)

	if err := poller.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce failed: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved hazard, got %d", len(repo.saved))
	}
	saved := repo.saved[0]
	if saved.Source != domain.SourceSensorPantai || saved.HazardType != domain.HazardTypeCoastal || saved.Severity != domain.SeverityAwas {
		t.Fatalf("unexpected normalized coastal event: %+v", saved)
	}
	if len(poller.Quarantine().List()) != 1 {
		t.Fatalf("expected 1 quarantined item, got %d", len(poller.Quarantine().List()))
	}
	if len(repo.unprocessed) != 1 || repo.unprocessed[0].Source != domain.SourceSensorPantai {
		t.Fatalf("expected 1 unprocessed DLQ message for sensor-pantai, got %+v", repo.unprocessed)
	}

	snap := poller.FallbackSnapshot()
	if snap.Stale || len(snap.Events) != 1 || snap.LastSuccessAt == nil {
		t.Fatalf("unexpected fresh fallback snapshot: %+v", snap)
	}

	// Simulate outage: breaker opens after 3 failures and marks coastal degraded while BMKG stays healthy.
	client.err = errors.New("sensor timeout")
	for i := 0; i < 3; i++ {
		_ = poller.pollOnce(context.Background())
	}
	coastalType := domain.HazardTypeCoastal
	degraded, staleSince := tracker.GetDegradedStatus(&coastalType)
	if !degraded || staleSince == nil {
		t.Fatalf("expected coastal degraded=true with stale_since, got degraded=%v staleSince=%v", degraded, staleSince)
	}
	bmkgType := domain.HazardTypeSeismic
	bmkgDegraded, _ := tracker.GetDegradedStatus(&bmkgType)
	if bmkgDegraded {
		t.Fatal("BMKG must remain healthy while coastal is unhealthy")
	}
}

func TestCoastalPoller_PersistNotCanceledByHTTPTimeout(t *testing.T) {
	client := &stubCoastalClient{
		stations: map[string]adapter.CoastalStation{
			"stn-padang-01": {StationID: "stn-padang-01", Name: "Padang", Province: "Sumatera Barat", Latitude: -0.95, Longitude: 100.35},
		},
		batch: &adapter.CoastalObservationBatch{
			GeneratedAt: time.Now().UTC(),
			Valid: []adapter.CoastalObservation{
				{
					ObservationID:    "obs-slow-db-1",
					StationID:        "stn-padang-01",
					ObservedAt:       "2026-11-20T14:00:00Z",
					SeaLevelAnomalyM: 0.4,
					WaveHeightM:      1.2,
					Quality:          "ok",
				},
			},
		},
	}

	repo := &recordingHazardSaver{saveDelay: 60 * time.Millisecond}
	tracker := status.NewTracker()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	poller := NewCoastalPoller(client, repo, tracker, time.Second, 40*time.Millisecond, 30*time.Second, log)

	if err := poller.pollOnce(context.Background()); err != nil {
		t.Fatalf("expected persistence exceeding HTTP reqTimeout to succeed, got: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved hazard, got %d", len(repo.saved))
	}
	coastalType := domain.HazardTypeCoastal
	degraded, _ := tracker.GetDegradedStatus(&coastalType)
	if degraded {
		t.Fatal("expected coastal status to remain healthy")
	}
}

type failingLeaseCoordinator struct {
	*recordingHazardSaver
}

var _ FencedCoordinator = failingLeaseCoordinator{}

func (failingLeaseCoordinator) AcquireOrRenewSourceLease(context.Context, domain.Source, string, int64, time.Duration) (domain.LeaderLease, bool, error) {
	return domain.LeaderLease{}, false, errors.New("coordinator unreachable")
}

func (failingLeaseCoordinator) ReleaseSourceLease(context.Context, domain.Source, string, int64) error {
	return nil
}

func (failingLeaseCoordinator) SaveHazardWithFence(context.Context, *domain.HazardEvent, string, domain.WriteFence) error {
	return nil
}

func (failingLeaseCoordinator) GetSourceCheckpoint(context.Context, domain.Source) (*domain.SourceCheckpoint, error) {
	return nil, nil
}

func (failingLeaseCoordinator) SaveSourceCheckpoint(context.Context, domain.SourceCheckpoint) error {
	return nil
}

func TestCoastalPoller_StandbyOrLeaseErrorDoesNotWedgeHalfOpenBreaker(t *testing.T) {
	now := time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC)
	cb := NewCircuitBreaker(1, time.Second)
	cb.nowFn = func() time.Time { return now }
	cb.RecordFailure() // opens breaker at now..now+1s

	now = now.Add(2 * time.Second) // cooldown elapsed -> next Allow() would enter half-open
	poller := NewCoastalPoller(&stubCoastalClient{}, nil, nil, time.Second, time.Second, 30*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithLeaseConfig("agg-standby", 15*time.Second, 5*time.Second)
	poller.breaker = cb
	poller.lease.repo = failingLeaseCoordinator{recordingHazardSaver: &recordingHazardSaver{}}

	if err := poller.pollOnce(context.Background()); err == nil {
		t.Fatal("expected lease acquisition error")
	}
	if cb.State() != BreakerOpen {
		t.Fatalf("expected breaker to stay open rather than wedging half-open on lease error, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected half-open probe slot to remain free after lease failure")
	}
}
