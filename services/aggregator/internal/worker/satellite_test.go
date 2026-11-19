package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/repository"
	"bnpb/aggregator/internal/satellite"
)

var errFeedDrained = errors.New("feed drained")

type fakeSatelliteFeed struct{ events []satellite.Event }

func (f *fakeSatelliteFeed) GetSession(context.Context) (satellite.Session, error) {
	return satellite.Session{LatestOffset: int64(len(f.events))}, nil
}

func (f *fakeSatelliteFeed) Run(ctx context.Context, lastApplied int64, handle func(satellite.Event) error) error {
	for _, ev := range f.events {
		if ev.Offset <= lastApplied {
			continue
		}
		if err := handle(ev); err != nil {
			return err
		}
	}
	return errFeedDrained
}

type fakeSatelliteStore struct {
	mu         sync.Mutex
	hazards    map[string]domain.HazardEvent
	cp         *repository.SatelliteCheckpoint
	failOffset int64 // fail the first hazard write for this offset once
	saves      int
	writers    map[string]bool // correlation IDs that wrote a hazard
}

func (s *fakeSatelliteStore) SaveHazardWithOutbox(_ context.Context, h *domain.HazardEvent, corrID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failOffset != 0 && corrID == fmt.Sprintf("satelit-%d", s.failOffset) {
		s.failOffset = 0
		return errors.New("simulated crash during persist")
	}
	s.hazards[h.HazardID] = *h
	s.saves++
	if s.writers == nil {
		s.writers = map[string]bool{}
	}
	s.writers[corrID] = true
	return nil
}

func (s *fakeSatelliteStore) LoadSatelliteCheckpoint(context.Context, string) (*repository.SatelliteCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cp == nil {
		return nil, nil
	}
	cp := *s.cp
	return &cp, nil
}

func (s *fakeSatelliteStore) SaveSatelliteCheckpoint(_ context.Context, cp repository.SatelliteCheckpoint, _ *domain.WriteFence) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cp != nil && s.cp.LastOffset > cp.LastOffset {
		return nil
	}
	cp.State = append([]byte(nil), cp.State...)
	s.cp = &cp
	return nil
}

func (s *fakeSatelliteStore) GetHazardBySourceRef(context.Context, domain.Source, string) (*domain.HazardEvent, error) {
	return nil, nil
}

func (s *fakeSatelliteStore) GetLatestOccurredAt(context.Context, domain.Source) (*time.Time, error) {
	return nil, nil
}

func satEvent(offset int64, typ, lat, hhmm, conf string, frp float64) satellite.Event {
	data := fmt.Sprintf(`{"satellite":"N20","acq_date":"2026-11-10","acq_time":"%s","latitude":"%s","longitude":"113.5","confidence":"%s","frp":%v}`, hhmm, lat, conf, frp)
	return satellite.Event{Offset: offset, Type: typ, Data: []byte(data)}
}

func satelliteScript() []satellite.Event {
	return []satellite.Event{
		satEvent(1, satellite.EventDetection, "-2.000", "0100", "n", 10),
		satEvent(2, satellite.EventDetection, "-2.005", "0130", "h", 15),
		satEvent(3, satellite.EventDetection, "-2.050", "0200", "n", 5),
		satEvent(4, satellite.EventDetection, "-2.055", "0210", "n", 5),
		{Offset: 5, Type: satellite.EventDetection, Data: []byte(`{"satellite":`)}, // poison
		satEvent(6, satellite.EventDetection, "-2.010", "0300", "n", 40),
		satEvent(7, satellite.EventUpdate, "-2.000", "0100", "h", 60),
		satEvent(8, satellite.EventRetract, "-2.055", "0210", "n", 5),
	}
}

func newTestSatelliteWorker(store *fakeSatelliteStore, interval time.Duration) *SatelliteWorker {
	w := NewSatelliteWorker(&fakeSatelliteFeed{events: satelliteScript()}, store, satellite.Config{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.checkpointInterval = interval
	return w
}

func summarize(hazards map[string]domain.HazardEvent) map[string]string {
	out := map[string]string{}
	for id, h := range hazards {
		out[id] = fmt.Sprintf("%s count=%v frp=%v status=%v", h.Severity, h.Attributes["detection_count"], h.Attributes["total_frp_mw"], h.Attributes["lifecycle_status"])
	}
	return out
}

func TestSatelliteWorkerPersistsFiresAndLifecycle(t *testing.T) {
	store := &fakeSatelliteStore{hazards: map[string]domain.HazardEvent{}}
	w := newTestSatelliteWorker(store, 0)
	if err := w.runOnce(context.Background(), nil); !errors.Is(err, errFeedDrained) {
		t.Fatalf("runOnce = %v", err)
	}
	if store.cp == nil || store.cp.LastOffset != 8 {
		t.Fatalf("checkpoint = %+v, want offset 8", store.cp)
	}
	var active, retired int
	for _, h := range store.hazards {
		if h.Source != "SATELIT" || h.HazardType != "WILDFIRE" {
			t.Fatalf("hazard = %+v", h)
		}
		switch h.Attributes["lifecycle_status"] {
		case "active":
			active++
			if h.Severity != domain.SeverityAwas { // 3 detections, 10+15+40 then 60+15+40 MW
				t.Fatalf("active fire severity = %s, attrs = %v", h.Severity, h.Attributes)
			}
		case "retired":
			retired++
			if h.Attributes["retired_reason"] != satellite.RetireBelowThreshold {
				t.Fatalf("retired = %v", h.Attributes)
			}
			if h.Severity != domain.SeverityNormal {
				t.Fatalf("retired fire severity = %s, want NORMAL", h.Severity)
			}
		}
	}
	if active != 1 || retired != 1 {
		t.Fatalf("active=%d retired=%d, hazards = %v", active, retired, summarize(store.hazards))
	}
	if st := w.Status(); st.RejectedEvents != 1 || st.LastAppliedOffset != 8 || st.LagEvents != 0 || st.ActiveFires != 1 {
		t.Fatalf("status = %+v", st)
	}
}

func TestSatelliteWorkerCrashRecoveryMatchesUninterruptedRun(t *testing.T) {
	reference := &fakeSatelliteStore{hazards: map[string]domain.HazardEvent{}}
	if err := newTestSatelliteWorker(reference, 0).runOnce(context.Background(), nil); !errors.Is(err, errFeedDrained) {
		t.Fatalf("reference run = %v", err)
	}
	want := summarize(reference.hazards)

	for _, interval := range []time.Duration{0, time.Hour} {
		for crashAt := int64(1); crashAt <= 8; crashAt++ {
			if !reference.writers[fmt.Sprintf("satelit-%d", crashAt)] {
				continue // singletons and the poison event write nothing
			}
			t.Run(fmt.Sprintf("interval=%s/crash=%d", interval, crashAt), func(t *testing.T) {
				store := &fakeSatelliteStore{hazards: map[string]domain.HazardEvent{}, failOffset: crashAt}
				w := newTestSatelliteWorker(store, interval)
				if err := w.runOnce(context.Background(), nil); err == nil || errors.Is(err, errFeedDrained) {
					t.Fatalf("first run should fail at offset %d, got %v", crashAt, err)
				}
				if store.cp != nil && store.cp.LastOffset >= crashAt {
					t.Fatalf("checkpoint %d moved past the failed offset %d", store.cp.LastOffset, crashAt)
				}
				if err := w.runOnce(context.Background(), nil); !errors.Is(err, errFeedDrained) {
					t.Fatalf("recovery run = %v", err)
				}
				if got := summarize(store.hazards); !reflect.DeepEqual(got, want) {
					t.Fatalf("after crash at %d:\n got %v\nwant %v", crashAt, got, want)
				}
				if store.cp.LastOffset != 8 {
					t.Fatalf("final checkpoint = %d", store.cp.LastOffset)
				}
				if st := w.Status(); st.ActiveFires != 1 {
					t.Fatalf("active fires after recovery = %d, want 1", st.ActiveFires)
				}
			})
		}
	}
}

// blockingFeed replays the script, then holds the stream open until the caller stops it.
type blockingFeed struct {
	events []satellite.Event
	mu     sync.Mutex
	froms  []int64
}

func (f *blockingFeed) GetSession(context.Context) (satellite.Session, error) {
	return satellite.Session{LatestOffset: int64(len(f.events))}, nil
}

func (f *blockingFeed) Run(ctx context.Context, lastApplied int64, handle func(satellite.Event) error) error {
	f.mu.Lock()
	f.froms = append(f.froms, lastApplied)
	f.mu.Unlock()
	for _, ev := range f.events {
		if ev.Offset <= lastApplied {
			continue
		}
		if err := handle(ev); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *blockingFeed) runs() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.froms...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSatelliteWorkerStreamsOnlyOnLeaseHolderAndFailsOver(t *testing.T) {
	primary := repository.NewMemoryUnitBackend()
	replica := repository.NewMemoryUnitBackend()
	repo, err := repository.NewShardedHazardRepository([]repository.ShardGroup{{
		ShardID: "shard-1",
		Units: []repository.StorageUnit{
			{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: primary},
			{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: replica},
		},
	}})
	if err != nil {
		t.Fatalf("NewShardedHazardRepository: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	feedA := &blockingFeed{events: satelliteScript()}
	feedB := &blockingFeed{events: satelliteScript()}
	workerA := NewSatelliteWorker(feedA, repo, satellite.Config{}, nil, logger).
		WithLeaseConfig("agg-a", time.Second, 20*time.Millisecond)
	workerB := NewSatelliteWorker(feedB, repo, satellite.Config{}, nil, logger).
		WithLeaseConfig("agg-b", time.Second, 20*time.Millisecond)
	workerA.checkpointInterval, workerB.checkpointInterval = 0, 0

	ctxA, stopA := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	go func() { workerA.Start(ctxA); close(doneA) }()
	waitFor(t, "agg-a to stream", func() bool { return len(feedA.runs()) == 1 })

	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	doneB := make(chan struct{})
	go func() { workerB.Start(ctxB); close(doneB) }()

	waitFor(t, "agg-a to checkpoint offset 8", func() bool {
		cp, err := repo.LoadSatelliteCheckpoint(context.Background(), satelliteStream)
		return err == nil && cp != nil && cp.LastOffset == 8
	})
	time.Sleep(100 * time.Millisecond) // several renew intervals for agg-b to try the lease
	if runs := feedB.runs(); len(runs) != 0 {
		t.Fatalf("standby streamed while agg-a held the lease: %v", runs)
	}
	cp, _ := repo.LoadSatelliteCheckpoint(context.Background(), satelliteStream)
	if cp.HolderID != "agg-a" || cp.FencingToken <= 0 {
		t.Fatalf("checkpoint not fenced by agg-a: holder=%q token=%d", cp.HolderID, cp.FencingToken)
	}
	// agg-a shuts down and releases the lease; agg-b resumes from the checkpoint.
	stopA()
	<-doneA
	waitFor(t, "agg-b to take over", func() bool { return len(feedB.runs()) == 1 })
	if from := feedB.runs()[0]; from != 8 {
		t.Fatalf("agg-b resumed from offset %d, want 8", from)
	}

	stopB()
	<-doneB
}
