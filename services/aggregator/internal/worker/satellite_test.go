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

func (s *fakeSatelliteStore) SaveSatelliteCheckpoint(_ context.Context, _ string, offset int64, state []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cp != nil && s.cp.LastOffset > offset {
		return nil
	}
	s.cp = &repository.SatelliteCheckpoint{LastOffset: offset, State: append([]byte(nil), state...)}
	return nil
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
	if err := w.runOnce(context.Background()); !errors.Is(err, errFeedDrained) {
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
		}
	}
	if active != 1 || retired != 1 {
		t.Fatalf("active=%d retired=%d, hazards = %v", active, retired, summarize(store.hazards))
	}
	if st := w.Status(); st.RejectedEvents != 1 || st.LastAppliedOffset != 8 || st.LagEvents != 0 {
		t.Fatalf("status = %+v", st)
	}
}

func TestSatelliteWorkerCrashRecoveryMatchesUninterruptedRun(t *testing.T) {
	reference := &fakeSatelliteStore{hazards: map[string]domain.HazardEvent{}}
	if err := newTestSatelliteWorker(reference, 0).runOnce(context.Background()); !errors.Is(err, errFeedDrained) {
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
				if err := w.runOnce(context.Background()); err == nil || errors.Is(err, errFeedDrained) {
					t.Fatalf("first run should fail at offset %d, got %v", crashAt, err)
				}
				if store.cp != nil && store.cp.LastOffset >= crashAt {
					t.Fatalf("checkpoint %d moved past the failed offset %d", store.cp.LastOffset, crashAt)
				}
				if err := w.runOnce(context.Background()); !errors.Is(err, errFeedDrained) {
					t.Fatalf("recovery run = %v", err)
				}
				if got := summarize(store.hazards); !reflect.DeepEqual(got, want) {
					t.Fatalf("after crash at %d:\n got %v\nwant %v", crashAt, got, want)
				}
				if store.cp.LastOffset != 8 {
					t.Fatalf("final checkpoint = %d", store.cp.LastOffset)
				}
			})
		}
	}
}
