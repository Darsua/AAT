package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDashboardUpdateAwareDeduplication(t *testing.T) {
	d := NewDashboard()
	t0 := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(2 * time.Minute)
	tOlder := t0.Add(-1 * time.Minute)

	first := d.Process(HazardEvent{
		HazardID:   "haz-upd-1",
		HazardType: "SEISMIC",
		Severity:   "WASPADA",
		AreaName:   "Bantul",
		OccurredAt: t0,
		IngestedAt: t0,
	})
	if first.Duplicate || first.Updated || first.SeverityCount != 1 {
		t.Fatalf("unexpected first result: %+v", first)
	}

	// Older update must be ignored as duplicate/stale.
	stale := d.Process(HazardEvent{
		HazardID:   "haz-upd-1",
		HazardType: "SEISMIC",
		Severity:   "AWAS",
		AreaName:   "Bantul",
		OccurredAt: t0,
		IngestedAt: tOlder,
	})
	if !stale.Duplicate || stale.Updated {
		t.Fatalf("expected stale update to be skipped, got %+v", stale)
	}

	// Newer escalation updates severity and correlated tsunami count without double-counting total.
	updated := d.Process(HazardEvent{
		HazardID:   "haz-upd-1",
		HazardType: "SEISMIC",
		Severity:   "AWAS",
		AreaName:   "Bantul",
		Attributes: map[string]any{"potential_tsunami": true},
		OccurredAt: t0,
		IngestedAt: t1,
	})
	if updated.Duplicate || !updated.Updated || updated.TypeCount != 1 || updated.SeverityCount != 1 {
		t.Fatalf("expected update in place, got %+v", updated)
	}

	snap := d.Snapshot("all", nil, t1)
	if snap.TotalHazards != 1 {
		t.Fatalf("total_hazards = %d, want 1", snap.TotalHazards)
	}
	if snap.BySeverity["WASPADA"] != 0 || snap.BySeverity["AWAS"] != 1 {
		t.Fatalf("unexpected severity distribution after update: %+v", snap.BySeverity)
	}
	if snap.CorrelatedTsunamiCount != 1 {
		t.Fatalf("correlated_tsunami_count = %d, want 1", snap.CorrelatedTsunamiCount)
	}
}

func TestDashboardAnalyticsTimeRangeAreasAndTsunamiSubset(t *testing.T) {
	d := NewDashboard()
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

	events := []HazardEvent{
		{
			HazardID:   "haz-old",
			HazardType: "VOLCANIC",
			Severity:   "NORMAL",
			AreaName:   "Merapi",
			OccurredAt: now.Add(-3 * time.Hour),
			IngestedAt: now.Add(-3 * time.Hour),
		},
		{
			HazardID:   "haz-seis-1",
			HazardType: "SEISMIC",
			Severity:   "AWAS",
			AreaName:   "Aceh",
			Attributes: map[string]any{"tsunami_warning": map[string]any{"threat_level": "Awas"}},
			OccurredAt: now.Add(-30 * time.Minute),
			IngestedAt: now.Add(-29 * time.Minute),
		},
		{
			HazardID:   "haz-wild-1",
			HazardType: "WILDFIRE",
			Severity:   "SIAGA",
			AreaName:   "Riau",
			OccurredAt: now.Add(-20 * time.Minute),
			IngestedAt: now.Add(-19 * time.Minute),
		},
		{
			HazardID:   "haz-coast-1",
			HazardType: "COASTAL",
			Severity:   "WASPADA",
			AreaName:   "Aceh",
			OccurredAt: now.Add(-10 * time.Minute),
			IngestedAt: now.Add(-9 * time.Minute),
		},
	}
	for _, ev := range events {
		d.Process(ev)
	}

	snap1h := d.Snapshot("1h", nil, now)
	if snap1h.TotalHazards != 3 {
		t.Fatalf("1h total_hazards = %d, want 3", snap1h.TotalHazards)
	}
	if snap1h.ByType["SEISMIC"] != 1 || snap1h.ByType["WILDFIRE"] != 1 || snap1h.ByType["COASTAL"] != 1 || snap1h.ByType["VOLCANIC"] != 0 {
		t.Fatalf("unexpected 1h by_type: %+v", snap1h.ByType)
	}
	if snap1h.CorrelatedTsunamiCount != 1 {
		t.Fatalf("correlated_tsunami_count = %d, want 1", snap1h.CorrelatedTsunamiCount)
	}
	if len(snap1h.ByArea) != 2 || snap1h.ByArea[0].AreaName != "Aceh" || snap1h.ByArea[0].Count != 2 {
		t.Fatalf("unexpected by_area: %+v", snap1h.ByArea)
	}
	if len(snap1h.Timeseries) == 0 || snap1h.Timeseries[len(snap1h.Timeseries)-1].Total != 3 {
		t.Fatalf("unexpected 1h timeseries: %+v", snap1h.Timeseries)
	}

	snapAll := d.Snapshot("all", nil, now)
	if snapAll.TotalHazards != 4 || snapAll.BySeverity["NORMAL"] != 1 || snapAll.BySeverity["WASPADA"] != 1 || snapAll.BySeverity["SIAGA"] != 1 || snapAll.BySeverity["AWAS"] != 1 {
		t.Fatalf("unexpected all-time snapshot: %+v", snapAll)
	}
}

func TestDashboardSyncPartialOutageAndRecovery(t *testing.T) {
	var mode atomic.Int32 // 0=healthy multi-page, 1=partial shard, 2=outage 503, 3=recovered with retraction
	now := time.Now().UTC()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 0:
			offset := r.URL.Query().Get("offset")
			w.Header().Set("Content-Type", "application/json")
			if offset == "0" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": []HazardEvent{{
						HazardID: "haz-1", Source: "BMKG", SourceRefID: "b1",
						HazardType: "SEISMIC", Severity: "SIAGA", AreaName: "Bantul",
						OccurredAt: now.Add(-15 * time.Minute), IngestedAt: now.Add(-14 * time.Minute),
					}},
					"meta": map[string]any{"total": 2, "degraded": false, "stale_since": nil},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []HazardEvent{{
					HazardID: "haz-2", Source: "PVMBG", SourceRefID: "p1",
					HazardType: "VOLCANIC", Severity: "AWAS", AreaName: "Merapi",
					OccurredAt: now.Add(-10 * time.Minute), IngestedAt: now.Add(-9 * time.Minute),
				}},
				"meta": map[string]any{"total": 2, "degraded": false, "stale_since": nil},
			})
		case 1:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Storage-Partial", "true")
			w.Header().Set("X-Unavailable-Units", "shard-1-primary")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []HazardEvent{{
					HazardID: "haz-1", Source: "BMKG", SourceRefID: "b1",
					HazardType: "SEISMIC", Severity: "SIAGA", AreaName: "Bantul",
					OccurredAt: now.Add(-15 * time.Minute), IngestedAt: now.Add(-14 * time.Minute),
				}},
				"meta": map[string]any{"total": 1, "degraded": true, "stale_since": now.Add(-2 * time.Minute)},
			})
		case 2:
			http.Error(w, "aggregator down", http.StatusServiceUnavailable)
		case 3:
			// haz-2 retracted, haz-1 remains
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []HazardEvent{{
					HazardID: "haz-1", Source: "BMKG", SourceRefID: "b1",
					HazardType: "SEISMIC", Severity: "SIAGA", AreaName: "Bantul",
					OccurredAt: now.Add(-15 * time.Minute), IngestedAt: now.Add(-14 * time.Minute),
				}},
				"meta": map[string]any{"total": 1, "degraded": false, "stale_since": nil},
			})
		}
	}))
	defer srv.Close()

	d := NewDashboard()
	httpHandler := newHTTPHandler(d)

	// 1. Healthy sync across both pages (meta.total=2 with 1 item per page).
	if err := syncFromAggregator(context.Background(), srv.URL, srv.Client(), d); err != nil {
		t.Fatalf("healthy sync failed: %v", err)
	}
	snap := d.Snapshot("all", nil, now)
	if snap.TotalHazards != 2 || snap.Status.PartialData {
		t.Fatalf("expected 2 hazards and healthy status, got %+v", snap)
	}

	// 2. Partial shard header triggers partial_data banner while preserving known hazards.
	mode.Store(1)
	if err := syncFromAggregator(context.Background(), srv.URL, srv.Client(), d); err != nil {
		t.Fatalf("partial sync failed: %v", err)
	}
	snap = d.Snapshot("all", nil, now)
	if !snap.Status.PartialData || !snap.Status.Degraded || len(snap.Status.UnavailableUnits) != 1 || snap.Status.UnavailableUnits[0] != "shard-1-primary" {
		t.Fatalf("expected partial_data with unavailable unit, got %+v", snap.Status)
	}
	if snap.TotalHazards != 2 {
		t.Fatalf("expected cached haz-2 preserved during partial outage, got %d", snap.TotalHazards)
	}

	// 3. Complete outage keeps UI and cached data available with partial_data=true.
	mode.Store(2)
	if err := syncFromAggregator(context.Background(), srv.URL, srv.Client(), d); err == nil {
		t.Fatal("expected error during 503 outage")
	}
	recJSON := httptest.NewRecorder()
	httpHandler.ServeHTTP(recJSON, httptest.NewRequest(http.MethodGet, "/api/analytics?window=all", nil))
	if recJSON.Code != http.StatusOK {
		t.Fatalf("expected 200 on /api/analytics during outage, got %d", recJSON.Code)
	}
	var outSnap AnalyticsSnapshot
	if err := json.NewDecoder(recJSON.Body).Decode(&outSnap); err != nil {
		t.Fatalf("decode /api/analytics: %v", err)
	}
	if !outSnap.Status.PartialData || outSnap.TotalHazards != 2 {
		t.Fatalf("expected partial_data=true and cached totals=2, got %+v", outSnap)
	}

	// 4. Recovery clears partial_data and reconciles retraction (haz-2 removed -> total=1).
	mode.Store(3)
	if err := syncFromAggregator(context.Background(), srv.URL, srv.Client(), d); err != nil {
		t.Fatalf("recovery sync failed: %v", err)
	}
	snap = d.Snapshot("all", nil, now)
	if snap.Status.PartialData || snap.Status.Degraded || snap.TotalHazards != 1 {
		t.Fatalf("expected recovered complete view with 1 hazard, got %+v", snap)
	}

	// 5. Verify browser HTML UI serves on / and /dashboard.
	for _, path := range []string{"/", "/dashboard"} {
		recHTML := httptest.NewRecorder()
		httpHandler.ServeHTTP(recHTML, httptest.NewRequest(http.MethodGet, path, nil))
		if recHTML.Code != http.StatusOK || !strings.Contains(recHTML.Body.String(), "BNPB Multi-Hazard Analytics Dashboard") {
			t.Fatalf("%s did not return dashboard HTML: code=%d", path, recHTML.Code)
		}
	}
}

func TestDashboardSourceRefChecksumAndFilters(t *testing.T) {
	d := NewDashboard()
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	d.Process(HazardEvent{
		HazardID:    "haz-1",
		Source:      "BMKG",
		SourceRefID: "bmkg-002",
		HazardType:  "SEISMIC",
		Severity:    "SIAGA",
		AreaName:    "Palu",
		OccurredAt:  now,
		IngestedAt:  now,
	})
	d.Process(HazardEvent{
		HazardID:    "haz-2",
		Source:      "PVMBG",
		SourceRefID: "pvmbg-001",
		HazardType:  "VOLCANIC",
		Severity:    "AWAS",
		AreaName:    "Sinabung",
		OccurredAt:  now,
		IngestedAt:  now,
	})

	// Over all hazards: source_ref_ids sorted: "bmkg-002\npvmbg-001"
	snapAll := d.SnapshotWithOptions(SnapshotOptions{Window: "all"}, now)
	if snapAll.TotalHazards != 2 {
		t.Fatalf("expected 2 hazards, got %d", snapAll.TotalHazards)
	}
	expectedChecksum := computeSourceRefChecksum([]string{"bmkg-002", "pvmbg-001"})
	if snapAll.SourceRefChecksum != expectedChecksum {
		t.Fatalf("checksum mismatch: got %q, want %q", snapAll.SourceRefChecksum, expectedChecksum)
	}

	// Filter by source: BMKG
	snapBMKG := d.SnapshotWithOptions(SnapshotOptions{Window: "all", SourceFilter: "BMKG"}, now)
	if snapBMKG.TotalHazards != 1 || snapBMKG.ByType["SEISMIC"] != 1 {
		t.Fatalf("unexpected BMKG filtered snapshot: %+v", snapBMKG)
	}
	expectedBMKGChecksum := computeSourceRefChecksum([]string{"bmkg-002"})
	if snapBMKG.SourceRefChecksum != expectedBMKGChecksum {
		t.Fatalf("BMKG checksum mismatch: got %q, want %q", snapBMKG.SourceRefChecksum, expectedBMKGChecksum)
	}

	// Filter by type: VOLCANIC
	snapVol := d.SnapshotWithOptions(SnapshotOptions{Window: "all", TypeFilter: "VOLCANIC"}, now)
	if snapVol.TotalHazards != 1 || snapVol.ByType["VOLCANIC"] != 1 {
		t.Fatalf("unexpected VOLCANIC filtered snapshot: %+v", snapVol)
	}
}

func TestDashboardRetractionSupport(t *testing.T) {
	d := NewDashboard()
	now := time.Date(2026, 11, 18, 14, 0, 0, 0, time.UTC)

	d.Process(HazardEvent{
		HazardID:    "haz-retract-1",
		Source:      "BMKG",
		SourceRefID: "bmkg-retract-1",
		HazardType:  "SEISMIC",
		Severity:    "AWAS",
		AreaName:    "Lombok",
		OccurredAt:  now,
		IngestedAt:  now,
	})

	snap := d.Snapshot("all", nil, now)
	if snap.TotalHazards != 1 || snap.BySeverity["AWAS"] != 1 {
		t.Fatalf("expected 1 initial hazard, got %+v", snap)
	}

	// Retract via Retract method
	if !d.Retract("haz-retract-1") {
		t.Fatalf("expected Retract to return true")
	}
	snapAfter := d.Snapshot("all", nil, now)
	if snapAfter.TotalHazards != 0 || snapAfter.BySeverity["AWAS"] != 0 {
		t.Fatalf("expected 0 hazards after retraction, got %+v", snapAfter)
	}

	// Retract via Process with retracted attribute
	d.Process(HazardEvent{
		HazardID:    "haz-retract-2",
		Source:      "PVMBG",
		SourceRefID: "pvmbg-retract-2",
		HazardType:  "VOLCANIC",
		Severity:    "SIAGA",
		AreaName:    "Merapi",
		OccurredAt:  now,
		IngestedAt:  now,
	})
	res := d.Process(HazardEvent{
		HazardID:   "haz-retract-2",
		Attributes: map[string]any{"retracted": true},
	})
	if !res.Retracted {
		t.Fatalf("expected result.Retracted to be true, got %+v", res)
	}
	snapFinal := d.Snapshot("all", nil, now)
	if snapFinal.TotalHazards != 0 {
		t.Fatalf("expected 0 hazards, got %d", snapFinal.TotalHazards)
	}
}

func TestAuditSyncCursorDeltaChecksumAndRetractionRecovery(t *testing.T) {
	t0 := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)

	var mu sync.Mutex
	events := []HazardEvent{
		{
			HazardID:    "haz-a-1",
			Source:      "BMKG",
			SourceRefID: "ref-a-1",
			HazardType:  "SEISMIC",
			Severity:    "WASPADA",
			AreaName:    "Bantul",
			OccurredAt:  t0,
			IngestedAt:  t0,
		},
		{
			HazardID:    "haz-a-2",
			Source:      "PVMBG",
			SourceRefID: "ref-a-2",
			HazardType:  "VOLCANIC",
			Severity:    "SIAGA",
			AreaName:    "Merapi",
			OccurredAt:  t0.Add(1 * time.Minute),
			IngestedAt:  t0.Add(1 * time.Minute),
		},
	}
	var fullWalkPages atomic.Int32
	var deltaCalls atomic.Int32
	var checksumCalls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/internal/hazards":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": events[:min(1, len(events))],
				"meta": map[string]any{"total": len(events), "degraded": false, "stale_since": nil},
			})
		case "/internal/audit/events":
			if r.Header.Get("Authorization") != "Bearer test-audit-secret" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			q := r.URL.Query()
			if rawSince := q.Get("since"); rawSince != "" {
				deltaCalls.Add(1)
				sinceT, err := time.Parse(time.RFC3339Nano, rawSince)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				var items []HazardEvent
				for _, ev := range events {
					if !ev.IngestedAt.Before(sinceT) {
						items = append(items, ev)
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items":             items,
					"next_cursor":       nil,
					"partial":           false,
					"unavailable_units": []string{},
				})
				return
			}

			fullWalkPages.Add(1)
			cursor := q.Get("cursor")
			switch cursor {
			case "":
				next := "page-2"
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items":             events[:1],
					"next_cursor":       &next,
					"partial":           false,
					"unavailable_units": []string{},
				})
			case "page-2":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items":             events[1:],
					"next_cursor":       nil,
					"partial":           false,
					"unavailable_units": []string{},
				})
			default:
				http.Error(w, "bad cursor", http.StatusBadRequest)
			}
		case "/internal/audit/checksum":
			if r.Header.Get("Authorization") != "Bearer test-audit-secret" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			checksumCalls.Add(1)
			var refs []string
			for _, ev := range events {
				refs = append(refs, ev.SourceRefID)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"source":            "ALL",
				"count":             len(events),
				"checksum":          computeSourceRefChecksum(refs),
				"partial":           false,
				"unavailable_units": []string{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	d := NewDashboard()

	// 1. Initial sync walks /internal/audit/events across 2 cursor pages.
	if err := syncFromAggregatorWithAudit(context.Background(), srv.URL, "test-audit-secret", 5*time.Minute, srv.Client(), d); err != nil {
		t.Fatalf("initial audit sync failed: %v", err)
	}
	if fullWalkPages.Load() != 2 {
		t.Fatalf("expected 2 full-walk pages on initial sync, got %d", fullWalkPages.Load())
	}
	if snap := d.Snapshot("all", nil, t0.Add(2*time.Minute)); snap.TotalHazards != 2 || snap.Status.PartialData {
		t.Fatalf("unexpected snapshot after initial audit sync: %+v", snap)
	}

	// 2. Append a third event; steady-state sync should ingest via ?since= + checksum without a full walk.
	mu.Lock()
	events = append(events, HazardEvent{
		HazardID:    "haz-a-3",
		Source:      "SATELIT",
		SourceRefID: "ref-a-3",
		HazardType:  "WILDFIRE",
		Severity:    "AWAS",
		AreaName:    "Riau",
		OccurredAt:  t0.Add(2 * time.Minute),
		IngestedAt:  t0.Add(2 * time.Minute),
	})
	mu.Unlock()

	if err := syncFromAggregatorWithAudit(context.Background(), srv.URL, "test-audit-secret", 5*time.Minute, srv.Client(), d); err != nil {
		t.Fatalf("incremental audit sync failed: %v", err)
	}
	if fullWalkPages.Load() != 2 {
		t.Fatalf("expected no additional full walk during incremental sync, got fullWalkPages=%d", fullWalkPages.Load())
	}
	if deltaCalls.Load() != 1 || checksumCalls.Load() != 1 {
		t.Fatalf("expected 1 delta call and 1 checksum call, got delta=%d checksum=%d", deltaCalls.Load(), checksumCalls.Load())
	}
	if snap := d.Snapshot("all", nil, t0.Add(3*time.Minute)); snap.TotalHazards != 3 || snap.ByType["WILDFIRE"] != 1 {
		t.Fatalf("unexpected snapshot after delta sync: %+v", snap)
	}

	// 3. Steady-state unchanged tick checks checksum without triggering a full walk.
	if err := syncFromAggregatorWithAudit(context.Background(), srv.URL, "test-audit-secret", 5*time.Minute, srv.Client(), d); err != nil {
		t.Fatalf("steady-state audit sync failed: %v", err)
	}
	if fullWalkPages.Load() != 2 {
		t.Fatalf("expected fullWalkPages=2 when parity holds, got %d", fullWalkPages.Load())
	}

	// 4. Retract haz-a-1 on server -> localCount > cs.Count triggers full cursor reconciliation.
	mu.Lock()
	events = events[1:]
	mu.Unlock()

	if err := syncFromAggregatorWithAudit(context.Background(), srv.URL, "test-audit-secret", 5*time.Minute, srv.Client(), d); err != nil {
		t.Fatalf("retraction audit sync failed: %v", err)
	}
	if fullWalkPages.Load() != 4 {
		t.Fatalf("expected full cursor reconciliation after retraction (fullWalkPages=4), got %d", fullWalkPages.Load())
	}
	if snap := d.Snapshot("all", nil, t0.Add(4*time.Minute)); snap.TotalHazards != 2 || snap.ByType["SEISMIC"] != 0 {
		t.Fatalf("expected haz-a-1 removed after full reconciliation, got %+v", snap)
	}
}

func TestAnalyticsDefaultOmitsEventsAndOptInCapsAt500(t *testing.T) {
	d := NewDashboard()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 510; i++ {
		d.Process(HazardEvent{
			HazardID:    fmt.Sprintf("haz-cap-%03d", i),
			Source:      "SATELIT",
			SourceRefID: fmt.Sprintf("ref-cap-%03d", i),
			HazardType:  "WILDFIRE",
			Severity:    "WASPADA",
			AreaName:    "Jambi",
			OccurredAt:  now.Add(time.Duration(i) * time.Second),
			IngestedAt:  now.Add(time.Duration(i) * time.Second),
		})
	}

	h := newHTTPHandler(d)

	// Default response omits events array.
	recDefault := httptest.NewRecorder()
	h.ServeHTTP(recDefault, httptest.NewRequest(http.MethodGet, "/api/analytics?window=all", nil))
	if recDefault.Code != http.StatusOK {
		t.Fatalf("default /api/analytics status = %d", recDefault.Code)
	}
	var rawDefault map[string]any
	if err := json.Unmarshal(recDefault.Body.Bytes(), &rawDefault); err != nil {
		t.Fatalf("decode default response: %v", err)
	}
	if _, hasEvents := rawDefault["events"]; hasEvents {
		t.Fatalf("expected default /api/analytics to omit events field")
	}
	if int(rawDefault["total_hazards"].(float64)) != 510 {
		t.Fatalf("total_hazards = %v, want 510", rawDefault["total_hazards"])
	}

	// Opt-in include_events=true returns newest 500 events.
	recWithEvents := httptest.NewRecorder()
	h.ServeHTTP(recWithEvents, httptest.NewRequest(http.MethodGet, "/api/analytics?window=all&include_events=true", nil))
	if recWithEvents.Code != http.StatusOK {
		t.Fatalf("include_events /api/analytics status = %d", recWithEvents.Code)
	}
	var snapWithEvents AnalyticsSnapshot
	if err := json.Unmarshal(recWithEvents.Body.Bytes(), &snapWithEvents); err != nil {
		t.Fatalf("decode include_events response: %v", err)
	}
	if snapWithEvents.TotalHazards != 510 {
		t.Fatalf("total_hazards = %d, want 510", snapWithEvents.TotalHazards)
	}
	if len(snapWithEvents.Events) != maxAnalyticsResponseEvents {
		t.Fatalf("events len = %d, want %d", len(snapWithEvents.Events), maxAnalyticsResponseEvents)
	}
	if snapWithEvents.Events[0].HazardID != "haz-cap-509" {
		t.Fatalf("newest event = %q, want haz-cap-509", snapWithEvents.Events[0].HazardID)
	}
}

func TestDecodeEventAllowsAttributeRetractionAndEmptyChecksum(t *testing.T) {
	ev, err := decodeEvent([]byte(`{"hazard_id":"haz-retract-2","attributes":{"retracted":true}}`))
	if err != nil || ev.HazardID != "haz-retract-2" {
		t.Fatalf("expected attribute-based retraction to decode cleanly, got %+v err=%v", ev, err)
	}
	if _, err := decodeEvent([]byte(`{"hazard_id":"haz-missing-fields"}`)); err == nil {
		t.Fatal("expected non-retraction without hazard_type/severity to fail decodeEvent")
	}
	if got := computeSourceRefChecksum(nil); got != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatalf("empty checksum = %q, want sha256('')", got)
	}
}
