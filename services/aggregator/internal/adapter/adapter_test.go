package adapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBMKGAdapter_FetchSeismicEvents(t *testing.T) {
	expectedKey := "test-bmkg-key"
	expectedSince := "2026-09-30T10:00:00Z"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/seismic-events" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if key := r.Header.Get("X-BMKG-Key"); key != expectedKey {
			t.Errorf("expected header X-BMKG-Key %s, got %s", expectedKey, key)
		}
		if since := r.URL.Query().Get("since"); since != expectedSince {
			t.Errorf("expected query param since %s, got %s", expectedSince, since)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[
			{
				"event_id": "bmkg-001",
				"magnitude": 5.8,
				"depth_km": 10.0,
				"epicenter_lat": -7.5,
				"epicenter_lon": 110.4,
				"region_name": "Yogyakarta",
				"occurred_at": "2026-09-30T10:05:00Z",
				"potential_tsunami": false
			}
		]`))
	}))
	defer server.Close()

	adapter := NewBMKGAdapter(server.URL, expectedKey, 2*time.Second)
	sinceTime, _ := time.Parse(time.RFC3339, expectedSince)

	events, err := adapter.FetchSeismicEvents(context.Background(), &sinceTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventID != "bmkg-001" || events[0].Magnitude != 5.8 {
		t.Errorf("unexpected event content: %+v", events[0])
	}
}

func TestBMKGAdapter_FetchTsunamiWarnings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[
			{
				"warning_id": "tsu-001",
				"related_event_id": "bmkg-001",
				"threat_level": "Waspada",
				"affected_zones": ["Pesisir Kulonprogo"],
				"estimated_arrival": "2026-09-30T10:30:00Z"
			}
		]`))
	}))
	defer server.Close()

	adapter := NewBMKGAdapter(server.URL, "key", 2*time.Second)
	warnings, err := adapter.FetchTsunamiWarnings(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d", len(warnings))
	}
	if warnings[0].WarningID != "tsu-001" || warnings[0].ThreatLevel != "Waspada" {
		t.Errorf("unexpected warning content: %+v", warnings[0])
	}
}

func TestPVMBGAdapter_FetchVolcanicReports_DynamicSchema(t *testing.T) {
	expectedToken := "test-pvmbg-token"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "Bearer "+expectedToken {
			t.Errorf("expected Authorization header Bearer %s, got %s", expectedToken, auth)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[
			{
				"report_id": "pvmbg-001",
				"volcano_id": "VLC-001",
				"alert_level": "Siaga",
				"eruption_count_24h": 5,
				"ash_column_height_m": 750.0,
				"reported_at": "2026-09-30T10:00:00Z",
				"confidence_level": 0.95,
				"custom_telemetry_sensor": "seismograph-alpha"
			}
		]`))
	}))
	defer server.Close()

	adapter := NewPVMBGAdapter(server.URL, expectedToken, 2*time.Second)
	reports, err := adapter.FetchVolcanicReports(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(reports))
	}

	r := reports[0]
	if r.ReportID != "pvmbg-001" || r.VolcanoID != "VLC-001" || r.AlertLevel != "Siaga" {
		t.Errorf("unexpected static report fields: %+v", r)
	}

	// Verify dynamic fields
	if r.DynamicFields == nil {
		t.Fatal("expected DynamicFields map to be populated")
	}
	if conf, ok := r.DynamicFields["confidence_level"].(float64); !ok || conf != 0.95 {
		t.Errorf("expected confidence_level = 0.95 in DynamicFields, got %v", r.DynamicFields["confidence_level"])
	}
	if sensor, ok := r.DynamicFields["custom_telemetry_sensor"].(string); !ok || sensor != "seismograph-alpha" {
		t.Errorf("expected custom_telemetry_sensor in DynamicFields, got %v", r.DynamicFields["custom_telemetry_sensor"])
	}
}

func TestPVMBGAdapter_OutageSimulation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error": "SERVICE_UNAVAILABLE"}`))
	}))
	defer server.Close()

	adapter := NewPVMBGAdapter(server.URL, "token", 2*time.Second)
	_, err := adapter.FetchVolcanicReports(context.Background(), nil)
	if !errors.Is(err, ErrUpstreamUnavailable) {
		t.Fatalf("expected ErrUpstreamUnavailable on 503, got: %v", err)
	}
}
