package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCoastalAdapter_FetchStationsAndObservationsIsolatesInvalid(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/stations":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"items": [
					{"station_id":"stn-padang-01","name":"Padang","province":"Sumatera Barat","latitude":-0.9492,"longitude":100.3543}
				]
			}`))
		case "/v1/observations":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"generated_at": "2026-11-12T02:43:05Z",
				"items": [
					{"observation_id":"obs-1","station_id":"stn-padang-01","observed_at":"2026-11-12T02:43:00Z","sea_level_anomaly_m":0.42,"wave_height_m":1.8,"quality":"ok","critical":true},
					{"observation_id":"obs-bad-neg","station_id":"stn-padang-01","observed_at":"2026-11-12T02:43:01Z","sea_level_anomaly_m":0.1,"wave_height_m":-2.0,"quality":"ok","critical":false},
					{"observation_id":"obs-unknown-stn","station_id":"stn-missing","observed_at":"2026-11-12T02:43:02Z","sea_level_anomaly_m":0.1,"wave_height_m":1.0,"quality":"ok","critical":false},
					{"observation_id":"obs-missing-field","station_id":"stn-padang-01","observed_at":"2026-11-12T02:43:03Z","sea_level_anomaly_m":0.1,"quality":"ok","critical":false}
				],
				"next_cursor": "cursor-2"
			}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	adp := NewCoastalAdapter(srv.URL, "test-token", time.Second)
	stations, err := adp.FetchStations(context.Background())
	if err != nil {
		t.Fatalf("FetchStations failed: %v", err)
	}
	if len(stations) != 1 {
		t.Fatalf("expected 1 station, got %d", len(stations))
	}

	now := time.Date(2026, 11, 12, 2, 43, 10, 0, time.UTC)
	batch, err := adp.FetchObservations(context.Background(), 50, "", stations, now)
	if err != nil {
		t.Fatalf("FetchObservations failed: %v", err)
	}
	if len(batch.Valid) != 1 || batch.Valid[0].ObservationID != "obs-1" {
		t.Fatalf("expected 1 valid observation obs-1, got %+v", batch.Valid)
	}
	if len(batch.Rejected) != 3 {
		t.Fatalf("expected 3 quarantined observations, got %d", len(batch.Rejected))
	}
	if batch.NextCursor != "cursor-2" {
		t.Fatalf("expected next_cursor cursor-2, got %q", batch.NextCursor)
	}
}

func TestCoastalAdapter_QuarantinesNullFieldsAndOversizedObservationID(t *testing.T) {
	now := time.Date(2026, 11, 12, 2, 43, 10, 0, time.UTC)
	stations := map[string]CoastalStation{
		"stn-padang-01": {StationID: "stn-padang-01", Name: "Padang", Province: "Sumatera Barat", Latitude: -0.95, Longitude: 100.35},
	}

	longID := "obs-" + string(make([]byte, 120))
	for i := range []byte(longID) {
		if i >= 4 {
			b := []byte(longID)
			b[i] = 'a'
			longID = string(b)
		}
	}
	nullRaw := []byte(`{"observation_id":"obs-null","station_id":"stn-padang-01","observed_at":"2026-11-12T02:43:00Z","sea_level_anomaly_m":null,"wave_height_m":1.2,"quality":"ok","critical":true}`)
	if _, reason := parseAndValidateObservation(nullRaw, stations, now); reason == "" {
		t.Fatal("expected JSON null sea_level_anomaly_m to be rejected")
	}

	oversizedRaw := []byte(`{"observation_id":"` + longID + `","station_id":"stn-padang-01","observed_at":"2026-11-12T02:43:00Z","sea_level_anomaly_m":0.2,"wave_height_m":1.2,"quality":"ok","critical":false}`)
	if _, reason := parseAndValidateObservation(oversizedRaw, stations, now); reason == "" {
		t.Fatal("expected oversized observation_id (>117 chars) to be rejected")
	}
}
