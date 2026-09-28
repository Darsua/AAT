package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBMKGSeedAndEndpoints(t *testing.T) {
	data, err := loadSeedData()
	if err != nil {
		t.Fatalf("loadSeedData() error = %v", err)
	}
	if len(data.SeismicEvents) < 20 {
		t.Fatalf("seed event count = %d, want at least 20", len(data.SeismicEvents))
	}

	svc := newService(data)
	svc.delay = func() {}
	handler := svc.routes("test-bmkg-key")

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", recorder.Code, http.StatusOK)
	}

	request = httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}

	request = httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	request.Header.Set("X-BMKG-Key", "test-bmkg-key")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("seismic status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var events []SeismicEvent
	if err := json.NewDecoder(recorder.Body).Decode(&events); err != nil {
		t.Fatalf("decode seismic response: %v", err)
	}
	if len(events) != len(data.SeismicEvents) {
		t.Fatalf("seismic event count = %d, want %d", len(events), len(data.SeismicEvents))
	}

	since := data.SeismicEvents[10].OccurredAt
	request = httptest.NewRequest(http.MethodGet, "/seismic-events?since="+since.Format(time.RFC3339), nil)
	request.Header.Set("X-BMKG-Key", "test-bmkg-key")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("filtered seismic status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if err := json.NewDecoder(recorder.Body).Decode(&events); err != nil {
		t.Fatalf("decode filtered seismic response: %v", err)
	}
	if len(events) == 0 || events[0].OccurredAt.Before(since) {
		t.Fatalf("filtered events should start at or after %s", since.Format(time.RFC3339))
	}

	request = httptest.NewRequest(http.MethodGet, "/tsunami-warnings", nil)
	request.Header.Set("X-BMKG-Key", "test-bmkg-key")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("tsunami status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var warnings []TsunamiWarning
	if err := json.NewDecoder(recorder.Body).Decode(&warnings); err != nil {
		t.Fatalf("decode tsunami response: %v", err)
	}
	if len(warnings) == 0 {
		t.Fatal("expected seeded tsunami warnings")
	}
}

func TestBMKGEventGeneration(t *testing.T) {
	data, err := loadSeedData()
	if err != nil {
		t.Fatalf("loadSeedData() error = %v", err)
	}

	svc := newService(data)
	svc.now = func() time.Time { return time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC) }
	before := len(svc.seismicEvents)
	svc.generateEvent()

	if len(svc.seismicEvents) != before+1 {
		t.Fatalf("event count = %d, want %d", len(svc.seismicEvents), before+1)
	}
	generated := svc.seismicEvents[len(svc.seismicEvents)-1]
	if generated.EventID == "" || generated.OccurredAt.IsZero() {
		t.Fatalf("generated event is incomplete: %+v", generated)
	}
}
