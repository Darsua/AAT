package main

import (
	"testing"
	"time"
)

func TestDecodeEvent(t *testing.T) {
	payload := []byte(`{
		"hazard_id":"haz-001",
		"source":"BMKG",
		"source_ref_id":"bmkg-001",
		"hazard_type":"SEISMIC",
		"severity":"SIAGA",
		"area_name":"Bantul",
		"latitude":-7.54,
		"longitude":110.44,
		"attributes":{"magnitude":6.2,"confidence_level":0.95},
		"occurred_at":"2026-09-05T15:30:00Z",
		"ingested_at":"2026-09-05T15:35:00Z"
	}`)

	event, err := decodeEvent(payload)
	if err != nil {
		t.Fatalf("decodeEvent() error = %v", err)
	}
	if event.HazardID != "haz-001" || event.HazardType != "SEISMIC" {
		t.Fatalf("decodeEvent() returned unexpected event: %+v", event)
	}
	if event.Attributes["magnitude"] != 6.2 {
		t.Fatalf("magnitude = %v, want 6.2", event.Attributes["magnitude"])
	}
	if event.OccurredAt != time.Date(2026, 9, 5, 15, 30, 0, 0, time.UTC) {
		t.Fatalf("occurred_at = %v", event.OccurredAt)
	}
}

func TestDecodeEventRejectsMissingIdentity(t *testing.T) {
	_, err := decodeEvent([]byte(`{"hazard_type":"SEISMIC","severity":"SIAGA"}`))
	if err == nil {
		t.Fatal("decodeEvent() expected an error for missing hazard_id")
	}
}

func TestDashboardCountsNewEvents(t *testing.T) {
	dashboard := NewDashboard()

	first := dashboard.Process(HazardEvent{
		HazardID:   "haz-001",
		HazardType: "SEISMIC",
		Severity:   "SIAGA",
	})
	second := dashboard.Process(HazardEvent{
		HazardID:   "haz-002",
		HazardType: "SEISMIC",
		Severity:   "SIAGA",
	})

	if first.Duplicate || first.TypeCount != 1 || first.SeverityCount != 1 {
		t.Fatalf("first result = %+v", first)
	}
	if second.Duplicate || second.TypeCount != 2 || second.SeverityCount != 2 {
		t.Fatalf("second result = %+v", second)
	}
}

func TestDashboardSkipsDuplicateHazard(t *testing.T) {
	dashboard := NewDashboard()
	event := HazardEvent{
		HazardID:   "haz-001",
		HazardType: "VOLCANIC",
		Severity:   "AWAS",
	}

	dashboard.Process(event)
	duplicate := dashboard.Process(event)

	if !duplicate.Duplicate {
		t.Fatal("second processing should be marked as duplicate")
	}

	next := dashboard.Process(HazardEvent{
		HazardID:   "haz-002",
		HazardType: "VOLCANIC",
		Severity:   "AWAS",
	})
	if next.TypeCount != 2 || next.SeverityCount != 2 {
		t.Fatalf("duplicate changed counters, next result = %+v", next)
	}
}
