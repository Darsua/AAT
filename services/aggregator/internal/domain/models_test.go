package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHazardEvent_JSONSerialization(t *testing.T) {
	occurredAt, _ := time.Parse(time.RFC3339, "2026-09-22T10:00:00Z")
	ingestedAt, _ := time.Parse(time.RFC3339, "2026-09-22T10:05:00Z")
	lat := -7.5407
	lon := 110.4456

	event := HazardEvent{
		HazardID:    "haz-pvmbg-20260922-001",
		Source:      SourcePVMBG,
		SourceRefID: "pvmbg-rep-101",
		HazardType:  HazardTypeVolcanic,
		Severity:    SeveritySiaga,
		AreaName:    "Gunung Merapi",
		Latitude:    &lat,
		Longitude:   &lon,
		Attributes: map[string]any{
			"volcano_id":          "VLC-001",
			"confidence_level":    0.95,
			"ash_column_height_m": 1200.0,
			"extra_custom_field":  "dynamic_value",
		},
		OccurredAt: occurredAt,
		IngestedAt: ingestedAt,
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal HazardEvent: %v", err)
	}

	var rawMap map[string]any
	if err := json.Unmarshal(data, &rawMap); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	// Verify required fields are present
	requiredFields := []string{
		"hazard_id", "source", "source_ref_id", "hazard_type",
		"severity", "area_name", "occurred_at", "ingested_at", "attributes",
	}
	for _, field := range requiredFields {
		if _, ok := rawMap[field]; !ok {
			t.Errorf("missing required field %q in serialized JSON", field)
		}
	}

	// Verify attributes dynamic payload
	attrs, ok := rawMap["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("expected attributes to be map[string]any, got %T", rawMap["attributes"])
	}

	if attrs["confidence_level"] != 0.95 {
		t.Errorf("expected confidence_level 0.95, got %v", attrs["confidence_level"])
	}
	if attrs["extra_custom_field"] != "dynamic_value" {
		t.Errorf("expected dynamic field preserved, got %v", attrs["extra_custom_field"])
	}
}
