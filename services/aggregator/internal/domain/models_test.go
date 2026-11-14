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

func TestValidationHelpers_M2Enums(t *testing.T) {
	for _, s := range []Source{SourceBMKG, SourcePVMBG, SourceSatelit, SourceSensorPantai} {
		if !IsValidSource(s) {
			t.Errorf("expected source %q to be valid", s)
		}
	}
	if IsValidSource(Source("FIELD_REPORT")) || IsValidSource(Source("")) {
		t.Error("expected unknown/empty source to be invalid")
	}

	for _, ht := range []HazardType{HazardTypeSeismic, HazardTypeVolcanic, HazardTypeWildfire, HazardTypeCoastal} {
		if !IsValidHazardType(ht) {
			t.Errorf("expected hazard type %q to be valid", ht)
		}
	}
	if IsValidHazardType(HazardType("TSUNAMI")) || IsValidHazardType(HazardType("")) {
		t.Error("expected unknown/empty hazard type to be invalid")
	}

	for _, sev := range []Severity{SeverityNormal, SeverityWaspada, SeveritySiaga, SeverityAwas} {
		if !IsValidSeverity(sev) {
			t.Errorf("expected severity %q to be valid", sev)
		}
	}
	if IsValidSeverity(Severity("CRITICAL")) {
		t.Error("expected unknown severity to be invalid")
	}
}

func TestUnprocessedMessage_JSONExcludesRawPayload(t *testing.T) {
	now := time.Date(2026, 11, 12, 2, 40, 11, 0, time.UTC)
	msg := UnprocessedMessage{
		MessageID:  "msg-31c9",
		Source:     SourcePVMBG,
		ReceivedAt: now,
		Reason:     "schema_incompatible",
		Retryable:  false,
		Attempts:   3,
		Status:     UnprocessedStatusParked,
		RawPayload: []byte(`{"broken":true}`),
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal UnprocessedMessage: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("failed to unmarshal UnprocessedMessage: %v", err)
	}
	if _, exists := decoded["RawPayload"]; exists {
		t.Error("RawPayload must not be exposed in JSON")
	}
	if _, exists := decoded["raw_payload"]; exists {
		t.Error("raw_payload must not be exposed in JSON")
	}
	if decoded["status"] != "parked" {
		t.Errorf("expected status parked, got %v", decoded["status"])
	}
}

func TestHazardEvent_JSONExcludesFencingToken(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	event := HazardEvent{
		HazardID:     "haz-bmkg-fence-1",
		Source:       SourceBMKG,
		SourceRefID:  "ref-fence-1",
		HazardType:   HazardTypeSeismic,
		Severity:     SeverityNormal,
		AreaName:     "Bantul",
		Attributes:   map[string]any{},
		OccurredAt:   now,
		IngestedAt:   now,
		FencingToken: 42,
	}

	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal HazardEvent: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("failed to unmarshal HazardEvent: %v", err)
	}
	if _, exists := decoded["fencing_token"]; exists {
		t.Error("fencing_token must not be exposed in HazardEvent JSON wire format")
	}
	if _, exists := decoded["FencingToken"]; exists {
		t.Error("FencingToken must not be exposed in HazardEvent JSON wire format")
	}
}
