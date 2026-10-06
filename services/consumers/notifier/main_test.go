package main

import (
	"testing"
)

func TestNotifierAlertRules(t *testing.T) {
	tests := []struct {
		name  string
		event HazardEvent
		alert bool
	}{
		{
			name: "major earthquake",
			event: HazardEvent{
				HazardID:   "seismic-major",
				HazardType: "SEISMIC",
				Severity:   "SIAGA",
				Attributes: map[string]any{"magnitude": 6.2},
			},
			alert: true,
		},
		{
			name: "minor earthquake",
			event: HazardEvent{
				HazardID:   "seismic-minor",
				HazardType: "SEISMIC",
				Severity:   "WASPADA",
				Attributes: map[string]any{"magnitude": 5.9},
			},
			alert: false,
		},
		{
			name: "tsunami siaga",
			event: HazardEvent{
				HazardID:   "tsunami-siaga",
				HazardType: "SEISMIC",
				Severity:   "SIAGA",
				Attributes: map[string]any{
					"tsunami_warning": map[string]any{"threat_level": "Siaga"},
				},
			},
			alert: true,
		},
		{
			name: "volcano awas",
			event: HazardEvent{
				HazardID:   "volcano-awas",
				HazardType: "VOLCANIC",
				Severity:   "AWAS",
			},
			alert: true,
		},
		{
			name: "volcano siaga",
			event: HazardEvent{
				HazardID:   "volcano-siaga",
				HazardType: "VOLCANIC",
				Severity:   "SIAGA",
			},
			alert: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := NewNotifier().Process(test.event)
			if result.Alert != test.alert {
				t.Fatalf("alert = %v, want %v; reason=%q", result.Alert, test.alert, result.Reason)
			}
		})
	}
}

func TestNotifierCombinesAlertReasons(t *testing.T) {
	result := NewNotifier().Process(HazardEvent{
		HazardID:   "seismic-tsunami",
		HazardType: "SEISMIC",
		Severity:   "AWAS",
		Attributes: map[string]any{
			"magnitude":       7.1,
			"tsunami_warning": map[string]any{"threat_level": "Awas"},
		},
	})

	if !result.Alert {
		t.Fatal("expected an alert")
	}
	if result.Reason != "earthquake magnitude 7.1; tsunami warning AWAS" {
		t.Fatalf("reason = %q", result.Reason)
	}
}

func TestNotifierSkipsDuplicateHazard(t *testing.T) {
	notifier := NewNotifier()
	event := HazardEvent{
		HazardID:   "haz-001",
		HazardType: "VOLCANIC",
		Severity:   "AWAS",
	}

	first := notifier.Process(event)
	second := notifier.Process(event)

	if !first.Alert || first.Duplicate {
		t.Fatalf("first result = %+v", first)
	}
	if !second.Duplicate || second.Alert {
		t.Fatalf("second result = %+v", second)
	}
}

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
		"attributes":{"magnitude":6.2},
		"occurred_at":"2026-09-05T15:30:00Z",
		"ingested_at":"2026-09-05T15:35:00Z"
	}`)

	event, err := decodeEvent(payload)
	if err != nil {
		t.Fatalf("decodeEvent() error = %v", err)
	}
	if event.HazardID != "haz-001" || event.Attributes["magnitude"] != 6.2 {
		t.Fatalf("decodeEvent() returned unexpected event: %+v", event)
	}
}
