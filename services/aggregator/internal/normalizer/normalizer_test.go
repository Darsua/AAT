package normalizer

import (
	"context"
	"testing"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
)

type mockVolcanoProvider struct {
	volcanoes map[string]domain.VolcanoReference
}

func (m *mockVolcanoProvider) GetVolcanoByID(_ context.Context, id string) (*domain.VolcanoReference, error) {
	if v, ok := m.volcanoes[id]; ok {
		return &v, nil
	}
	return nil, nil
}

func TestNormalizeSeismic_MagnitudeThresholds(t *testing.T) {
	norm := NewNormalizer(nil)
	occurred := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		magnitude        float64
		expectedSeverity domain.Severity
	}{
		{4.9, domain.SeverityNormal},
		{5.0, domain.SeverityWaspada},
		{5.99, domain.SeverityWaspada},
		{6.0, domain.SeveritySiaga},
		{6.99, domain.SeveritySiaga},
		{7.0, domain.SeverityAwas},
		{8.1, domain.SeverityAwas},
	}

	for _, tt := range tests {
		event := adapter.SeismicEventDTO{
			EventID:          "bmkg-test",
			Magnitude:        tt.magnitude,
			DepthKM:          15.0,
			EpicenterLat:     -6.2,
			EpicenterLon:     106.8,
			RegionName:       "Jakarta",
			OccurredAt:       occurred,
			PotentialTsunami: false,
		}

		canonical := norm.NormalizeSeismic(event, nil)

		if canonical.Severity != tt.expectedSeverity {
			t.Errorf("magnitude %.2f: expected severity %s, got %s", tt.magnitude, tt.expectedSeverity, canonical.Severity)
		}
		if canonical.HazardID != "haz-bmkg-bmkg-test" {
			t.Errorf("unexpected HazardID: %s", canonical.HazardID)
		}
		if canonical.Source != domain.SourceBMKG || canonical.HazardType != domain.HazardTypeSeismic {
			t.Errorf("unexpected Source or HazardType: %v / %v", canonical.Source, canonical.HazardType)
		}
	}
}

func TestNormalizeSeismic_TsunamiWarningOverrides(t *testing.T) {
	norm := NewNormalizer(nil)
	occurred := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	event := adapter.SeismicEventDTO{
		EventID:          "bmkg-target",
		Magnitude:        4.5, // magnitude alone would be NORMAL
		DepthKM:          10.0,
		EpicenterLat:     -7.5,
		EpicenterLon:     110.4,
		RegionName:       "Yogyakarta",
		OccurredAt:       occurred,
		PotentialTsunami: true,
	}

	tests := []struct {
		threatLevel      string
		expectedSeverity domain.Severity
	}{
		{"Awas", domain.SeverityAwas},
		{"Siaga", domain.SeveritySiaga},
		{"Waspada", domain.SeverityWaspada},
	}

	for _, tt := range tests {
		warnings := []adapter.TsunamiWarningDTO{
			{
				WarningID:        "tsu-1",
				RelatedEventID:   "bmkg-other", // Unrelated
				ThreatLevel:      "Normal",
				AffectedZones:    []string{"Other"},
				EstimatedArrival: occurred.Add(15 * time.Minute),
			},
			{
				WarningID:        "tsu-target",
				RelatedEventID:   "bmkg-target", // Matches!
				ThreatLevel:      tt.threatLevel,
				AffectedZones:    []string{"Pesisir Selatan"},
				EstimatedArrival: occurred.Add(20 * time.Minute),
			},
		}

		canonical := norm.NormalizeSeismic(event, warnings)

		if canonical.Severity != tt.expectedSeverity {
			t.Errorf("tsunami threat %s: expected severity %s, got %s", tt.threatLevel, tt.expectedSeverity, canonical.Severity)
		}

		tsuMap, ok := canonical.Attributes["tsunami_warning"].(map[string]any)
		if !ok {
			t.Fatalf("expected tsunami_warning attribute to be present")
		}
		if tsuMap["warning_id"] != "tsu-target" || tsuMap["threat_level"] != tt.threatLevel {
			t.Errorf("unexpected tsunami_warning attributes: %+v", tsuMap)
		}
	}
}

func TestNormalizeVolcanic_CoordinateEnrichmentAndDynamicFields(t *testing.T) {
	mockVols := &mockVolcanoProvider{
		volcanoes: map[string]domain.VolcanoReference{
			"VLC-001": {
				VolcanoID:   "VLC-001",
				VolcanoName: "Gunung Merapi",
				Latitude:    -7.5407,
				Longitude:   110.4456,
			},
		},
	}

	norm := NewNormalizer(mockVols)
	occurred := time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)

	report := adapter.VolcanicReportDTO{
		ReportID:         "pvmbg-rpt-001",
		VolcanoID:        "VLC-001",
		AlertLevel:       "Siaga",
		EruptionCount24h: 8,
		AshColumnHeightM: 1200.0,
		ReportedAt:       occurred,
		DynamicFields: map[string]any{
			"confidence_level": 0.95,
			"crater_glow":      true,
		},
	}

	canonical, err := norm.NormalizeVolcanic(context.Background(), report)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if canonical.HazardID != "haz-pvmbg-pvmbg-rpt-001" {
		t.Errorf("unexpected HazardID: %s", canonical.HazardID)
	}
	if canonical.AreaName != "Gunung Merapi" {
		t.Errorf("expected AreaName 'Gunung Merapi', got %s", canonical.AreaName)
	}
	if canonical.Latitude == nil || *canonical.Latitude != -7.5407 {
		t.Errorf("expected Latitude -7.5407, got %v", canonical.Latitude)
	}
	if canonical.Longitude == nil || *canonical.Longitude != 110.4456 {
		t.Errorf("expected Longitude 110.4456, got %v", canonical.Longitude)
	}
	if canonical.Severity != domain.SeveritySiaga {
		t.Errorf("expected Severity SIAGA, got %s", canonical.Severity)
	}

	// Verify dynamic attributes
	if conf, ok := canonical.Attributes["confidence_level"].(float64); !ok || conf != 0.95 {
		t.Errorf("expected confidence_level = 0.95 in Attributes, got %v", canonical.Attributes["confidence_level"])
	}
	if glow, ok := canonical.Attributes["crater_glow"].(bool); !ok || !glow {
		t.Errorf("expected crater_glow = true in Attributes, got %v", canonical.Attributes["crater_glow"])
	}
}

func TestNormalizeVolcanic_UnknownVolcanoFallback(t *testing.T) {
	norm := NewNormalizer(&mockVolcanoProvider{volcanoes: map[string]domain.VolcanoReference{}})

	report := adapter.VolcanicReportDTO{
		ReportID:   "pvmbg-unknown",
		VolcanoID:  "VLC-999",
		AlertLevel: "Normal",
		ReportedAt: time.Now().UTC(),
	}

	canonical, err := norm.NormalizeVolcanic(context.Background(), report)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if canonical.AreaName != "VLC-999" {
		t.Errorf("expected AreaName fallback to 'VLC-999', got %s", canonical.AreaName)
	}
	if canonical.Latitude != nil || canonical.Longitude != nil {
		t.Errorf("expected nil coordinates on unknown volcano, got %v, %v", canonical.Latitude, canonical.Longitude)
	}
	if canonical.Severity != domain.SeverityNormal {
		t.Errorf("expected Severity NORMAL, got %s", canonical.Severity)
	}
}

func TestApplyTsunamiWarning(t *testing.T) {
	norm := NewNormalizer(nil)

	hazard := &domain.HazardEvent{
		HazardID:    "haz-bmkg-123",
		Source:      domain.SourceBMKG,
		SourceRefID: "bmkg-123",
		Severity:    domain.SeverityWaspada,
		Attributes:  map[string]any{"magnitude": 5.8},
	}

	warning := adapter.TsunamiWarningDTO{
		WarningID:        "tsu-warn-999",
		RelatedEventID:   "bmkg-123",
		ThreatLevel:      "Awas",
		AffectedZones:    []string{"Pesisir Selatan", "Bantul"},
		EstimatedArrival: time.Now().Add(30 * time.Minute),
	}

	updated := norm.ApplyTsunamiWarning(hazard, warning)

	if updated.Severity != domain.SeverityAwas {
		t.Errorf("expected escalated severity AWAS, got %s", updated.Severity)
	}
	tsuWarning, ok := updated.Attributes["tsunami_warning"].(map[string]any)
	if !ok {
		t.Fatalf("expected tsunami_warning attribute object, got %v", updated.Attributes["tsunami_warning"])
	}
	if tsuWarning["warning_id"] != "tsu-warn-999" {
		t.Errorf("expected warning_id 'tsu-warn-999', got %v", tsuWarning["warning_id"])
	}
}
