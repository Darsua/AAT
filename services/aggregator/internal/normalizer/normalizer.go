package normalizer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
)

// VolcanoProvider defines the dependency needed to enrich volcanic reports with static coordinates.
type VolcanoProvider interface {
	GetVolcanoByID(ctx context.Context, id string) (*domain.VolcanoReference, error)
}

// Normalizer transforms upstream provider payloads into canonical HazardEvent entities.
type Normalizer struct {
	volcanoes VolcanoProvider
}

// NewNormalizer creates a new Normalizer instance.
func NewNormalizer(volcanoes VolcanoProvider) *Normalizer {
	return &Normalizer{
		volcanoes: volcanoes,
	}
}

// formatHazardID generates a deterministic, collision-resistant canonical hazard ID bounded to 64 chars.
func formatHazardID(prefix, sourceRefID string) string {
	candidate := fmt.Sprintf("%s-%s", prefix, sourceRefID)
	if len(candidate) <= 64 {
		return candidate
	}
	h := sha256.Sum256([]byte(candidate))
	hashHex := hex.EncodeToString(h[:])[:16]
	prefixWithDash := prefix + "-"
	maxRefLen := 64 - len(prefixWithDash) - len(hashHex) - 1
	if maxRefLen > len(sourceRefID) {
		maxRefLen = len(sourceRefID)
	}
	return fmt.Sprintf("%s%s-%s", prefixWithDash, sourceRefID[:maxRefLen], hashHex)
}

// NormalizeSeismic maps BMKG seismic sensor data and correlated tsunami advisories into a canonical HazardEvent.
func (n *Normalizer) NormalizeSeismic(event adapter.SeismicEventDTO, warnings []adapter.TsunamiWarningDTO) domain.HazardEvent {
	hazardID := formatHazardID("haz-bmkg", event.EventID)
	lat := event.EpicenterLat
	lon := event.EpicenterLon

	// Check for correlated tsunami warning
	var correlated *adapter.TsunamiWarningDTO
	for i := range warnings {
		if warnings[i].RelatedEventID == event.EventID {
			correlated = &warnings[i]
			break
		}
	}

	severity := deriveSeismicSeverity(event.Magnitude, correlated)

	attrs := map[string]any{
		"magnitude":         event.Magnitude,
		"depth_km":          event.DepthKM,
		"potential_tsunami": event.PotentialTsunami,
	}

	if correlated != nil {
		attrs["tsunami_warning"] = map[string]any{
			"warning_id":        correlated.WarningID,
			"threat_level":      correlated.ThreatLevel,
			"affected_zones":    correlated.AffectedZones,
			"estimated_arrival": correlated.EstimatedArrival.UTC().Format(time.RFC3339),
		}
	}

	return domain.HazardEvent{
		HazardID:    hazardID,
		Source:      domain.SourceBMKG,
		SourceRefID: event.EventID,
		HazardType:  domain.HazardTypeSeismic,
		Severity:    severity,
		AreaName:    event.RegionName,
		Latitude:    &lat,
		Longitude:   &lon,
		Attributes:  attrs,
		OccurredAt:  event.OccurredAt.UTC(),
	}
}

// NormalizeVolcanic maps PVMBG volcanic observations into a canonical HazardEvent, enriching coordinates from static references.
func (n *Normalizer) NormalizeVolcanic(ctx context.Context, report adapter.VolcanicReportDTO) (domain.HazardEvent, error) {
	hazardID := formatHazardID("haz-pvmbg", report.ReportID)

	areaName := report.VolcanoID
	var latPtr, lonPtr *float64

	if n.volcanoes != nil {
		volcano, err := n.volcanoes.GetVolcanoByID(ctx, report.VolcanoID)
		if err != nil {
			return domain.HazardEvent{}, fmt.Errorf("failed to lookup volcano reference %q: %w", report.VolcanoID, err)
		}
		if volcano != nil {
			areaName = volcano.VolcanoName
			lat := volcano.Latitude
			lon := volcano.Longitude
			latPtr = &lat
			lonPtr = &lon
		}
	}

	severity := mapAlertLevelToSeverity(report.AlertLevel)

	attrs := map[string]any{
		"volcano_id":          report.VolcanoID,
		"eruption_count_24h":  report.EruptionCount24h,
		"ash_column_height_m": report.AshColumnHeightM,
	}

	// Dynamic additive field forwarding (P1 live schema evolution requirement)
	for k, v := range report.DynamicFields {
		attrs[k] = v
	}

	return domain.HazardEvent{
		HazardID:    hazardID,
		Source:      domain.SourcePVMBG,
		SourceRefID: report.ReportID,
		HazardType:  domain.HazardTypeVolcanic,
		Severity:    severity,
		AreaName:    areaName,
		Latitude:    latPtr,
		Longitude:   lonPtr,
		Attributes:  attrs,
		OccurredAt:  report.ReportedAt.UTC(),
	}, nil
}

// ApplyTsunamiWarning updates an existing seismic hazard with new tsunami advisory information.
func (n *Normalizer) ApplyTsunamiWarning(hazard *domain.HazardEvent, warning adapter.TsunamiWarningDTO) *domain.HazardEvent {
	updated := *hazard
	if updated.Attributes == nil {
		updated.Attributes = make(map[string]any)
	} else {
		newAttrs := make(map[string]any, len(updated.Attributes)+2)
		for k, v := range updated.Attributes {
			newAttrs[k] = v
		}
		updated.Attributes = newAttrs
	}

	updated.Attributes["tsunami_warning"] = map[string]any{
		"warning_id":        warning.WarningID,
		"threat_level":      warning.ThreatLevel,
		"affected_zones":    warning.AffectedZones,
		"estimated_arrival": warning.EstimatedArrival.UTC().Format(time.RFC3339),
	}
	updated.Attributes["potential_tsunami"] = true

	lvl := strings.ToUpper(strings.TrimSpace(warning.ThreatLevel))
	switch lvl {
	case "AWAS":
		updated.Severity = domain.SeverityAwas
	case "SIAGA":
		updated.Severity = domain.SeveritySiaga
	case "WASPADA":
		updated.Severity = domain.SeverityWaspada
	}

	return &updated
}

// deriveSeismicSeverity implements the graded severity derivation rules in spec §3.3.
func deriveSeismicSeverity(magnitude float64, warning *adapter.TsunamiWarningDTO) domain.Severity {
	if warning != nil {
		lvl := strings.ToUpper(strings.TrimSpace(warning.ThreatLevel))
		switch lvl {
		case "AWAS":
			return domain.SeverityAwas
		case "SIAGA":
			return domain.SeveritySiaga
		case "WASPADA":
			return domain.SeverityWaspada
		}
	}

	// Fallback to magnitude-based severity
	if magnitude < 5.0 {
		return domain.SeverityNormal
	} else if magnitude < 6.5 {
		return domain.SeverityWaspada
	}
	return domain.SeveritySiaga
}

// mapAlertLevelToSeverity maps PVMBG alert levels 1:1 to canonical Severity enum.
func mapAlertLevelToSeverity(alertLevel string) domain.Severity {
	switch strings.ToUpper(strings.TrimSpace(alertLevel)) {
	case "AWAS":
		return domain.SeverityAwas
	case "SIAGA":
		return domain.SeveritySiaga
	case "WASPADA":
		return domain.SeverityWaspada
	case "NORMAL":
		return domain.SeverityNormal
	default:
		return domain.SeverityNormal
	}
}
