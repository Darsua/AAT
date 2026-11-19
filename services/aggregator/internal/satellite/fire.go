package satellite

import (
	"fmt"
	"sort"
	"time"

	"bnpb/aggregator/internal/domain"
)

// Canonical values for satellite wildfires (contracts/hazard_event.json).
const (
	SourceSatelit      domain.Source     = "SATELIT"
	HazardTypeWildfire domain.HazardType = "WILDFIRE"
	hazardIDPrefix                       = "haz-satelit-"
	minFireDetections                    = 2
	awasDetections                       = 20
	awasFRPMW                            = 100.0
	siagaDetections                      = 6
	siagaFRPMW                           = 30.0
)

// Fire is one connected component with at least two detections.
type Fire struct {
	ID            string
	DetectionKeys []string // sorted, exactly as received
	Satellites    []string // sorted, unique
	MaxConfidence string
	TotalFRPMW    float64
	Latitude      float64 // centroid
	Longitude     float64
	EarliestAt    time.Time
	Severity      domain.Severity
}

// Severity applies the issue #28 thresholds.
func Severity(detections int, totalFRPMW float64) domain.Severity {
	switch {
	case detections >= awasDetections || totalFRPMW >= awasFRPMW:
		return domain.SeverityAwas
	case detections >= siagaDetections || totalFRPMW >= siagaFRPMW:
		return domain.SeveritySiaga
	default:
		return domain.SeverityWaspada
	}
}

func (e *Engine) buildFire(c *component) Fire { return e.buildFireFromKeys(c.id, c.keys) }

func (e *Engine) buildFireFromKeys(id string, keys map[string]struct{}) Fire {
	f := Fire{ID: id, DetectionKeys: sortedKeys(keys)}
	sats := make(map[string]struct{})
	bestRank := -2.0
	var latSum, lonSum float64
	for i, k := range f.DetectionKeys {
		d := e.detections[k]
		sats[d.Satellite] = struct{}{}
		f.TotalFRPMW += d.FRP
		latSum += d.lat
		lonSum += d.lon
		if r := confidenceRank(d.Confidence); r > bestRank {
			bestRank, f.MaxConfidence = r, d.Confidence
		}
		if i == 0 || d.acquired.Before(f.EarliestAt) {
			f.EarliestAt = d.acquired
		}
	}
	if n := float64(len(f.DetectionKeys)); n > 0 {
		f.Latitude, f.Longitude = latSum/n, lonSum/n
	}
	for s := range sats {
		f.Satellites = append(f.Satellites, s)
	}
	sort.Strings(f.Satellites)
	f.Severity = Severity(len(f.DetectionKeys), f.TotalFRPMW)
	return f
}

// HazardIDFor returns the canonical hazard ID of a fire.
func HazardIDFor(fireID string) string { return hazardIDPrefix + fireID }

// AreaLocator names the area for a centroid, e.g. the nearest reference-province centroid.
type AreaLocator func(lat, lon float64) string

// ToHazard maps a fire to the canonical HazardEvent. IngestedAt is left for the repository.
func (f Fire) ToHazard(locate AreaLocator) domain.HazardEvent {
	area := ""
	if locate != nil {
		area = locate(f.Latitude, f.Longitude)
	}
	if area == "" {
		area = fmt.Sprintf("Titik api %.4f, %.4f", f.Latitude, f.Longitude)
	}
	lat, lon := f.Latitude, f.Longitude
	return domain.HazardEvent{
		HazardID:    HazardIDFor(f.ID),
		Source:      SourceSatelit,
		SourceRefID: f.ID,
		HazardType:  HazardTypeWildfire,
		Severity:    f.Severity,
		AreaName:    area,
		OccurredAt:  f.EarliestAt,
		Latitude:    &lat,
		Longitude:   &lon,
		Attributes: map[string]any{
			"detection_count": len(f.DetectionKeys),
			"satellites":      f.Satellites,
			"max_confidence":  f.MaxConfidence,
			"total_frp_mw":    f.TotalFRPMW,
			"detection_keys":  f.DetectionKeys,
		},
	}
}
