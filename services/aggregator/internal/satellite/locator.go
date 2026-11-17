package satellite

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
)

// ProvinceCentroid is one entry of the reference-province table used for area_name.
type ProvinceCentroid struct {
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// ParseProvinceCentroids decodes a JSON array of {"name","latitude","longitude"}.
func ParseProvinceCentroids(data []byte) ([]ProvinceCentroid, error) {
	var centroids []ProvinceCentroid
	if err := json.Unmarshal(data, &centroids); err != nil {
		return nil, fmt.Errorf("decode province centroids: %w", err)
	}
	if len(centroids) == 0 {
		return nil, errors.New("province centroid table is empty")
	}
	for i, c := range centroids {
		if strings.TrimSpace(c.Name) == "" {
			return nil, fmt.Errorf("province centroid %d has no name", i)
		}
		if c.Latitude < -90 || c.Latitude > 90 || c.Longitude < -180 || c.Longitude > 180 {
			return nil, fmt.Errorf("province centroid %q has out-of-range coordinates", c.Name)
		}
	}
	return centroids, nil
}

// LoadProvinceCentroids reads the reference table from a JSON file.
func LoadProvinceCentroids(path string) ([]ProvinceCentroid, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read province centroids: %w", err)
	}
	return ParseProvinceCentroids(data)
}

// NearestCentroidLocator names a fire after the closest reference-province centroid by
// great-circle distance. Ties go to the earlier table entry, so the result is deterministic.
func NearestCentroidLocator(centroids []ProvinceCentroid) AreaLocator {
	table := append([]ProvinceCentroid(nil), centroids...)
	return func(lat, lon float64) string {
		best, bestKm := "", math.Inf(1)
		for _, c := range table {
			if km := haversineKm(lat, lon, c.Latitude, c.Longitude); km < bestKm {
				best, bestKm = c.Name, km
			}
		}
		return best
	}
}

func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	return distanceKm(Detection{lat: lat1, lon: lon1}, Detection{lat: lat2, lon: lon2})
}
