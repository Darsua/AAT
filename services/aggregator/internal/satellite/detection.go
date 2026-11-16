// Package satellite turns the satellite hotspot feed (M2 P4, issue #28) into wildfire hazards.
//
// It is pure, storage-free logic: detection identity, event-time lateness, connectivity
// clustering, severity, and fire lifecycle. Persistence, checkpoints, and the SSE client
// are wired around it separately so the rules can be tested and replayed deterministically.
package satellite

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Detection is one satellite hotspot as received from the feed.
//
// Latitude and Longitude keep their exact textual form so the identity key is never
// rounded; the parsed float values are used only for distance computation.
type Detection struct {
	Satellite  string
	AcqDate    string // YYYY-MM-DD, UTC
	AcqTime    string // HHMM, UTC
	Latitude   string
	Longitude  string
	Confidence string
	FRP        float64

	lat      float64
	lon      float64
	acquired time.Time
}

// rawDetection mirrors the assumed JSON shape of a detection payload. Coordinates are
// decoded as json.Number so their original text survives into the identity key.
type rawDetection struct {
	Satellite  string          `json:"satellite"`
	AcqDate    string          `json:"acq_date"`
	AcqTime    json.RawMessage `json:"acq_time"`
	Latitude   json.Number     `json:"latitude"`
	Longitude  json.Number     `json:"longitude"`
	Confidence json.RawMessage `json:"confidence"`
	FRP        json.Number     `json:"frp"`
}

// ParseDetection decodes and validates a detection payload.
func ParseDetection(payload []byte) (Detection, error) {
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.UseNumber()
	var raw rawDetection
	if err := dec.Decode(&raw); err != nil {
		return Detection{}, fmt.Errorf("decode detection: %w", err)
	}
	acqTime, err := scalarText(raw.AcqTime)
	if err != nil {
		return Detection{}, fmt.Errorf("acq_time: %w", err)
	}
	confidence, err := scalarText(raw.Confidence)
	if err != nil {
		return Detection{}, fmt.Errorf("confidence: %w", err)
	}
	frp := 0.0
	if raw.FRP != "" {
		if frp, err = raw.FRP.Float64(); err != nil {
			return Detection{}, fmt.Errorf("frp: %w", err)
		}
	}
	return NewDetection(raw.Satellite, raw.AcqDate, acqTime, raw.Latitude.String(), raw.Longitude.String(), confidence, frp)
}

// scalarText returns a JSON string or number as its text, so "0905" and 905 are both accepted.
func scalarText(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return "", fmt.Errorf("must be a string or number, got %s", raw)
	}
	return n.String(), nil
}

// NewDetection validates fields and precomputes coordinates and acquisition time.
func NewDetection(satellite, acqDate, acqTime, latitude, longitude, confidence string, frp float64) (Detection, error) {
	d := Detection{
		Satellite:  strings.TrimSpace(satellite),
		AcqDate:    strings.TrimSpace(acqDate),
		AcqTime:    strings.TrimSpace(acqTime),
		Latitude:   strings.TrimSpace(latitude),
		Longitude:  strings.TrimSpace(longitude),
		Confidence: strings.TrimSpace(confidence),
		FRP:        frp,
	}
	if d.Satellite == "" {
		return Detection{}, fmt.Errorf("satellite is required")
	}
	var err error
	if d.lat, err = strconv.ParseFloat(d.Latitude, 64); err != nil || d.lat < -90 || d.lat > 90 {
		return Detection{}, fmt.Errorf("invalid latitude %q", latitude)
	}
	if d.lon, err = strconv.ParseFloat(d.Longitude, 64); err != nil || d.lon < -180 || d.lon > 180 {
		return Detection{}, fmt.Errorf("invalid longitude %q", longitude)
	}
	if math.IsNaN(frp) || math.IsInf(frp, 0) || frp < 0 {
		return Detection{}, fmt.Errorf("invalid frp %v", frp)
	}
	if d.acquired, err = parseAcquisition(d.AcqDate, d.AcqTime); err != nil {
		return Detection{}, err
	}
	return d, nil
}

// parseAcquisition combines acq_date and acq_time (HHMM, leading zeros optional) as UTC.
func parseAcquisition(acqDate, acqTime string) (time.Time, error) {
	day, err := time.Parse("2006-01-02", acqDate)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid acq_date %q", acqDate)
	}
	hhmm, err := strconv.Atoi(acqTime)
	if err != nil || hhmm < 0 || hhmm/100 > 23 || hhmm%100 > 59 {
		return time.Time{}, fmt.Errorf("invalid acq_time %q", acqTime)
	}
	return day.Add(time.Duration(hhmm/100)*time.Hour + time.Duration(hhmm%100)*time.Minute).UTC(), nil
}

// Key is the exact detection identity: satellite|acq_date|acq_time|latitude|longitude.
func (d Detection) Key() string {
	return strings.Join([]string{d.Satellite, d.AcqDate, d.AcqTime, d.Latitude, d.Longitude}, "|")
}

// AcquiredAt is the event time of the detection in UTC.
func (d Detection) AcquiredAt() time.Time { return d.acquired }

// LowConfidence reports whether the detection should be excluded from clustering.
//
// Assumption, to verify against the live feed: VIIRS-style categories ("l"/"low") or a
// MODIS-style 0-100 score below 30 mark low confidence.
func (d Detection) LowConfidence() bool {
	c := strings.ToLower(d.Confidence)
	switch c {
	case "l", "low":
		return true
	case "", "n", "nominal", "h", "high":
		return false
	}
	if score, err := strconv.ParseFloat(c, 64); err == nil {
		return score < 30
	}
	return false
}

// confidenceRank orders confidence values so the strongest can be reported per fire.
func confidenceRank(confidence string) float64 {
	switch strings.ToLower(confidence) {
	case "h", "high":
		return 1000
	case "n", "nominal":
		return 500
	case "l", "low":
		return 0
	}
	if score, err := strconv.ParseFloat(confidence, 64); err == nil {
		return score
	}
	return -1
}

const earthRadiusKm = 6371.0088

// distanceKm is the great-circle distance between two detections.
func distanceKm(a, b Detection) float64 {
	lat1, lat2 := a.lat*math.Pi/180, b.lat*math.Pi/180
	dLat := lat2 - lat1
	dLon := (b.lon - a.lon) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKm * math.Asin(math.Min(1, math.Sqrt(h)))
}
