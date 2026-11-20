package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"bnpb/aggregator/internal/logger"
)

const (
	maxCoastalBodyBytes    = 4 << 20 // 4 MiB safety cap for untrusted blackbox responses
	maxFutureObservedSkew  = 5 * time.Minute
)

// CoastalStation represents a buoy station from GET /v1/stations.
type CoastalStation struct {
	StationID string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// CoastalObservation represents a buoy reading from GET /v1/observations.
type CoastalObservation struct {
	ObservationID     string  `json:"observation_id"`
	StationID         string  `json:"station_id"`
	ObservedAt        string  `json:"observed_at"`
	SeaLevelAnomalyM  float64 `json:"sea_level_anomaly_m"`
	WaveHeightM       float64 `json:"wave_height_m"`
	Quality           string  `json:"quality"`
	Critical          bool    `json:"critical"`
}

// RejectedObservation captures a contract-violating coastal item for quarantine.
type RejectedObservation struct {
	ObservationID string
	StationID     string
	Reason        string
	RawPayload    []byte
}

// CoastalObservationBatch holds valid and quarantined observations from one page.
type CoastalObservationBatch struct {
	GeneratedAt time.Time
	Valid       []CoastalObservation
	Rejected    []RejectedObservation
	NextCursor  string
}

// CoastalAdapter polls the untrusted blackbox Coastal Sensor Network API.
type CoastalAdapter struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewCoastalAdapter creates a dedicated CoastalAdapter with its own isolated HTTP client.
func NewCoastalAdapter(baseURL, token string, timeout time.Duration) *CoastalAdapter {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &CoastalAdapter{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// FetchStations retrieves the active coastal station catalog.
func (a *CoastalAdapter) FetchStations(ctx context.Context) (map[string]CoastalStation, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+"/v1/stations", nil)
	if err != nil {
		return nil, fmt.Errorf("create coastal stations request: %w", err)
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	if corrID := logger.CorrelationIDFromContext(ctx); corrID != "" {
		req.Header.Set("X-Correlation-ID", corrID)
	}

	resp, err := doLogged(ctx, a.httpClient, req, "sensor-pantai")
	if err != nil {
		return nil, fmt.Errorf("fetch coastal stations: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, ErrUpstreamUnavailable
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("coastal stations returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCoastalBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("read coastal stations response: %w", err)
	}

	var payload struct {
		Items []CoastalStation `json:"items"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode coastal stations: %w", err)
	}

	catalog := make(map[string]CoastalStation, len(payload.Items))
	for _, st := range payload.Items {
		if strings.TrimSpace(st.StationID) == "" ||
			math.IsNaN(st.Latitude) || math.IsInf(st.Latitude, 0) || st.Latitude < -90 || st.Latitude > 90 ||
			math.IsNaN(st.Longitude) || math.IsInf(st.Longitude, 0) || st.Longitude < -180 || st.Longitude > 180 {
			continue
		}
		catalog[st.StationID] = st
	}
	if len(catalog) == 0 {
		return nil, errors.New("coastal stations catalog is empty or invalid")
	}
	return catalog, nil
}

// FetchObservations fetches one page of observations and isolates invalid items.
func (a *CoastalAdapter) FetchObservations(ctx context.Context, limit int, cursor string, stations map[string]CoastalStation, now time.Time) (*CoastalObservationBatch, error) {
	u, err := url.Parse(a.baseURL + "/v1/observations")
	if err != nil {
		return nil, fmt.Errorf("parse coastal observations url: %w", err)
	}
	q := u.Query()
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create coastal observations request: %w", err)
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	if corrID := logger.CorrelationIDFromContext(ctx); corrID != "" {
		req.Header.Set("X-Correlation-ID", corrID)
	}

	resp, err := doLogged(ctx, a.httpClient, req, "sensor-pantai")
	if err != nil {
		return nil, fmt.Errorf("fetch coastal observations: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, ErrUpstreamUnavailable
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("coastal observations returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCoastalBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("read coastal observations response: %w", err)
	}

	var envelope struct {
		GeneratedAt string            `json:"generated_at"`
		Items       []json.RawMessage `json:"items"`
		NextCursor  *string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode coastal observations envelope: %w", err)
	}
	genAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(envelope.GeneratedAt))
	if err != nil {
		return nil, fmt.Errorf("invalid coastal generated_at timestamp: %w", err)
	}

	batch := &CoastalObservationBatch{
		GeneratedAt: genAt.UTC(),
		Valid:       make([]CoastalObservation, 0, len(envelope.Items)),
	}
	if envelope.NextCursor != nil {
		batch.NextCursor = strings.TrimSpace(*envelope.NextCursor)
	}

	for _, rawItem := range envelope.Items {
		obs, reason := parseAndValidateObservation(rawItem, stations, now)
		if reason != "" {
			var partial struct {
				ObservationID string `json:"observation_id"`
				StationID     string `json:"station_id"`
			}
			_ = json.Unmarshal(rawItem, &partial)
			batch.Rejected = append(batch.Rejected, RejectedObservation{
				ObservationID: partial.ObservationID,
				StationID:     partial.StationID,
				Reason:        reason,
				RawPayload:    append([]byte(nil), rawItem...),
			})
			continue
		}
		batch.Valid = append(batch.Valid, obs)
	}

	return batch, nil
}

func parseAndValidateObservation(raw []byte, stations map[string]CoastalStation, now time.Time) (CoastalObservation, string) {
	var rawMap map[string]any
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		return CoastalObservation{}, fmt.Sprintf("malformed observation JSON: %v", err)
	}
	for _, reqField := range []string{
		"observation_id",
		"station_id",
		"observed_at",
		"sea_level_anomaly_m",
		"wave_height_m",
		"quality",
		"critical",
	} {
		if _, ok := rawMap[reqField]; !ok {
			return CoastalObservation{}, fmt.Sprintf("missing required field %q", reqField)
		}
	}

	var obs CoastalObservation
	if err := json.Unmarshal(raw, &obs); err != nil {
		return CoastalObservation{}, fmt.Sprintf("invalid observation field types: %v", err)
	}

	obs.ObservationID = strings.TrimSpace(obs.ObservationID)
	obs.StationID = strings.TrimSpace(obs.StationID)
	obs.ObservedAt = strings.TrimSpace(obs.ObservedAt)
	obs.Quality = strings.TrimSpace(obs.Quality)

	if obs.ObservationID == "" {
		return CoastalObservation{}, "observation_id is empty"
	}
	if obs.StationID == "" {
		return CoastalObservation{}, "station_id is empty"
	}
	if len(stations) > 0 {
		if _, ok := stations[obs.StationID]; !ok {
			return CoastalObservation{}, fmt.Sprintf("unknown station_id %q", obs.StationID)
		}
	}
	observedAt, err := time.Parse(time.RFC3339Nano, obs.ObservedAt)
	if err != nil {
		return CoastalObservation{}, fmt.Sprintf("invalid observed_at timestamp %q", obs.ObservedAt)
	}
	if !now.IsZero() && observedAt.After(now.Add(maxFutureObservedSkew)) {
		return CoastalObservation{}, fmt.Sprintf("observed_at %s is in the future", obs.ObservedAt)
	}
	if math.IsNaN(obs.SeaLevelAnomalyM) || math.IsInf(obs.SeaLevelAnomalyM, 0) {
		return CoastalObservation{}, "sea_level_anomaly_m is not finite"
	}
	if math.IsNaN(obs.WaveHeightM) || math.IsInf(obs.WaveHeightM, 0) || obs.WaveHeightM < 0 {
		return CoastalObservation{}, fmt.Sprintf("invalid wave_height_m %v", obs.WaveHeightM)
	}
	if obs.Quality == "" {
		return CoastalObservation{}, "quality is empty"
	}
	obs.ObservedAt = observedAt.UTC().Format(time.RFC3339Nano)
	return obs, ""
}
