package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"bnpb/aggregator/internal/logger"
)

// SeismicEventDTO represents the upstream seismic event model from BMKG.
type SeismicEventDTO struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

// TsunamiWarningDTO represents the upstream tsunami warning model from BMKG.
type TsunamiWarningDTO struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"` // "Waspada", "Siaga", "Awas"
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

// BMKGAdapter provides client methods to poll BMKG upstream feeds.
type BMKGAdapter struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewBMKGAdapter instantiates a new BMKGAdapter.
func NewBMKGAdapter(baseURL, apiKey string, timeout time.Duration) *BMKGAdapter {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &BMKGAdapter{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// FetchSeismicEvents retrieves seismic sensor telemetry records, optionally filtered by since.
func (a *BMKGAdapter) FetchSeismicEvents(ctx context.Context, since *time.Time) ([]SeismicEventDTO, error) {
	reqURL := fmt.Sprintf("%s/seismic-events", a.baseURL)
	if since != nil && !since.IsZero() {
		q := url.Values{}
		q.Set("since", since.UTC().Format(time.RFC3339))
		reqURL += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create BMKG seismic request: %w", err)
	}

	if a.apiKey != "" {
		req.Header.Set("X-BMKG-Key", a.apiKey)
	}
	if corrID := logger.CorrelationIDFromContext(ctx); corrID != "" {
		req.Header.Set("X-Correlation-ID", corrID)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute BMKG seismic request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("BMKG seismic endpoint returned non-200 status: %d", resp.StatusCode)
	}

	var events []SeismicEventDTO
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		return nil, fmt.Errorf("failed to decode BMKG seismic events: %w", err)
	}

	return events, nil
}

// FetchTsunamiWarnings retrieves active or recent tsunami warning advisories.
func (a *BMKGAdapter) FetchTsunamiWarnings(ctx context.Context, since *time.Time) ([]TsunamiWarningDTO, error) {
	reqURL := fmt.Sprintf("%s/tsunami-warnings", a.baseURL)
	if since != nil && !since.IsZero() {
		q := url.Values{}
		q.Set("since", since.UTC().Format(time.RFC3339))
		reqURL += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create BMKG tsunami request: %w", err)
	}

	if a.apiKey != "" {
		req.Header.Set("X-BMKG-Key", a.apiKey)
	}
	if corrID := logger.CorrelationIDFromContext(ctx); corrID != "" {
		req.Header.Set("X-Correlation-ID", corrID)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute BMKG tsunami request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("BMKG tsunami endpoint returned non-200 status: %d", resp.StatusCode)
	}

	var warnings []TsunamiWarningDTO
	if err := json.NewDecoder(resp.Body).Decode(&warnings); err != nil {
		return nil, fmt.Errorf("failed to decode BMKG tsunami warnings: %w", err)
	}

	return warnings, nil
}
