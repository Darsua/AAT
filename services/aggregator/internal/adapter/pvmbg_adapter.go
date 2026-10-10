package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"bnpb/aggregator/internal/logger"
)

// ErrUpstreamUnavailable is returned when the upstream service returns HTTP 503 (e.g. outage simulation).
var ErrUpstreamUnavailable = errors.New("upstream service unavailable")

// VolcanicReportDTO represents the upstream volcanic monitoring report from PVMBG.
// Captures both canonical properties and dynamically evolving additive fields.
type VolcanicReportDTO struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"` // "Normal", "Waspada", "Siaga", "Awas"
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`

	// DynamicFields holds any additive fields not in the fixed struct (e.g. confidence_level).
	DynamicFields map[string]any `json:"-"`
}

// UnmarshalJSON implements custom JSON unmarshaling to dynamically preserve additive fields (P1 live schema evolution).
func (v *VolcanicReportDTO) UnmarshalJSON(data []byte) error {
	type Alias VolcanicReportDTO
	var aux Alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	*v = VolcanicReportDTO(aux)

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Remove known standard fields
	delete(raw, "report_id")
	delete(raw, "volcano_id")
	delete(raw, "alert_level")
	delete(raw, "eruption_count_24h")
	delete(raw, "ash_column_height_m")
	delete(raw, "reported_at")

	v.DynamicFields = raw
	return nil
}

// PVMBGAdapter provides client methods to poll PVMBG upstream feeds.
type PVMBGAdapter struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewPVMBGAdapter instantiates a new PVMBGAdapter.
func NewPVMBGAdapter(baseURL, token string, timeout time.Duration) *PVMBGAdapter {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &PVMBGAdapter{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// FetchVolcanicReports retrieves volcanic observation reports, optionally filtered by since.
func (a *PVMBGAdapter) FetchVolcanicReports(ctx context.Context, since *time.Time) ([]VolcanicReportDTO, error) {
	reqURL := fmt.Sprintf("%s/volcanic-reports", a.baseURL)
	if since != nil && !since.IsZero() {
		q := url.Values{}
		q.Set("since", since.UTC().Format(time.RFC3339))
		reqURL += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create PVMBG request: %w", err)
	}

	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	if corrID := logger.CorrelationIDFromContext(ctx); corrID != "" {
		req.Header.Set("X-Correlation-ID", corrID)
	}

	resp, err := doLogged(ctx, a.httpClient, req, "pvmbg")
	if err != nil {
		return nil, fmt.Errorf("%w: failed to execute PVMBG request: %w", ErrUpstreamUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, ErrUpstreamUnavailable
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("PVMBG endpoint returned non-200 status: %d", resp.StatusCode)
	}

	var reports []VolcanicReportDTO
	if err := json.NewDecoder(resp.Body).Decode(&reports); err != nil {
		return nil, fmt.Errorf("failed to decode PVMBG volcanic reports: %w", err)
	}

	return reports, nil
}
