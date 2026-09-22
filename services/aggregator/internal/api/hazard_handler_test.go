package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/repository"
)

type mockHazardQuerier struct {
	events []domain.HazardEvent
	total  int
	err    error
}

func (m *mockHazardQuerier) QueryHazards(ctx context.Context, filter repository.HazardFilter) ([]domain.HazardEvent, int, error) {
	if m.err != nil {
		return nil, 0, m.err
	}
	return m.events, m.total, nil
}

func TestHazardHandler_Success(t *testing.T) {
	now := time.Now().UTC()
	lat := -7.5407
	lon := 110.4456

	mockRepo := &mockHazardQuerier{
		events: []domain.HazardEvent{
			{
				HazardID:    "haz-1",
				Source:      domain.SourcePVMBG,
				SourceRefID: "pvmbg-1",
				HazardType:  domain.HazardTypeVolcanic,
				Severity:    domain.SeveritySiaga,
				AreaName:    "Gunung Merapi",
				Latitude:    &lat,
				Longitude:   &lon,
				Attributes: map[string]any{
					"volcano_id":       "VLC-001",
					"confidence_level": 0.95,
				},
				OccurredAt: now,
				IngestedAt: now,
			},
		},
		total: 1,
	}

	handler := NewHazardHandler(mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/internal/hazards?type=VOLCANIC&severity=SIAGA&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp HazardListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Meta.Total != 1 || len(resp.Data) != 1 {
		t.Errorf("expected 1 event, got total=%d len=%d", resp.Meta.Total, len(resp.Data))
	}
	if resp.Data[0].HazardID != "haz-1" {
		t.Errorf("expected HazardID haz-1, got %s", resp.Data[0].HazardID)
	}
}

func TestHazardHandler_FilterParameterPassing(t *testing.T) {
	var capturedFilter repository.HazardFilter

	// Wrap with custom interceptor
	interceptor := &capturingQuerier{
		onQuery: func(f repository.HazardFilter) {
			capturedFilter = f
		},
	}

	handler := NewHazardHandler(interceptor)

	req := httptest.NewRequest(http.MethodGet, "/internal/hazards?since=2026-09-22T08:00:00Z&type=SEISMIC&severity=WASPADA&limit=25&offset=50", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	if capturedFilter.Limit != 25 {
		t.Errorf("expected limit 25, got %d", capturedFilter.Limit)
	}
	if capturedFilter.Offset != 50 {
		t.Errorf("expected offset 50, got %d", capturedFilter.Offset)
	}
	if capturedFilter.Type == nil || *capturedFilter.Type != domain.HazardTypeSeismic {
		t.Errorf("expected type SEISMIC, got %v", capturedFilter.Type)
	}
	if capturedFilter.Severity == nil || *capturedFilter.Severity != domain.SeverityWaspada {
		t.Errorf("expected severity WASPADA, got %v", capturedFilter.Severity)
	}
	if capturedFilter.Since == nil || capturedFilter.Since.Format(time.RFC3339) != "2026-09-22T08:00:00Z" {
		t.Errorf("expected since 2026-09-22T08:00:00Z, got %v", capturedFilter.Since)
	}
}

type capturingQuerier struct {
	onQuery func(f repository.HazardFilter)
}

func (c *capturingQuerier) QueryHazards(ctx context.Context, filter repository.HazardFilter) ([]domain.HazardEvent, int, error) {
	if c.onQuery != nil {
		c.onQuery(filter)
	}
	return []domain.HazardEvent{}, 0, nil
}

func TestHazardHandler_ValidationErrors(t *testing.T) {
	handler := NewHazardHandler(&mockHazardQuerier{})

	testCases := []struct {
		name       string
		url        string
		expectedMsg string
	}{
		{
			name:        "Invalid type",
			url:         "/internal/hazards?type=TSUNAMI",
			expectedMsg: "SEISMIC or VOLCANIC",
		},
		{
			name:        "Invalid severity",
			url:         "/internal/hazards?severity=CRITICAL",
			expectedMsg: "NORMAL, WASPADA, SIAGA, or AWAS",
		},
		{
			name:        "Limit too high",
			url:         "/internal/hazards?limit=999",
			expectedMsg: "between 1 and 500",
		},
		{
			name:        "Limit negative",
			url:         "/internal/hazards?limit=-5",
			expectedMsg: "between 1 and 500",
		},
		{
			name:        "Offset negative",
			url:         "/internal/hazards?offset=-1",
			expectedMsg: "non-negative integer",
		},
		{
			name:        "Invalid since format",
			url:         "/internal/hazards?since=not-a-date",
			expectedMsg: "RFC3339",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected status 400, got %d for url %s", rec.Code, tc.url)
			}
		})
	}
}

func TestHazardHandler_InternalError(t *testing.T) {
	mockRepo := &mockHazardQuerier{err: errors.New("db query failed")}
	handler := NewHazardHandler(mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/internal/hazards", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}
}
