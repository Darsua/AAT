package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockPinger struct {
	err error
}

func (m *mockPinger) Ping(ctx context.Context) error {
	return m.err
}

func TestHealthHandler_DatabaseHealthy(t *testing.T) {
	handler := NewHealthHandler(&mockPinger{err: nil})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp HealthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "UP" || resp.Database != "UP" || resp.Service != "aggregator" {
		t.Errorf("unexpected health status payload: %+v", resp)
	}
}

func TestHealthHandler_DatabaseDown(t *testing.T) {
	handler := NewHealthHandler(&mockPinger{err: errors.New("connection refused")})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var resp HealthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "DOWN" || resp.Database != "DOWN" {
		t.Errorf("unexpected health status payload when DB is down: %+v", resp)
	}
}
