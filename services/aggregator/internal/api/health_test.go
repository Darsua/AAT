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

type mockBrokerPinger struct {
	err error
}

func (m *mockBrokerPinger) PingBroker(ctx context.Context) error {
	return m.err
}

func TestHealthHandler_AllHealthy(t *testing.T) {
	handler := NewHealthHandler(&mockPinger{err: nil}, &mockBrokerPinger{err: nil})

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

	if resp.Status != "UP" || resp.Database != "UP" || resp.Broker != "UP" || resp.Service != "aggregator" {
		t.Errorf("unexpected health status payload: %+v", resp)
	}
}

func TestHealthHandler_DatabaseDown(t *testing.T) {
	handler := NewHealthHandler(&mockPinger{err: errors.New("connection refused")}, &mockBrokerPinger{err: nil})

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

	if resp.Status != "DOWN" || resp.Database != "DOWN" || resp.Broker != "UP" {
		t.Errorf("unexpected health status payload when DB is down: %+v", resp)
	}
}

func TestHealthHandler_BrokerDown(t *testing.T) {
	handler := NewHealthHandler(&mockPinger{err: nil}, &mockBrokerPinger{err: errors.New("broker connection refused")})

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

	if resp.Status != "DOWN" || resp.Database != "UP" || resp.Broker != "DOWN" {
		t.Errorf("unexpected health status payload when broker is down: %+v", resp)
	}
}

func TestLivezHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	rec := httptest.NewRecorder()

	LivezHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp struct {
		Status  string `json:"status"`
		Service string `json:"service"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "UP" || resp.Service != "aggregator" {
		t.Fatalf("unexpected livez payload: %+v", resp)
	}
}

func TestReadyzHandler_BrokerDownRemainsReady(t *testing.T) {
	handler := NewHealthHandler(&mockPinger{err: nil}, &mockBrokerPinger{err: errors.New("broker down")})

	rec := httptest.NewRecorder()
	handler.ReadyzHandler(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /readyz when only broker is down, got %d", rec.Code)
	}

	handler.SetDraining(true)
	recDrain := httptest.NewRecorder()
	handler.ReadyzHandler(recDrain, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recDrain.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /readyz while draining, got %d", recDrain.Code)
	}
}
