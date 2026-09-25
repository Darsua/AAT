package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPVMBGAuthSchemaAndOutageControls(t *testing.T) {
	data, err := loadSeedData()
	if err != nil {
		t.Fatalf("loadSeedData() error = %v", err)
	}
	if len(data.Reports) < 20 {
		t.Fatalf("seed report count = %d, want at least 20", len(data.Reports))
	}

	svc := newService(data)
	svc.delay = func() {}
	handler := svc.routes("test-pvmbg-token")

	request := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}

	request = authorizedRequest(http.MethodGet, "/volcanic-reports", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("reports status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var reports []VolcanicReport
	if err := json.NewDecoder(recorder.Body).Decode(&reports); err != nil {
		t.Fatalf("decode reports response: %v", err)
	}
	if len(reports) != len(data.Reports) {
		t.Fatalf("report count = %d, want %d", len(reports), len(data.Reports))
	}
	for _, report := range reports {
		if report.ConfidenceLevel != nil {
			t.Fatalf("confidence_level should be absent before schema toggle: %+v", report)
		}
	}

	request = authorizedRequest(http.MethodPost, "/admin/schema-version", bytes.NewBufferString(`{"include_confidence_level":true}`))
	request.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("schema toggle status = %d, want %d", recorder.Code, http.StatusOK)
	}

	request = authorizedRequest(http.MethodGet, "/volcanic-reports", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if err := json.NewDecoder(recorder.Body).Decode(&reports); err != nil {
		t.Fatalf("decode evolved reports response: %v", err)
	}
	for _, report := range reports {
		if report.ConfidenceLevel == nil || *report.ConfidenceLevel != 0.95 {
			t.Fatalf("confidence_level missing or incorrect: %+v", report)
		}
	}

	request = authorizedRequest(http.MethodPost, "/admin/outage", bytes.NewBufferString(`{"is_outage":true}`))
	request.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("outage toggle status = %d, want %d", recorder.Code, http.StatusOK)
	}

	request = authorizedRequest(http.MethodGet, "/volcanic-reports", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("outage reports status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}

	request = httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("outage health status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}

	request = authorizedRequest(http.MethodPost, "/admin/outage", bytes.NewBufferString(`{"is_outage":false}`))
	request.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("restore outage status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestPVMBGReportFilterAndGeneration(t *testing.T) {
	data, err := loadSeedData()
	if err != nil {
		t.Fatalf("loadSeedData() error = %v", err)
	}

	svc := newService(data)
	svc.now = func() time.Time { return time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC) }
	before := len(svc.reports)
	svc.generateReport()
	if len(svc.reports) != before+1 {
		t.Fatalf("report count = %d, want %d", len(svc.reports), before+1)
	}

	since := data.Reports[10].ReportedAt
	filtered := filterReports(svc.reports, &since)
	if len(filtered) == 0 || filtered[0].ReportedAt.Before(since) {
		t.Fatalf("filtered reports should start at or after %s", since.Format(time.RFC3339))
	}
}

func authorizedRequest(method, path string, body io.Reader) *http.Request {
	request := httptest.NewRequest(method, path, body)
	request.Header.Set("Authorization", "Bearer test-pvmbg-token")
	return request
}
