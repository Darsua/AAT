package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stubFieldReportStager struct {
	mu      sync.Mutex
	staged  []StagedFieldReport
	calls   atomic.Int32
	stageFn func(ctx context.Context, report StagedFieldReport) error
}

func (s *stubFieldReportStager) Stage(ctx context.Context, report StagedFieldReport) error {
	s.calls.Add(1)
	if s.stageFn != nil {
		if err := s.stageFn(ctx, report); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.staged = append(s.staged, report)
	s.mu.Unlock()
	return nil
}

func (s *stubFieldReportStager) Close() error { return nil }

func validFieldReportJSON() string {
	return `{
		"reporter_id": "tim-sar-reg1",
		"hazard_type": "WILDFIRE",
		"severity": "SIAGA",
		"area_name": "Kecamatan Bukit Batu, Bengkalis, Riau",
		"latitude": 1.4523,
		"longitude": 101.9821,
		"occurred_at": "2026-11-12T02:30:00Z",
		"description": "Titik api meluas ke arah barat daya"
	}`
}

func TestFieldReports_BothRoutesAcceptedForFieldScope(t *testing.T) {
	const secret = "test-signing-secret"
	stager := &stubFieldReportStager{}
	h := newAPIHandlerWithDeps(secret, NewRemoteAggregatorClient("http://127.0.0.1:1"), stager).routes()
	token := signTestAccessToken(t, secret, scopeAll, jwtIssuer, time.Now().Add(time.Minute))

	for _, path := range []string{"/api/v1/field-reports", "/field-reports"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(validFieldReportJSON()))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(correlationHead, "corr-field-1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("%s: expected 202, got %d: %s", path, rec.Code, rec.Body.String())
		}
		var resp FieldReportResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("%s: decode response: %v", path, err)
		}
		if resp.ReportID == "" || resp.Status != "accepted" || resp.ReceivedAt == "" {
			t.Fatalf("%s: unexpected response: %+v", path, resp)
		}
	}

	if stager.calls.Load() != 2 {
		t.Fatalf("expected 2 staged reports, got %d", stager.calls.Load())
	}
}

func TestFieldReports_MediaScopeForbidden(t *testing.T) {
	const secret = "test-signing-secret"
	stager := &stubFieldReportStager{}
	h := newAPIHandlerWithDeps(secret, NewRemoteAggregatorClient("http://127.0.0.1:1"), stager).routes()
	mediaToken := signTestAccessToken(t, secret, scopeSummary, jwtIssuer, time.Now().Add(time.Minute))

	for _, path := range []string{"/api/v1/field-reports", "/field-reports"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(validFieldReportJSON()))
		req.Header.Set("Authorization", "Bearer "+mediaToken)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s: expected 403 for Media token, got %d: %s", path, rec.Code, rec.Body.String())
		}
	}
	if stager.calls.Load() != 0 {
		t.Fatalf("media request must not reach durable stager")
	}
}

func TestFieldReports_ValidationErrors(t *testing.T) {
	const secret = "test-signing-secret"
	stager := &stubFieldReportStager{}
	h := newAPIHandlerWithDeps(secret, NewRemoteAggregatorClient("http://127.0.0.1:1"), stager).routes()
	token := signTestAccessToken(t, secret, scopeAll, jwtIssuer, time.Now().Add(time.Minute))

	cases := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"missing_reporter", `{"hazard_type":"WILDFIRE","severity":"SIAGA","area_name":"Riau","occurred_at":"2026-11-12T02:30:00Z"}`},
		{"bad_type", `{"reporter_id":"tim-1","hazard_type":"FLOOD","severity":"SIAGA","area_name":"Riau","occurred_at":"2026-11-12T02:30:00Z"}`},
		{"bad_severity", `{"reporter_id":"tim-1","hazard_type":"WILDFIRE","severity":"CRITICAL","area_name":"Riau","occurred_at":"2026-11-12T02:30:00Z"}`},
		{"bad_lat", `{"reporter_id":"tim-1","hazard_type":"WILDFIRE","severity":"SIAGA","area_name":"Riau","latitude":95,"occurred_at":"2026-11-12T02:30:00Z"}`},
		{"bad_occurred_at", `{"reporter_id":"tim-1","hazard_type":"WILDFIRE","severity":"SIAGA","area_name":"Riau","occurred_at":"not-a-time"}`},
		{"unknown_field", `{"reporter_id":"tim-1","hazard_type":"WILDFIRE","severity":"SIAGA","area_name":"Riau","occurred_at":"2026-11-12T02:30:00Z","extra":1}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/field-reports", strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
	if stager.calls.Load() != 0 {
		t.Fatalf("invalid requests must not be staged")
	}
}

func TestFieldReports_StagingFailureReturns503Never202(t *testing.T) {
	const secret = "test-signing-secret"
	stager := &stubFieldReportStager{
		stageFn: func(context.Context, StagedFieldReport) error {
			return errors.New("broker unavailable")
		},
	}
	h := newAPIHandlerWithDeps(secret, NewRemoteAggregatorClient("http://127.0.0.1:1"), stager).routes()
	token := signTestAccessToken(t, secret, scopeAll, jwtIssuer, time.Now().Add(time.Minute))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/field-reports", strings.NewReader(validFieldReportJSON()))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", "idem-fail-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when staging fails, got %d: %s", rec.Code, rec.Body.String())
	}

	// Subsequent retry with same Idempotency-Key after broker recovers must succeed.
	stager.stageFn = nil
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/field-reports", strings.NewReader(validFieldReportJSON()))
	req2.Header.Set("Authorization", "Bearer "+token)
	req2.Header.Set("Idempotency-Key", "idem-fail-1")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Fatalf("expected 202 on retry after recovery, got %d: %s", rec2.Code, rec2.Body.String())
	}
}

func TestFieldReports_IdempotencyRetryConflictAndConcurrency(t *testing.T) {
	const secret = "test-signing-secret"
	gate := make(chan struct{})
	stager := &stubFieldReportStager{
		stageFn: func(ctx context.Context, report StagedFieldReport) error {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
	}
	h := newAPIHandlerWithDeps(secret, NewRemoteAggregatorClient("http://127.0.0.1:1"), stager).routes()
	token := signTestAccessToken(t, secret, scopeAll, jwtIssuer, time.Now().Add(time.Minute))

	// Fire two concurrent duplicate requests with the same Idempotency-Key.
	var wg sync.WaitGroup
	resps := make([]FieldReportResponse, 2)
	codes := make([]int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/field-reports", strings.NewReader(validFieldReportJSON()))
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", "idem-concurrent-1")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			codes[idx] = rec.Code
			_ = json.NewDecoder(rec.Body).Decode(&resps[idx])
		}(i)
	}

	time.Sleep(20 * time.Millisecond)
	close(gate)
	wg.Wait()

	if codes[0] != http.StatusAccepted || codes[1] != http.StatusAccepted {
		t.Fatalf("expected both concurrent duplicates to get 202, got %v", codes)
	}
	if resps[0].ReportID != resps[1].ReportID || !strings.HasPrefix(resps[0].ReportID, "rep-idem-") {
		t.Fatalf("expected identical deterministic report_id, got %+v vs %+v", resps[0], resps[1])
	}
	if stager.calls.Load() != 1 {
		t.Fatalf("expected exactly 1 durable stage call for concurrent duplicates, got %d", stager.calls.Load())
	}

	// Sequential retry with same key + same payload returns same 202 without re-staging.
	reqRetry := httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(validFieldReportJSON()))
	reqRetry.Header.Set("Authorization", "Bearer "+token)
	reqRetry.Header.Set("Idempotency-Key", "idem-concurrent-1")
	recRetry := httptest.NewRecorder()
	h.ServeHTTP(recRetry, reqRetry)
	if recRetry.Code != http.StatusAccepted {
		t.Fatalf("expected 202 on sequential retry, got %d", recRetry.Code)
	}
	if stager.calls.Load() != 1 {
		t.Fatalf("expected still 1 stage call after retry, got %d", stager.calls.Load())
	}

	// Reusing same key with different payload returns 409 Conflict.
	differentBody := strings.Replace(validFieldReportJSON(), `"SIAGA"`, `"AWAS"`, 1)
	reqConflict := httptest.NewRequest(http.MethodPost, "/api/v1/field-reports", strings.NewReader(differentBody))
	reqConflict.Header.Set("Authorization", "Bearer "+token)
	reqConflict.Header.Set("Idempotency-Key", "idem-concurrent-1")
	recConflict := httptest.NewRecorder()
	h.ServeHTTP(recConflict, reqConflict)
	if recConflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on payload mismatch, got %d: %s", recConflict.Code, recConflict.Body.String())
	}
}
