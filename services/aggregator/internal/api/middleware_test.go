package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"bnpb/aggregator/internal/logger"
)

func TestCorrelationIDMiddleware_PreservesExisting(t *testing.T) {
	expectedCorrID := "my-trace-id-12345"
	var ctxCorrID string

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxCorrID = logger.CorrelationIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	mw := CorrelationIDMiddleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Correlation-ID", expectedCorrID)
	rec := httptest.NewRecorder()

	mw.ServeHTTP(rec, req)

	if ctxCorrID != expectedCorrID {
		t.Errorf("expected context correlation ID %q, got %q", expectedCorrID, ctxCorrID)
	}
	if rec.Header().Get("X-Correlation-ID") != expectedCorrID {
		t.Errorf("expected header correlation ID %q, got %q", expectedCorrID, rec.Header().Get("X-Correlation-ID"))
	}
}

func TestCorrelationIDMiddleware_GeneratesWhenMissing(t *testing.T) {
	var ctxCorrID string

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxCorrID = logger.CorrelationIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	mw := CorrelationIDMiddleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	mw.ServeHTTP(rec, req)

	if ctxCorrID == "" {
		t.Error("expected generated correlation ID in context, got empty")
	}
	if rec.Header().Get("X-Correlation-ID") == "" {
		t.Error("expected generated correlation ID in response header, got empty")
	}
	if rec.Header().Get("X-Correlation-ID") != ctxCorrID {
		t.Errorf("expected header %q to match context %q", rec.Header().Get("X-Correlation-ID"), ctxCorrID)
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("database connection lost")
	})

	mw := RecoveryMiddleware(panicHandler)

	req := httptest.NewRequest(http.MethodGet, "/internal/hazards", nil)
	rec := httptest.NewRecorder()

	mw.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}
	if resp["error"] != "INTERNAL_ERROR" {
		t.Errorf("expected error INTERNAL_ERROR, got %q", resp["error"])
	}
	if resp["message"] != "An unexpected internal server error occurred" {
		t.Errorf("expected message 'An unexpected internal server error occurred', got %q", resp["message"])
	}
}
