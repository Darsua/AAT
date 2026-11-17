package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTraceparentRoundTrip(t *testing.T) {
	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	spanID := "00f067aa0ba902b7"
	header := formatTraceparent(traceID, spanID, true)
	if header != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("unexpected header: %s", header)
	}
	gotTrace, gotSpan, sampled, ok := parseTraceparent(header)
	if !ok || gotTrace != traceID || gotSpan != spanID || !sampled {
		t.Fatalf("parseTraceparent failed: got=(%s,%s,%v,%v)", gotTrace, gotSpan, sampled, ok)
	}
	if _, _, _, ok := parseTraceparent("invalid"); ok {
		t.Fatal("expected invalid traceparent to fail")
	}
}

func TestRegistryPrometheusFormat(t *testing.T) {
	reg := newRegistry()
	reg.IncCounter("http_requests_total", map[string]string{"service": "client-api", "status": "200"}, 2)
	reg.ObserveHistogram("http_request_duration_seconds", map[string]string{"service": "client-api"}, 0.02)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	reg.Handler()(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `http_requests_total{service="client-api",status="200"} 2`) {
		t.Fatalf("missing counter in /metrics:\n%s", body)
	}
	if !strings.Contains(body, `http_request_duration_seconds_bucket{service="client-api",le="0.025"} 1`) {
		t.Fatalf("missing histogram bucket in /metrics:\n%s", body)
	}
	if !strings.Contains(body, `http_request_duration_seconds_count{service="client-api"} 1`) {
		t.Fatalf("missing histogram count in /metrics:\n%s", body)
	}
}

func TestObservabilityMiddlewarePropagatesTraceparent(t *testing.T) {
	logger := newJSONLogger("client-api")
	reg := newRegistry()
	tracer := newTracer("client-api", logger)
	inTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"

	handler := correlationMiddleware(observabilityMiddleware("client-api", reg, tracer, logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := traceFromContext(r.Context())
		if st.TraceID != inTraceID {
			t.Fatalf("expected traceID %s, got %s", inTraceID, st.TraceID)
		}
		w.WriteHeader(http.StatusOK)
	})))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("traceparent", "00-"+inTraceID+"-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Correlation-ID") == "" {
		t.Fatal("expected X-Correlation-ID response header")
	}
	if !strings.HasPrefix(rec.Header().Get("traceparent"), "00-"+inTraceID+"-") {
		t.Fatalf("expected traceparent response header with traceID %s, got %s", inTraceID, rec.Header().Get("traceparent"))
	}
}
