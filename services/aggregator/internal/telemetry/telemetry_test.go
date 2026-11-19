package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTraceparentAndMetrics(t *testing.T) {
	reg := NewRegistry()
	tracer := NewTracer("aggregator")
	inTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"

	handler := HTTPMiddleware("aggregator", reg, tracer, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := TraceFromContext(r.Context())
		if st.TraceID != inTraceID {
			t.Fatalf("expected traceID %s, got %s", inTraceID, st.TraceID)
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/internal/hazards", nil)
	req.Header.Set(TraceparentHeader, "00-"+inTraceID+"-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	mReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec := httptest.NewRecorder()
	reg.Handler()(mRec, mReq)

	body := mRec.Body.String()
	if !strings.Contains(body, `http_requests_total{method="GET",route="/internal/hazards",service="aggregator",status="200"} 1`) {
		t.Fatalf("missing http_requests_total in /metrics:\n%s", body)
	}
}
