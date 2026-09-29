package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestMediaReceivesSummaryWithoutRawFields(t *testing.T) {
	var receivedCorrelationID string
	aggregator := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedCorrelationID = r.Header.Get(correlationHead)
		writeAggregatorResponse(t, w, []hazardEvent{testHazardEvent()}, 1)
	})

	handler, signingSecret := newTestClientAPI(t, aggregator)
	token := signTestAccessToken(t, signingSecret, scopeSummary, jwtIssuer, time.Now().Add(time.Minute))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/hazards?fields=summary&limit=10&offset=0", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set(correlationHead, "summary-test-request")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", response.Code, response.Body.String())
	}
	if receivedCorrelationID != "summary-test-request" {
		t.Fatalf("correlation ID was not propagated: %q", receivedCorrelationID)
	}

	var payload struct {
		Data []map[string]any `json:"data"`
		Meta publicMeta       `json:"meta"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Data) != 1 || payload.Meta.Limit != 10 || payload.Meta.Offset != 0 {
		t.Fatalf("unexpected response: %+v", payload)
	}
	for _, forbiddenField := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
		if _, exists := payload.Data[0][forbiddenField]; exists {
			t.Fatalf("summary leaked raw field %q", forbiddenField)
		}
	}
}

func TestMediaCannotRequestAllFields(t *testing.T) {
	var aggregatorCalls atomic.Int32
	aggregator := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		aggregatorCalls.Add(1)
		writeAggregatorResponse(t, w, []hazardEvent{testHazardEvent()}, 1)
	})

	handler, signingSecret := newTestClientAPI(t, aggregator)
	token := signTestAccessToken(t, signingSecret, scopeSummary, jwtIssuer, time.Now().Add(time.Minute))
	response := performClientRequest(handler, "/api/v1/hazards?fields=all", token)
	if response.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d: %s", response.Code, response.Body.String())
	}
	if aggregatorCalls.Load() != 0 {
		t.Fatal("request reached aggregator before the scope check")
	}
}

func TestAllScopeReceivesRawFieldsAndForwardsFilters(t *testing.T) {
	var receivedQuery url.Values
	aggregator := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.URL.Query()
		writeAggregatorResponse(t, w, []hazardEvent{testHazardEvent()}, 1)
	})

	handler, signingSecret := newTestClientAPI(t, aggregator)
	token := signTestAccessToken(t, signingSecret, scopeAll, jwtIssuer, time.Now().Add(time.Minute))
	target := "/api/v1/hazards?fields=all&type=VOLCANIC&severity=AWAS&since=2026-09-28T00%3A00%3A00Z&limit=25&offset=50"
	response := performClientRequest(handler, target, token)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", response.Code, response.Body.String())
	}
	for key, expected := range map[string]string{
		"type": "VOLCANIC", "severity": "AWAS", "since": "2026-09-28T00:00:00Z", "limit": "25", "offset": "50",
	} {
		if receivedQuery.Get(key) != expected {
			t.Fatalf("query %q: expected %q, got %q", key, expected, receivedQuery.Get(key))
		}
	}

	var payload struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	for _, requiredField := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
		if _, exists := payload.Data[0][requiredField]; !exists {
			t.Fatalf("raw response is missing field %q", requiredField)
		}
	}
}

func TestJWTValidation(t *testing.T) {
	aggregator := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeAggregatorResponse(t, w, nil, 0)
	})
	handler, signingSecret := newTestClientAPI(t, aggregator)

	tests := []struct {
		name       string
		token      func() string
		wantStatus int
	}{
		{name: "missing token", token: func() string { return "" }, wantStatus: http.StatusUnauthorized},
		{
			name: "invalid signature",
			token: func() string {
				return signTestAccessToken(t, testClientSecret(t), scopeSummary, jwtIssuer, time.Now().Add(time.Minute))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "expired token",
			token: func() string {
				return signTestAccessToken(t, signingSecret, scopeSummary, jwtIssuer, time.Now().Add(-time.Minute))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong issuer",
			token: func() string {
				return signTestAccessToken(t, signingSecret, scopeSummary, "unexpected-issuer", time.Now().Add(time.Minute))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "unknown scope",
			token: func() string {
				return signTestAccessToken(t, signingSecret, "hazards:write", jwtIssuer, time.Now().Add(time.Minute))
			},
			wantStatus: http.StatusForbidden,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := performClientRequest(handler, "/api/v1/hazards", test.token())
			if response.Code != test.wantStatus {
				t.Fatalf("expected status %d, got %d: %s", test.wantStatus, response.Code, response.Body.String())
			}
		})
	}
}

func TestGetHazardByID(t *testing.T) {
	aggregator := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeAggregatorResponse(t, w, []hazardEvent{testHazardEvent()}, 1)
	})
	handler, signingSecret := newTestClientAPI(t, aggregator)
	token := signTestAccessToken(t, signingSecret, scopeSummary, jwtIssuer, time.Now().Add(time.Minute))

	response := performClientRequest(handler, "/api/v1/hazards/hazard-test-1?fields=summary", token)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload["hazard_id"] != "hazard-test-1" {
		t.Fatalf("unexpected hazard: %+v", payload)
	}
	if _, exists := payload["attributes"]; exists {
		t.Fatal("summary by-ID response leaked attributes")
	}
}

func TestAggregatorFailureReturnsBadGateway(t *testing.T) {
	aggregator := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "UNAVAILABLE", Message: "aggregator unavailable"})
	})
	handler, signingSecret := newTestClientAPI(t, aggregator)
	token := signTestAccessToken(t, signingSecret, scopeAll, jwtIssuer, time.Now().Add(time.Minute))

	response := performClientRequest(handler, "/api/v1/hazards?fields=all", token)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("expected status 502, got %d: %s", response.Code, response.Body.String())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func newTestClientAPI(t *testing.T, aggregator http.Handler) (http.Handler, string) {
	t.Helper()
	signingSecret := testClientSecret(t)
	handler := newAPIHandler(signingSecret, "http://aggregator.test")
	handler.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response := httptest.NewRecorder()
		aggregator.ServeHTTP(response, request)
		return response.Result(), nil
	})}
	return handler.routes(), signingSecret
}

func signTestAccessToken(t *testing.T, secret, scope, tokenIssuer string, expiresAt time.Time) string {
	t.Helper()
	now := time.Now().UTC()
	claims := accessClaims{
		Role:  "test-role",
		Scope: scope,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    tokenIssuer,
			Subject:   "test-client",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign test token: %v", err)
	}
	return token
}

func performClientRequest(handler http.Handler, target, token string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, target, nil)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func writeAggregatorResponse(t *testing.T, w http.ResponseWriter, events []hazardEvent, total int) {
	t.Helper()
	if events == nil {
		events = []hazardEvent{}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(aggregatorResponse{
		Data: events,
		Meta: aggregatorMeta{Total: total},
	}); err != nil {
		t.Errorf("encode aggregator response: %v", err)
	}
}

func testHazardEvent() hazardEvent {
	latitude := -6.2
	longitude := 106.8
	return hazardEvent{
		HazardID:    "hazard-test-1",
		Source:      "PVMBG",
		SourceRefID: "source-test-1",
		HazardType:  "VOLCANIC",
		Severity:    "AWAS",
		AreaName:    "Test Area",
		Latitude:    &latitude,
		Longitude:   &longitude,
		Attributes:  map[string]any{"internal_note": "test-only"},
		OccurredAt:  "2026-09-28T00:00:00Z",
		IngestedAt:  "2026-09-28T00:00:01Z",
	}
}

func testClientSecret(t *testing.T) string {
	t.Helper()
	value, err := newUUID()
	if err != nil {
		t.Fatalf("generate test signing secret: %v", err)
	}
	return strings.ReplaceAll(value, "-", "")
}
