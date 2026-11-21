package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestQoSLimiter_ShedsMediaUnderOverloadWhileServingFieldTeam(t *testing.T) {
	const secret = "test-signing-secret"
	gate := make(chan struct{})
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-gate
		writeAggregatorResponse(t, w, []hazardEvent{testHazardEvent()}, 1)
	})

	srv := httptest.NewServer(upstream)
	defer srv.Close()

	h := newAPIHandlerWithService(secret, NewRemoteAggregatorClient(srv.URL))
	h.qos = NewQoSLimiter(5, 1) // Shed Media when >1 concurrent request; Field Team allowed up to 5.
	router := h.routes()

	fieldToken := signTestAccessToken(t, secret, scopeAll, jwtIssuer, time.Now().Add(time.Minute))
	mediaToken := signTestAccessToken(t, secret, scopeSummary, jwtIssuer, time.Now().Add(time.Minute))

	// Hold 1 in-flight Field Team request at the upstream gate.
	var wg sync.WaitGroup
	wg.Add(1)
	var firstCode int
	go func() {
		defer wg.Done()
		rec := performClientRequest(router, "/api/v1/hazards?fields=all", fieldToken)
		firstCode = rec.Code
	}()

	// Wait briefly for the first request to enter the gate.
	deadline := time.Now().Add(time.Second)
	for h.qos.inFlight.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// Media request during overload (cur=2 > mediaShedThreshold=1) must get 429.
	mediaRec := performClientRequest(router, "/api/v1/hazards?fields=summary", mediaToken)
	if mediaRec.Code != http.StatusTooManyRequests {
		close(gate)
		wg.Wait()
		t.Fatalf("expected 429 TOO_MANY_REQUESTS for Media under overload, got %d: %s", mediaRec.Code, mediaRec.Body.String())
	}

	// Release gate and verify Field Team request succeeded.
	close(gate)
	wg.Wait()
	if firstCode != http.StatusOK {
		t.Fatalf("expected Field Team request to succeed with 200, got %d", firstCode)
	}

	stats := h.qos.Stats()
	if stats["media_shed"] != 1 || stats["field_accepted"] < 1 {
		t.Fatalf("unexpected QoS stats: %+v", stats)
	}
}

func TestQoSLimiter_MediaFloodSheddingDoesNotConsumeFieldCapacity(t *testing.T) {
	const secret = "test-signing-secret"
	gate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-gate
		writeAggregatorResponse(t, w, []hazardEvent{testHazardEvent()}, 1)
	}))
	defer srv.Close()

	h := newAPIHandlerWithService(secret, NewRemoteAggregatorClient(srv.URL))
	h.qos = NewQoSLimiter(2, 0) // Max 2 concurrent requests; shed Media when cur > 0 -> 1+ already in flight or shed immediately when >0.
	h.qos.mediaShedThreshold = 0
	router := h.routes()

	fieldToken := signTestAccessToken(t, secret, scopeAll, jwtIssuer, time.Now().Add(time.Minute))
	mediaToken := signTestAccessToken(t, secret, scopeSummary, jwtIssuer, time.Now().Add(time.Minute))

	// Hold 1 accepted Field Team request inside the upstream gate.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		performClientRequest(router, "/api/v1/hazards?fields=all", fieldToken)
	}()
	deadline := time.Now().Add(time.Second)
	for h.qos.inFlight.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// Shed 5 Media requests; verify inFlight returns to 1 (not stuck or leaking slots past maxConcurrent=2).
	for i := 0; i < 5; i++ {
		rec := performClientRequest(router, "/api/v1/hazards?fields=summary", mediaToken)
		if rec.Code != http.StatusTooManyRequests {
			close(gate)
			wg.Wait()
			t.Fatalf("expected 429 for Media flood request %d, got %d", i, rec.Code)
		}
	}
	if got := h.qos.inFlight.Load(); got != 1 {
		close(gate)
		wg.Wait()
		t.Fatalf("expected inFlight=1 after shed Media 429s, got %d", got)
	}

	// Second Field Team request must still succeed while 1st holds slot and Media was shed.
	wg.Add(1)
	var secondCode int
	go func() {
		defer wg.Done()
		secondCode = performClientRequest(router, "/api/v1/hazards?fields=all", fieldToken).Code
	}()
	time.Sleep(25 * time.Millisecond)
	close(gate)
	wg.Wait()
	if secondCode != http.StatusOK {
		t.Fatalf("expected second Field Team request to succeed with 200, got %d", secondCode)
	}
}
