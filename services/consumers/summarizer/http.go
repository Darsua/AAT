package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"
)

func buildHTTPMux(engine *SummarizerEngine) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		snap := engine.Snapshot(false)
		writeJSON(w, http.StatusOK, map[string]string{
			"status":        "ok",
			"service":       "summarizer-consumer",
			"circuit_state": snap.CircuitState,
		})
	})
	mux.HandleFunc("GET /summaries", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, engine.Snapshot(true))
	})
	mux.HandleFunc("GET /summaries/latest", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, engine.Snapshot(false))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		snap := engine.Snapshot(false)
		circuitOpen := 0
		if snap.CircuitState == "open" {
			circuitOpen = 1
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "# HELP bnpb_summarizer_events_consumed_total Total canonical events processed by summarizer.\n")
		fmt.Fprintf(w, "# TYPE bnpb_summarizer_events_consumed_total counter\n")
		fmt.Fprintf(w, "bnpb_summarizer_events_consumed_total %d\n", snap.Metrics.EventsConsumedTotal)
		fmt.Fprintf(w, "# HELP bnpb_summarizer_summaries_generated_total Summaries generated partitioned by mode.\n")
		fmt.Fprintf(w, "# TYPE bnpb_summarizer_summaries_generated_total counter\n")
		fmt.Fprintf(w, "bnpb_summarizer_summaries_generated_total{mode=\"llm\"} %d\n", snap.Metrics.LLMSuccessTotal)
		fmt.Fprintf(w, "bnpb_summarizer_summaries_generated_total{mode=\"extractive-fallback\"} %d\n", snap.Metrics.FallbackTotal)
		fmt.Fprintf(w, "# HELP bnpb_summarizer_coalesced_total Trigger signals coalesced under load.\n")
		fmt.Fprintf(w, "# TYPE bnpb_summarizer_coalesced_total counter\n")
		fmt.Fprintf(w, "bnpb_summarizer_coalesced_total %d\n", snap.Metrics.CoalescedTotal)
		fmt.Fprintf(w, "# HELP bnpb_summarizer_quota_exhausted_total Summaries fallen back due to quota exhaustion.\n")
		fmt.Fprintf(w, "# TYPE bnpb_summarizer_quota_exhausted_total counter\n")
		fmt.Fprintf(w, "bnpb_summarizer_quota_exhausted_total %d\n", snap.Metrics.QuotaExhaustedTotal)
		fmt.Fprintf(w, "# HELP bnpb_summarizer_circuit_open Whether AI upstream circuit breaker is open (1=open).\n")
		fmt.Fprintf(w, "# TYPE bnpb_summarizer_circuit_open gauge\n")
		fmt.Fprintf(w, "bnpb_summarizer_circuit_open %d\n", circuitOpen)
		fmt.Fprintf(w, "# HELP bnpb_summarizer_last_latency_ms Latency of last summary generation in milliseconds.\n")
		fmt.Fprintf(w, "# TYPE bnpb_summarizer_last_latency_ms gauge\n")
		fmt.Fprintf(w, "bnpb_summarizer_last_latency_ms %d\n", snap.Metrics.LastLatencyMS)
		fmt.Fprintf(w, "# HELP bnpb_summarizer_window_events Current bounded event window count.\n")
		fmt.Fprintf(w, "# TYPE bnpb_summarizer_window_events gauge\n")
		fmt.Fprintf(w, "bnpb_summarizer_window_events %d\n", snap.Metrics.WindowEvents)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func startHTTPServer(ctx context.Context, port string, engine *SummarizerEngine) {
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           buildHTTPMux(engine),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[summarizer] http server stopped: %v", err)
		}
	}()
	log.Printf("[summarizer] http server listening on :%s", port)
}
