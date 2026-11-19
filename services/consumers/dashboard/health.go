package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"
)

// healthHandler reports that the consumer process is running.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP", "service": "dashboard-consumer"})
}

func newHTTPHandler(dashboard *Dashboard) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	if dashboard != nil {
		mux.HandleFunc("GET /api/analytics", analyticsHandler(dashboard))
		mux.HandleFunc("GET /dashboard/api/analytics", analyticsHandler(dashboard))
		mux.HandleFunc("GET /", dashboardPageHandler)
		mux.HandleFunc("GET /dashboard", dashboardPageHandler)
		mux.HandleFunc("GET /dashboard/", dashboardPageHandler)
	}
	return mux
}

// startHealthServer serves GET /health until ctx is cancelled.
func startHealthServer(ctx context.Context, port string) {
	startDashboardServer(ctx, port, nil)
}

// startDashboardServer serves health, analytics JSON API, and the web analytics UI until ctx is cancelled.
func startDashboardServer(ctx context.Context, port string, dashboard *Dashboard) {
	server := &http.Server{Addr: ":" + port, Handler: newHTTPHandler(dashboard), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[dashboard] HTTP server stopped: %v", err)
		}
	}()
	log.Printf("[dashboard] HTTP server listening on :%s", port)
}
