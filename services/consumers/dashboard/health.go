package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"time"
)

// healthHandler reports that the consumer process is running.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP", "service": "dashboard-consumer"})
}

func newHTTPHandler(dashboard *Dashboard) http.Handler {
	return newHTTPHandlerWithTelemetry(dashboard, nil)
}

func newHTTPHandlerWithTelemetry(dashboard *Dashboard, reg *Registry) http.Handler {
	return newHTTPHandlerWithObservability(dashboard, reg, nil, nil)
}

func newHTTPHandlerWithObservability(dashboard *Dashboard, reg *Registry, tracer *Tracer, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	if reg != nil {
		mux.HandleFunc("GET /metrics", reg.Handler())
	}
	if dashboard != nil {
		mux.HandleFunc("GET /api/analytics", analyticsHandler(dashboard))
		mux.HandleFunc("GET /dashboard/api/analytics", analyticsHandler(dashboard))
		mux.HandleFunc("GET /", dashboardPageHandler)
		mux.HandleFunc("GET /dashboard", dashboardPageHandler)
		mux.HandleFunc("GET /dashboard/", dashboardPageHandler)
	}
	if reg != nil && tracer != nil && logger != nil {
		return correlationMiddleware(observabilityMiddleware("dashboard-consumer", reg, tracer, logger, mux))
	}
	return mux
}

// startHealthServer serves GET /health until ctx is cancelled.
func startHealthServer(ctx context.Context, port string) {
	startDashboardServerWithTelemetry(ctx, port, nil, nil)
}

// startDashboardServer serves health, analytics JSON API, and the web analytics UI until ctx is cancelled.
func startDashboardServer(ctx context.Context, port string, dashboard *Dashboard) {
	startDashboardServerWithTelemetry(ctx, port, dashboard, nil)
}

// startDashboardServerWithTelemetry serves health, /metrics, analytics JSON API, and the web analytics UI until ctx is cancelled.
func startDashboardServerWithTelemetry(ctx context.Context, port string, dashboard *Dashboard, reg *Registry) {
	startDashboardServerWithObservability(ctx, port, dashboard, reg, nil, nil)
}

func startDashboardServerWithObservability(ctx context.Context, port string, dashboard *Dashboard, reg *Registry, tracer *Tracer, logger *slog.Logger) {
	server := &http.Server{Addr: ":" + port, Handler: newHTTPHandlerWithObservability(dashboard, reg, tracer, logger), ReadHeaderTimeout: 5 * time.Second}

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
