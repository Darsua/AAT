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
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP", "service": "notifier-consumer"})
}

// startHealthServer serves GET /health until ctx is cancelled.
func startHealthServer(ctx context.Context, port string) {
	startHTTPServer(ctx, port, nil)
}

// startHTTPServer serves GET /health and GET /metrics until ctx is cancelled.
func startHTTPServer(ctx context.Context, port string, reg *Registry) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	if reg != nil {
		mux.HandleFunc("GET /metrics", reg.Handler())
	}
	server := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[notifier] health server stopped: %v", err)
		}
	}()
	log.Printf("[notifier] health endpoint listening on :%s", port)
}
