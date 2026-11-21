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
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP", "service": "notifier-consumer"})
}

// startHealthServer serves GET /health until ctx is cancelled.
func startHealthServer(ctx context.Context, port string) {
	startHTTPServer(ctx, port, nil, nil)
}

// startHTTPServer serves GET /health, GET /metrics, and GET /notifications until ctx is cancelled.
func startHTTPServer(ctx context.Context, port string, reg *Registry, notifier *Notifier) {
	startHTTPServerWithObservability(ctx, port, reg, nil, nil, notifier)
}

func startHTTPServerWithObservability(ctx context.Context, port string, reg *Registry, tracer *Tracer, logger *slog.Logger, notifier *Notifier) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	if reg != nil {
		mux.HandleFunc("GET /metrics", reg.Handler())
	}
	mux.HandleFunc("GET /notifications", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var items []NotificationDelivery
		if notifier != nil {
			items = notifier.Deliveries()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count": len(items),
			"items": items,
		})
	})
	var handler http.Handler = mux
	if reg != nil && tracer != nil && logger != nil {
		handler = correlationMiddleware(observabilityMiddleware("notifier-consumer", reg, tracer, logger, mux))
	}
	server := &http.Server{Addr: ":" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

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
