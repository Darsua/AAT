package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bnpb/aggregator/internal/domain"
)

type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type Meta struct {
	Total      int     `json:"total"`
	Degraded   bool    `json:"degraded"`
	StaleSince *string `json:"stale_since"`
}

type HazardListResponse struct {
	Data []domain.HazardEvent `json:"data"`
	Meta Meta                 `json:"meta"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8083"
	}

	mux := http.NewServeMux()

	// GET /health
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(HealthResponse{
			Status:  "UP",
			Service: "aggregator",
		})
	})

	// GET /internal/hazards stub
	mux.HandleFunc("GET /internal/hazards", func(w http.ResponseWriter, r *http.Request) {
		// Stub response returning canonical hazard list matching aggregator_internal.yaml
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := HazardListResponse{
			Data: []domain.HazardEvent{},
			Meta: Meta{
				Total:      0,
				Degraded:   false,
				StaleSince: nil,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[aggregator] Internal service listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[aggregator] Server error: %v", err)
		}
	}()

	<-stop
	log.Println("[aggregator] Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("[aggregator] Forced shutdown: %v", err)
	} else {
		log.Println("[aggregator] Server stopped cleanly")
	}
}
