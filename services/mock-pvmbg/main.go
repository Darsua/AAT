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
)

type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	mux := http.NewServeMux()

	// Health check endpoint (for container readiness & orchestration)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(HealthResponse{
			Status:  "UP",
			Service: "mock-pvmbg",
		})
	})

	// NOTE (Aldoy / Member A):
	// Implement mock endpoints here (GET /volcanic-reports, POST /admin/schema-version, POST /admin/outage).
	// Refer to services/mock-pvmbg/README.md and contracts/pvmbg.yaml for specifications.

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
		log.Printf("[mock-pvmbg] Server listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[mock-pvmbg] Server error: %v", err)
		}
	}()

	<-stop
	log.Println("[mock-pvmbg] Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("[mock-pvmbg] Forced shutdown: %v", err)
	} else {
		log.Println("[mock-pvmbg] Server stopped cleanly")
	}
}
