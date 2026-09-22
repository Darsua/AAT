package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Pinger defines the interface for checking database connectivity.
type Pinger interface {
	Ping(ctx context.Context) error
}

// BrokerPinger defines the interface for checking message broker connectivity.
type BrokerPinger interface {
	PingBroker(ctx context.Context) error
}

// HealthStatus represents the response payload conforming to contracts/aggregator_internal.yaml.
type HealthStatus struct {
	Status   string `json:"status"`
	Service  string `json:"service"`
	Database string `json:"database"`
	Broker   string `json:"broker"`
}

// HealthHandler handles the GET /health probe.
type HealthHandler struct {
	db     Pinger
	broker BrokerPinger
}

// NewHealthHandler creates a new HealthHandler.
func NewHealthHandler(db Pinger, broker BrokerPinger) *HealthHandler {
	return &HealthHandler{
		db:     db,
		broker: broker,
	}
}

// ServeHTTP checks database and broker connectivity and returns 200 UP or 503 DOWN.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	dbStatus := "UP"
	brokerStatus := "UP"
	overallStatus := "UP"
	httpStatusCode := http.StatusOK

	if h.db != nil {
		if err := h.db.Ping(ctx); err != nil {
			dbStatus = "DOWN"
			overallStatus = "DOWN"
			httpStatusCode = http.StatusServiceUnavailable
		}
	}

	if h.broker != nil {
		if err := h.broker.PingBroker(ctx); err != nil {
			brokerStatus = "DOWN"
			overallStatus = "DOWN"
			httpStatusCode = http.StatusServiceUnavailable
		}
	}

	resp := HealthStatus{
		Status:   overallStatus,
		Service:  "aggregator",
		Database: dbStatus,
		Broker:   brokerStatus,
	}

	w.WriteHeader(httpStatusCode)
	_ = json.NewEncoder(w).Encode(resp)
}
