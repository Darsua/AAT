package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/logger"
	"bnpb/aggregator/internal/repository"
)

// HazardQuerier defines the read operations needed by the hazards API handler.
type HazardQuerier interface {
	QueryHazards(ctx context.Context, filter repository.HazardFilter) ([]domain.HazardEvent, int, error)
}

// HazardListResponse represents the JSON response for GET /internal/hazards.
type HazardListResponse struct {
	Data []domain.HazardEvent `json:"data"`
	Meta HazardMeta           `json:"meta"`
}

// HazardMeta provides pagination and circuit degradation status.
type HazardMeta struct {
	Total      int        `json:"total"`
	Degraded   bool       `json:"degraded"`
	StaleSince *time.Time `json:"stale_since"`
}

// ErrorResponse represents standard error payloads.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// HazardHandler handles HTTP requests for hazard retrieval.
type HazardHandler struct {
	repo HazardQuerier
}

// NewHazardHandler creates a new HazardHandler.
func NewHazardHandler(repo HazardQuerier) *HazardHandler {
	return &HazardHandler{repo: repo}
}

// ServeHTTP parses query filters, fetches hazard records, and returns the response.
func (h *HazardHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "METHOD_NOT_ALLOWED",
			Message: fmt.Sprintf("HTTP method %s is not supported", r.Method),
		})
		return
	}

	filter := repository.HazardFilter{
		Limit:  50,
		Offset: 0,
	}

	q := r.URL.Query()

	// Parse 'since'
	if sinceStr := q.Get("since"); sinceStr != "" {
		t, err := time.Parse(time.RFC3339, sinceStr)
		_, offset := t.Zone()
		if err != nil || offset != 0 {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "BAD_REQUEST",
				Message: "Invalid query parameter 'since': must be ISO 8601 UTC RFC3339 format",
			})
			return
		}
		utc := t.UTC()
		filter.Since = &utc
	}

	// Parse 'type'
	if typeStr := q.Get("type"); typeStr != "" {
		ht := domain.HazardType(typeStr)
		if ht != domain.HazardTypeSeismic && ht != domain.HazardTypeVolcanic {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "BAD_REQUEST",
				Message: "Invalid query parameter 'type': must be SEISMIC or VOLCANIC",
			})
			return
		}
		filter.Type = &ht
	}

	// Parse 'severity'
	if sevStr := q.Get("severity"); sevStr != "" {
		sev := domain.Severity(sevStr)
		switch sev {
		case domain.SeverityNormal, domain.SeverityWaspada, domain.SeveritySiaga, domain.SeverityAwas:
			filter.Severity = &sev
		default:
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "BAD_REQUEST",
				Message: "Invalid query parameter 'severity': must be NORMAL, WASPADA, SIAGA, or AWAS",
			})
			return
		}
	}

	// Parse 'limit'
	if limitStr := q.Get("limit"); limitStr != "" {
		lim, err := strconv.Atoi(limitStr)
		if err != nil || lim < 1 || lim > 500 {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "BAD_REQUEST",
				Message: "Invalid query parameter 'limit': must be an integer between 1 and 500",
			})
			return
		}
		filter.Limit = lim
	}

	// Parse 'offset'
	if offsetStr := q.Get("offset"); offsetStr != "" {
		off, err := strconv.Atoi(offsetStr)
		if err != nil || off < 0 {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "BAD_REQUEST",
				Message: "Invalid query parameter 'offset': must be a non-negative integer",
			})
			return
		}
		filter.Offset = off
	}

	events, total, err := h.repo.QueryHazards(r.Context(), filter)
	if err != nil {
		log := logger.FromContext(r.Context())
		log.Error("failed to query hazards", "error", err)

		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "INTERNAL_ERROR",
			Message: "An unexpected error occurred while querying hazard events",
		})
		return
	}

	resp := HazardListResponse{
		Data: events,
		Meta: HazardMeta{
			Total:      total,
			Degraded:   false,
			StaleSince: nil,
		},
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
