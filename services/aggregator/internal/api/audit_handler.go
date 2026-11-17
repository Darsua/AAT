package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/logger"
	"bnpb/aggregator/internal/repository"
)

// AuditStore defines repository operations backing the internal Appendix A Audit API.
type AuditStore interface {
	QueryAuditEvents(ctx context.Context, filter repository.AuditEventsFilter) (repository.AuditEventsResult, error)
	ComputeAuditChecksum(ctx context.Context, source *domain.Source, allSourcesLabel string) (repository.AuditChecksumResult, error)
	LocateHazard(ctx context.Context, hazardID string) (repository.AuditLocationResult, error)
	QueryAuditUnprocessed(ctx context.Context, filter repository.AuditUnprocessedFilter) (repository.AuditUnprocessedResult, error)
}

// AuditErrorDetail describes an Appendix A structured error object.
type AuditErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AuditErrorResponse wraps AuditErrorDetail under the top-level "error" key.
type AuditErrorResponse struct {
	Error AuditErrorDetail `json:"error"`
}

// AuditHandler serves the protected /internal/audit/* endpoints defined in contracts/audit_api.yaml.
type AuditHandler struct {
	store           AuditStore
	auditToken      string
	allSourcesLabel string
}

// NewAuditHandler constructs an AuditHandler requiring Bearer token authentication.
func NewAuditHandler(store AuditStore, auditToken, allSourcesLabel string) *AuditHandler {
	if strings.TrimSpace(allSourcesLabel) == "" {
		allSourcesLabel = "ALL"
	}
	return &AuditHandler{
		store:           store,
		auditToken:      auditToken,
		allSourcesLabel: allSourcesLabel,
	}
}

// ServeHTTP authenticates and dispatches requests to /internal/audit/* endpoints.
func (h *AuditHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if !h.authorize(r.Header.Get("Authorization")) {
		writeAuditError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
		return
	}

	if r.Method != http.MethodGet {
		writeAuditError(w, http.StatusMethodNotAllowed, "method_not_allowed", fmt.Sprintf("HTTP method %s is not supported", r.Method))
		return
	}

	switch r.URL.Path {
	case "/internal/audit/events":
		h.handleEvents(w, r)
	case "/internal/audit/checksum":
		h.handleChecksum(w, r)
	case "/internal/audit/location":
		h.handleLocation(w, r)
	case "/internal/audit/unprocessed":
		h.handleUnprocessed(w, r)
	default:
		writeAuditError(w, http.StatusNotFound, "not_found", "audit endpoint not found")
	}
}

func (h *AuditHandler) authorize(header string) bool {
	if h.auditToken == "" || header == "" {
		return false
	}
	scheme, token, found := strings.Cut(strings.TrimSpace(header), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	token = strings.TrimSpace(token)
	if len(token) == 0 || len(token) != len(h.auditToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(h.auditToken)) == 1
}

func (h *AuditHandler) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	source, ok := parseOptionalSource(w, q.Get("source"))
	if !ok {
		return
	}

	var since *time.Time
	if sinceStr := strings.TrimSpace(q.Get("since")); sinceStr != "" {
		t, err := time.Parse(time.RFC3339Nano, sinceStr)
		_, offset := t.Zone()
		if err != nil || offset != 0 {
			writeAuditError(w, http.StatusBadRequest, "bad_request", "invalid query parameter 'since': must be ISO 8601 UTC")
			return
		}
		utc := t.UTC()
		since = &utc
	}

	limit, ok := parseAuditLimit(w, q.Get("limit"))
	if !ok {
		return
	}

	res, err := h.store.QueryAuditEvents(r.Context(), repository.AuditEventsFilter{
		Source: source,
		Since:  since,
		Limit:  limit,
		Cursor: q.Get("cursor"),
	})
	if err != nil {
		h.handleStoreError(w, r, err, "events")
		return
	}
	if res.Items == nil {
		res.Items = []domain.HazardEvent{}
	}
	if res.UnavailableUnits == nil {
		res.UnavailableUnits = []string{}
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(res)
}

func (h *AuditHandler) handleChecksum(w http.ResponseWriter, r *http.Request) {
	source, ok := parseOptionalSource(w, r.URL.Query().Get("source"))
	if !ok {
		return
	}

	res, err := h.store.ComputeAuditChecksum(r.Context(), source, h.allSourcesLabel)
	if err != nil {
		h.handleStoreError(w, r, err, "checksum")
		return
	}
	if res.UnavailableUnits == nil {
		res.UnavailableUnits = []string{}
	}
	res.AsOf = res.AsOf.UTC()

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(res)
}

func (h *AuditHandler) handleLocation(w http.ResponseWriter, r *http.Request) {
	hazardID := strings.TrimSpace(r.URL.Query().Get("hazard_id"))
	if hazardID == "" {
		writeAuditError(w, http.StatusBadRequest, "bad_request", "missing required query parameter 'hazard_id'")
		return
	}

	res, err := h.store.LocateHazard(r.Context(), hazardID)
	if err != nil {
		if errors.Is(err, repository.ErrHazardNotFound) {
			writeAuditError(w, http.StatusNotFound, "not_found", "hazard_id tidak ditemukan")
			return
		}
		h.handleStoreError(w, r, err, "location")
		return
	}
	if res.Locations == nil {
		res.Locations = []domain.StorageLocation{}
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(res)
}

func (h *AuditHandler) handleUnprocessed(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	source, ok := parseOptionalSource(w, q.Get("source"))
	if !ok {
		return
	}
	limit, ok := parseAuditLimit(w, q.Get("limit"))
	if !ok {
		return
	}

	res, err := h.store.QueryAuditUnprocessed(r.Context(), repository.AuditUnprocessedFilter{
		Source: source,
		Limit:  limit,
		Cursor: q.Get("cursor"),
	})
	if err != nil {
		h.handleStoreError(w, r, err, "unprocessed")
		return
	}
	if res.Items == nil {
		res.Items = []domain.UnprocessedMessage{}
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(res)
}

func (h *AuditHandler) handleStoreError(w http.ResponseWriter, r *http.Request, err error, endpoint string) {
	switch {
	case errors.Is(err, repository.ErrInvalidCursor):
		writeAuditError(w, http.StatusBadRequest, "bad_request", "invalid query parameter 'cursor'")
	case errors.Is(err, repository.ErrAllStorageUnavailable):
		writeAuditError(w, http.StatusServiceUnavailable, "unavailable", "cluster penyimpanan tidak merespons")
	default:
		log := logger.FromContext(r.Context())
		log.Error("audit query failed", "endpoint", endpoint, "error", err)
		writeAuditError(w, http.StatusInternalServerError, "internal_error", "failed to execute audit query")
	}
}

func parseOptionalSource(w http.ResponseWriter, raw string) (*domain.Source, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, true
	}
	src := domain.Source(trimmed)
	if !domain.IsValidSource(src) {
		writeAuditError(w, http.StatusBadRequest, "bad_request", "invalid source filter")
		return nil, false
	}
	return &src, true
}

func parseAuditLimit(w http.ResponseWriter, raw string) (int, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 100, true
	}
	lim, err := strconv.Atoi(trimmed)
	if err != nil || lim < 1 || lim > 1000 {
		writeAuditError(w, http.StatusBadRequest, "bad_request", "invalid query parameter 'limit': must be between 1 and 1000")
		return 0, false
	}
	return lim, true
}

func writeAuditError(w http.ResponseWriter, status int, code, message string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(AuditErrorResponse{
		Error: AuditErrorDetail{
			Code:    code,
			Message: message,
		},
	})
}
