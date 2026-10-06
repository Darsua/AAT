package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	jwtIssuer       = "bnpb-auth-service"
	scopeSummary    = "hazards:read:summary"
	scopeAll        = "hazards:read:all"
	correlationHead = "X-Correlation-ID" // Propagated for request tracing.
)

// config contains runtime settings.
type config struct {
	Port          string
	JWTSecret     string
	AggregatorURL string
}

type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type accessClaims struct {
	Role  string `json:"role"`
	Scope string `json:"scope"`
	jwt.RegisteredClaims
}

// Hazard response models.
type hazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    *float64       `json:"latitude"`
	Longitude   *float64       `json:"longitude"`
	Attributes  map[string]any `json:"attributes"`
	OccurredAt  string         `json:"occurred_at"`
	IngestedAt  string         `json:"ingested_at"`
}

type hazardSummary struct {
	HazardID   string `json:"hazard_id"`
	Source     string `json:"source"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
	OccurredAt string `json:"occurred_at"`
	IngestedAt string `json:"ingested_at"`
}

// aggregatorResponse matches the Aggregator API payload.
type aggregatorResponse struct {
	Data []hazardEvent  `json:"data"`
	Meta aggregatorMeta `json:"meta"`
}

type aggregatorMeta struct {
	Total      int     `json:"total"`
	Degraded   bool    `json:"degraded"`
	StaleSince *string `json:"stale_since"`
}

// publicMeta is returned to API clients.
type publicMeta struct {
	Total      int     `json:"total"`
	Limit      int     `json:"limit"`
	Offset     int     `json:"offset"`
	Degraded   bool    `json:"degraded"`
	StaleSince *string `json:"stale_since"`
}

type publicListResponse struct {
	Data any        `json:"data"`
	Meta publicMeta `json:"meta"`
}

// queryOptions contains validated projection and pagination parameters.
type queryOptions struct {
	Fields string

	// Forwarded to the Aggregator.
	Limit      int
	Offset     int
	Downstream url.Values
}

// contextKey prevents collisions with other context values.
type contextKey string

const claimsContextKey contextKey = "access-claims"

// apiHandler serves authenticated public endpoints.
type apiHandler struct {
	jwtSecret     []byte
	aggregatorURL string
	httpClient    *http.Client
}

func newAPIHandler(jwtSecret, aggregatorURL string) *apiHandler {
	return &apiHandler{
		jwtSecret:     []byte(jwtSecret),
		aggregatorURL: strings.TrimRight(aggregatorURL, "/"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 50,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (h *apiHandler) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.Handle("GET /api/v1/hazards", h.requireJWT(http.HandlerFunc(h.listHazards)))
	mux.Handle("GET /api/v1/hazards/{id}", h.requireJWT(http.HandlerFunc(h.getHazardByID)))
	return correlationMiddleware(loggingMiddleware(mux))
}

func (h *apiHandler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "UP", Service: "client-api"})
}

func (h *apiHandler) requireJWT(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenString, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeJSON(w, http.StatusUnauthorized, errorResponse{
				Error: "UNAUTHORIZED", Message: "Missing or invalid Bearer token",
			})
			return
		}

		claims := &accessClaims{}
		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (any, error) {
			if token.Method != jwt.SigningMethodHS256 {
				return nil, fmt.Errorf("unexpected JWT signing method")
			}
			return h.jwtSecret, nil
		},
			jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
			jwt.WithIssuer(jwtIssuer),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
		)
		if err != nil || !token.Valid || claims.Subject == "" {
			writeJSON(w, http.StatusUnauthorized, errorResponse{
				Error: "UNAUTHORIZED", Message: "Missing or invalid Bearer token",
			})
			return
		}
		if claims.Scope != scopeSummary && claims.Scope != scopeAll {
			writeJSON(w, http.StatusForbidden, errorResponse{
				Error: "FORBIDDEN", Message: "Token does not contain a permitted hazard read scope",
			})
			return
		}

		ctx := context.WithValue(r.Context(), claimsContextKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *apiHandler) listHazards(w http.ResponseWriter, r *http.Request) {
	options, err := parseListQuery(r.URL.Query())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "BAD_REQUEST", Message: err.Error()})
		return
	}
	if !projectionAllowed(claimsFromContext(r.Context()), options.Fields) {
		writeForbidden(w)
		return
	}

	response, err := h.fetchHazards(r.Context(), options.Downstream, correlationID(r))
	if err != nil {
		handleUpstreamError(w, r, err)
		return
	}

	var data any = response.Data
	if options.Fields == "summary" {
		data = summarizeHazards(response.Data)
	}
	writeJSON(w, http.StatusOK, publicListResponse{
		Data: data,
		Meta: publicMeta{
			Total:      response.Meta.Total,
			Limit:      options.Limit,
			Offset:     options.Offset,
			Degraded:   response.Meta.Degraded,
			StaleSince: response.Meta.StaleSince,
		},
	})
}

func (h *apiHandler) getHazardByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "BAD_REQUEST", Message: "Hazard ID is required"})
		return
	}
	fields, err := parseFields(r.URL.Query().Get("fields"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "BAD_REQUEST", Message: err.Error()})
		return
	}
	if !projectionAllowed(claimsFromContext(r.Context()), fields) {
		writeForbidden(w)
		return
	}

	event, found, err := h.findHazardByID(r.Context(), id, correlationID(r))
	if err != nil {
		handleUpstreamError(w, r, err)
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "NOT_FOUND", Message: "Hazard event not found"})
		return
	}
	if fields == "summary" {
		writeJSON(w, http.StatusOK, summarizeHazard(event))
		return
	}
	writeJSON(w, http.StatusOK, event)
}

func (h *apiHandler) findHazardByID(ctx context.Context, id, correlationID string) (hazardEvent, bool, error) {
	const pageSize = 500 // Aggregator maximum page size.
	for offset := 0; ; {
		query := url.Values{
			"limit":  []string{strconv.Itoa(pageSize)},
			"offset": []string{strconv.Itoa(offset)},
		}
		response, err := h.fetchHazards(ctx, query, correlationID)
		if err != nil {
			return hazardEvent{}, false, err
		}
		for _, event := range response.Data {
			if event.HazardID == id {
				return event, true, nil
			}
		}
		if len(response.Data) == 0 || offset+len(response.Data) >= response.Meta.Total {
			return hazardEvent{}, false, nil
		}
		offset += len(response.Data)
	}
}

func (h *apiHandler) fetchHazards(ctx context.Context, query url.Values, correlationID string) (aggregatorResponse, error) {
	endpoint := h.aggregatorURL + "/internal/hazards"
	if encoded := query.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return aggregatorResponse{}, err
	}
	req.Header.Set(correlationHead, correlationID)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return aggregatorResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var payload errorResponse
		if err := decodeJSON(resp.Body, &payload); err != nil || payload.Error == "" {
			payload = errorResponse{Error: "BAD_GATEWAY", Message: "Aggregator returned an invalid response"}
		}
		return aggregatorResponse{}, upstreamHTTPError{Status: resp.StatusCode, Payload: payload}
	}

	var response aggregatorResponse
	if err := decodeJSON(resp.Body, &response); err != nil {
		return aggregatorResponse{}, fmt.Errorf("decode aggregator response: %w", err)
	}
	if response.Data == nil {
		response.Data = []hazardEvent{}
	}
	return response, nil
}

// upstreamHTTPError preserves errors returned by the Aggregator.
type upstreamHTTPError struct {
	Status  int
	Payload errorResponse
}

func (e upstreamHTTPError) Error() string {
	return fmt.Sprintf("aggregator returned HTTP %d", e.Status)
}

func handleUpstreamError(w http.ResponseWriter, r *http.Request, err error) {
	var responseError upstreamHTTPError
	if errors.As(err, &responseError) && responseError.Status == http.StatusBadRequest {
		writeJSON(w, http.StatusBadRequest, responseError.Payload)
		return
	}

	log.Printf("[client-api] correlation_id=%q upstream_error=%q", correlationID(r), err)
	writeJSON(w, http.StatusInternalServerError, errorResponse{
		Error: "INTERNAL_ERROR", Message: "An unexpected error occurred",
	})
}

func parseListQuery(query url.Values) (queryOptions, error) {
	fields, err := parseFields(query.Get("fields"))
	if err != nil {
		return queryOptions{}, err
	}
	options := queryOptions{Fields: fields, Limit: 50, Offset: 0, Downstream: make(url.Values)}

	if raw := query.Get("since"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'since': must be ISO 8601 UTC RFC3339 format")
		}
		_, offset := parsed.Zone()
		if offset != 0 {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'since': must be ISO 8601 UTC RFC3339 format")
		}
		options.Downstream.Set("since", raw)
	}
	if raw := query.Get("type"); raw != "" {
		if raw != "SEISMIC" && raw != "VOLCANIC" {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'type': must be SEISMIC or VOLCANIC")
		}
		options.Downstream.Set("type", raw)
	}
	if raw := query.Get("severity"); raw != "" {
		switch raw {
		case "NORMAL", "WASPADA", "SIAGA", "AWAS":
			options.Downstream.Set("severity", raw)
		default:
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'severity': must be NORMAL, WASPADA, SIAGA, or AWAS")
		}
	}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 500 {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'limit': must be an integer between 1 and 500")
		}
		options.Limit = limit
	}
	if raw := query.Get("offset"); raw != "" {
		offset, err := strconv.Atoi(raw)
		if err != nil || offset < 0 {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'offset': must be a non-negative integer")
		}
		options.Offset = offset
	}
	options.Downstream.Set("limit", strconv.Itoa(options.Limit))
	options.Downstream.Set("offset", strconv.Itoa(options.Offset))
	return options, nil
}

func parseFields(fields string) (string, error) {
	if fields == "" {
		return "summary", nil
	}
	if fields != "summary" && fields != "all" {
		return "", fmt.Errorf("Invalid fields query value. Allowed: summary, all")
	}
	return fields, nil
}

func projectionAllowed(claims *accessClaims, fields string) bool {
	return claims != nil && (fields != "all" || claims.Scope == scopeAll)
}

func claimsFromContext(ctx context.Context) *accessClaims {
	claims, _ := ctx.Value(claimsContextKey).(*accessClaims)
	return claims
}

func summarizeHazards(events []hazardEvent) []hazardSummary {
	summaries := make([]hazardSummary, 0, len(events))
	for _, event := range events {
		summaries = append(summaries, summarizeHazard(event))
	}
	return summaries
}

func summarizeHazard(event hazardEvent) hazardSummary {
	return hazardSummary{
		HazardID:   event.HazardID,
		Source:     event.Source,
		HazardType: event.HazardType,
		Severity:   event.Severity,
		AreaName:   event.AreaName,
		OccurredAt: event.OccurredAt,
		IngestedAt: event.IngestedAt,
	}
}

// bearerToken parses an Authorization: Bearer header.
func bearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

func writeForbidden(w http.ResponseWriter) {
	writeJSON(w, http.StatusForbidden, errorResponse{
		Error:   "FORBIDDEN",
		Message: "Scope 'hazards:read:all' required to access raw fields or fields=all",
	})
}

func correlationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get(correlationHead))
		if id == "" {
			var err error
			id, err = newUUID()
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, errorResponse{
					Error: "INTERNAL_ERROR", Message: "Unable to generate correlation ID",
				})
				return
			}
		}
		w.Header().Set(correlationHead, id)
		r.Header.Set(correlationHead, id)
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		log.Printf("[client-api] correlation_id=%q method=%q path=%q status=%d duration_ms=%d",
			correlationID(r), r.Method, r.URL.Path, status, time.Since(started).Milliseconds())
	})
}

// Request and JSON helpers.
func correlationID(r *http.Request) string {
	return r.Header.Get(correlationHead)
}

func newUUID() (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func decodeJSON(reader io.Reader, destination any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, 16<<20))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("response must contain one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("[client-api] failed to encode response: %v", err)
	}
}

// loadConfig reads and validates environment configuration.
func loadConfig() (config, error) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		return config{}, fmt.Errorf("required environment variable JWT_SECRET is not set")
	}
	aggregatorURL := os.Getenv("AGGREGATOR_URL")
	if aggregatorURL == "" {
		aggregatorURL = "http://aggregator:8083"
	}
	parsed, err := url.ParseRequestURI(aggregatorURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return config{}, fmt.Errorf("AGGREGATOR_URL must be a valid HTTP or HTTPS URL")
	}
	return config{Port: port, JWTSecret: jwtSecret, AggregatorURL: aggregatorURL}, nil
}

func main() {
	configuration, err := loadConfig()
	if err != nil {
		log.Fatalf("[client-api] Configuration error: %v", err)
	}

	handler := newAPIHandler(configuration.JWTSecret, configuration.AggregatorURL)
	server := &http.Server{
		Addr:         ":" + configuration.Port,
		Handler:      handler.routes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	go func() {
		log.Printf("[client-api] Public API gateway listening on port %s", configuration.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[client-api] Server error: %v", err)
		}
	}()

	<-stop
	log.Println("[client-api] Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("[client-api] Forced shutdown: %v", err)
	} else {
		log.Println("[client-api] Server stopped cleanly")
	}
}
