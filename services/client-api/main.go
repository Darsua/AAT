package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
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
	Port              string
	JWTSecret         string
	AggregatorURL     string
	RedpandaBrokers   []string
	FieldReportsTopic string
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

// HazardResponse is the transport-independent result returned by HazardService.
type HazardResponse struct {
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

// QueryFilter contains transport-independent hazard query parameters.
type QueryFilter struct {
	HazardID   string
	Since      string
	HazardType string
	Severity   string
	Limit      int
	Offset     int
}

// queryOptions contains validated projection and downstream filter parameters.
type queryOptions struct {
	Fields string
	Filter QueryFilter
}

// contextKey prevents collisions with other context values.
type contextKey string

const (
	claimsContextKey      contextKey = "access-claims"
	correlationContextKey contextKey = "correlation-id"
)

// HazardService keeps HTTP handlers independent from the downstream transport.
type HazardService interface {
	GetHazards(context.Context, QueryFilter) (*HazardResponse, error)
}

// RemoteAggregatorClient retrieves hazards from the Aggregator over HTTP.
type RemoteAggregatorClient struct {
	baseURL    string
	httpClient *http.Client
	registry   *Registry
	tracer     *Tracer
	logger     *slog.Logger
}

func NewRemoteAggregatorClient(baseURL string) *RemoteAggregatorClient {
	return &RemoteAggregatorClient{
		baseURL: strings.TrimRight(baseURL, "/"),
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

// apiHandler serves authenticated public endpoints.
type apiHandler struct {
	jwtSecret     []byte
	hazardService HazardService
	reportStager  FieldReportStager
	idempotency   *idempotencyStore
	registry      *Registry
	tracer        *Tracer
	logger        *slog.Logger
	qos           *QoSLimiter
}

func newAPIHandler(jwtSecret, aggregatorURL string) *apiHandler {
	return newAPIHandlerWithDeps(jwtSecret, NewRemoteAggregatorClient(aggregatorURL), nil)
}

func newAPIHandlerWithService(jwtSecret string, hazardService HazardService) *apiHandler {
	return newAPIHandlerWithDeps(jwtSecret, hazardService, nil)
}

func newAPIHandlerWithDeps(jwtSecret string, hazardService HazardService, stager FieldReportStager) *apiHandler {
	logger := newJSONLogger("client-api")
	reg := newRegistry()
	tracer := newTracer("client-api", logger)
	if client, ok := hazardService.(*RemoteAggregatorClient); ok {
		if client.registry == nil {
			client.registry = reg
		}
		if client.tracer == nil {
			client.tracer = tracer
		}
		if client.logger == nil {
			client.logger = logger
		}
	}
	return &apiHandler{
		jwtSecret:     []byte(jwtSecret),
		hazardService: hazardService,
		reportStager:  stager,
		idempotency:   newIdempotencyStore(defaultIdempotencyTTL, defaultIdempotencyCap),
		registry:      reg,
		tracer:        tracer,
		logger:        logger,
		qos:           newQoSLimiterFromEnv(),
	}
}

func (h *apiHandler) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /metrics", h.registry.Handler())
	mux.Handle("GET /api/v1/hazards", h.requireJWT(h.qos.Middleware(http.HandlerFunc(h.listHazards))))
	mux.Handle("GET /api/v1/hazards/{id}", h.requireJWT(h.qos.Middleware(http.HandlerFunc(h.getHazardByID))))
	mux.Handle("POST /api/v1/field-reports", h.requireJWT(http.HandlerFunc(h.submitFieldReport)))
	mux.Handle("POST /field-reports", h.requireJWT(http.HandlerFunc(h.submitFieldReport)))
	return correlationMiddleware(observabilityMiddleware("client-api", h.registry, h.tracer, h.logger, mux))
}

func (h *apiHandler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "UP",
		"service": "client-api",
		"qos":     h.qos.Stats(),
	})
}

func (h *apiHandler) requireJWT(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenString, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			h.registry.IncCounter("auth_rejections_total", map[string]string{"service": "client-api", "reason": "missing_or_malformed"}, 1)
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
			h.registry.IncCounter("auth_rejections_total", map[string]string{"service": "client-api", "reason": "invalid_token"}, 1)
			writeJSON(w, http.StatusUnauthorized, errorResponse{
				Error: "UNAUTHORIZED", Message: "Missing or invalid Bearer token",
			})
			return
		}
		if claims.Scope != scopeSummary && claims.Scope != scopeAll {
			h.registry.IncCounter("auth_rejections_total", map[string]string{"service": "client-api", "reason": "forbidden_scope"}, 1)
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

	response, err := h.hazardService.GetHazards(r.Context(), options.Filter)
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
			Limit:      options.Filter.Limit,
			Offset:     options.Filter.Offset,
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

	event, found, err := h.findHazardByID(r.Context(), id)
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

func (h *apiHandler) findHazardByID(ctx context.Context, id string) (hazardEvent, bool, error) {
	const pageSize = 500 // Aggregator maximum page size.
	firstResp, err := h.hazardService.GetHazards(ctx, QueryFilter{
		HazardID: id,
		Limit:    1,
		Offset:   0,
	})
	if err != nil {
		return hazardEvent{}, false, err
	}
	for _, event := range firstResp.Data {
		if event.HazardID == id {
			return event, true, nil
		}
	}
	if len(firstResp.Data) == 0 {
		return hazardEvent{}, false, nil
	}

	for offset := 0; ; {
		filter := QueryFilter{
			Limit:  pageSize,
			Offset: offset,
		}
		response, err := h.hazardService.GetHazards(ctx, filter)
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

func (c *RemoteAggregatorClient) GetHazards(ctx context.Context, filter QueryFilter) (*HazardResponse, error) {
	query := make(url.Values)
	if filter.HazardID != "" {
		query.Set("id", filter.HazardID)
	}
	if filter.Since != "" {
		query.Set("since", filter.Since)
	}
	if filter.HazardType != "" {
		query.Set("type", filter.HazardType)
	}
	if filter.Severity != "" {
		query.Set("severity", filter.Severity)
	}
	query.Set("limit", strconv.Itoa(filter.Limit))
	query.Set("offset", strconv.Itoa(filter.Offset))

	endpoint := c.baseURL + "/internal/hazards"
	if encoded := query.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	spanStatus := http.StatusOK
	spanMsg := ""
	if c.tracer != nil {
		var span *Span
		ctx, span = c.tracer.StartSpan(ctx, "GET /internal/hazards", spanKindClient,
			SpanAttr{Key: "peer.service", Value: "aggregator"},
			SpanAttr{Key: "http.method", Value: http.MethodGet},
			SpanAttr{Key: "http.target", Value: "/internal/hazards"},
		)
		defer func() {
			if span != nil {
				span.End(spanStatus, spanMsg)
			}
		}()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		spanStatus = http.StatusInternalServerError
		spanMsg = err.Error()
		return nil, err
	}
	injectTraceHTTP(ctx, req)

	logger := c.logger
	if logger == nil {
		logger = slog.Default()
	}
	st := traceFromContext(ctx)

	started := time.Now()
	resp, err := c.httpClient.Do(req)
	dur := time.Since(started)
	if c.registry != nil {
		c.registry.ObserveHistogram("upstream_request_duration_seconds", map[string]string{
			"service":  "client-api",
			"upstream": "aggregator",
		}, dur.Seconds())
	}
	if err != nil {
		spanStatus = http.StatusBadGateway
		spanMsg = err.Error()
		if c.registry != nil {
			c.registry.IncCounter("upstream_requests_total", map[string]string{
				"service":  "client-api",
				"upstream": "aggregator",
				"status":   "error",
			}, 1)
		}
		logger.Warn("outbound call failed",
			slog.String("correlation_id", correlationIDFromContext(ctx)),
			slog.String("trace_id", st.TraceID),
			slog.String("span_id", st.SpanID),
			slog.String("upstream", "aggregator"),
			slog.String("method", req.Method),
			slog.String("path", req.URL.Path),
			slog.String("error", err.Error()),
			slog.Int64("duration_ms", dur.Milliseconds()),
		)
		return nil, err
	}
	defer resp.Body.Close()
	spanStatus = resp.StatusCode
	spanMsg = http.StatusText(resp.StatusCode)
	if c.registry != nil {
		c.registry.IncCounter("upstream_requests_total", map[string]string{
			"service":  "client-api",
			"upstream": "aggregator",
			"status":   strconv.Itoa(resp.StatusCode),
		}, 1)
	}
	logger.Info("outbound call completed",
		slog.String("correlation_id", correlationIDFromContext(ctx)),
		slog.String("trace_id", st.TraceID),
		slog.String("span_id", st.SpanID),
		slog.String("upstream", "aggregator"),
		slog.String("method", req.Method),
		slog.String("path", req.URL.Path),
		slog.Int("status", resp.StatusCode),
		slog.Int64("duration_ms", dur.Milliseconds()),
	)

	if resp.StatusCode != http.StatusOK {
		var payload errorResponse
		if err := decodeJSON(resp.Body, &payload); err != nil || payload.Error == "" {
			payload = errorResponse{Error: "BAD_GATEWAY", Message: "Aggregator returned an invalid response"}
		}
		return nil, upstreamHTTPError{Status: resp.StatusCode, Payload: payload}
	}

	var response HazardResponse
	if err := decodeJSON(resp.Body, &response); err != nil {
		return nil, fmt.Errorf("decode aggregator response: %w", err)
	}
	if response.Data == nil {
		response.Data = []hazardEvent{}
	}
	return &response, nil
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
	options := queryOptions{Fields: fields, Filter: QueryFilter{Limit: 50}}

	if raw := query.Get("since"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'since': must be ISO 8601 UTC RFC3339 format")
		}
		_, offset := parsed.Zone()
		if offset != 0 {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'since': must be ISO 8601 UTC RFC3339 format")
		}
		options.Filter.Since = raw
	}
	if raw := query.Get("type"); raw != "" {
		switch raw {
		case "SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL":
			options.Filter.HazardType = raw
		default:
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'type': must be SEISMIC, VOLCANIC, WILDFIRE, or COASTAL")
		}
	}
	if raw := query.Get("severity"); raw != "" {
		switch raw {
		case "NORMAL", "WASPADA", "SIAGA", "AWAS":
			options.Filter.Severity = raw
		default:
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'severity': must be NORMAL, WASPADA, SIAGA, or AWAS")
		}
	}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 500 {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'limit': must be an integer between 1 and 500")
		}
		options.Filter.Limit = limit
	}
	if raw := query.Get("offset"); raw != "" {
		offset, err := strconv.Atoi(raw)
		if err != nil || offset < 0 {
			return queryOptions{}, fmt.Errorf("Invalid query parameter 'offset': must be a non-negative integer")
		}
		options.Filter.Offset = offset
	}
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
		ctx := context.WithValue(r.Context(), correlationContextKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
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
	return correlationIDFromContext(r.Context())
}

func correlationIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(correlationContextKey).(string)
	return id
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
	rawBrokers := os.Getenv("REDPANDA_BROKERS")
	if rawBrokers == "" {
		rawBrokers = "redpanda:9092"
	}
	var brokers []string
	for _, b := range strings.Split(rawBrokers, ",") {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	if len(brokers) == 0 {
		return config{}, fmt.Errorf("REDPANDA_BROKERS must contain at least one broker address")
	}
	fieldReportsTopic := strings.TrimSpace(os.Getenv("REDPANDA_TOPIC_FIELD_REPORTS"))
	if fieldReportsTopic == "" {
		fieldReportsTopic = defaultFieldReportsTopic
	}
	return config{
		Port:              port,
		JWTSecret:         jwtSecret,
		AggregatorURL:     aggregatorURL,
		RedpandaBrokers:   brokers,
		FieldReportsTopic: fieldReportsTopic,
	}, nil
}

func main() {
	configuration, err := loadConfig()
	if err != nil {
		log.Fatalf("[client-api] Configuration error: %v", err)
	}

	stager := NewKafkaFieldReportStager(configuration.RedpandaBrokers, configuration.FieldReportsTopic)
	defer func() {
		if err := stager.Close(); err != nil {
			log.Printf("[client-api] Failed to close field report stager: %v", err)
		}
	}()

	handler := newAPIHandlerWithDeps(configuration.JWTSecret, NewRemoteAggregatorClient(configuration.AggregatorURL), stager)
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
