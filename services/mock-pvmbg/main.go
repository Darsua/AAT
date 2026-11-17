package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const reportGenerationInterval = 10 * time.Second

// schemaMode selects how /volcanic-reports serializes reports, as defined in contracts/pvmbg.yaml.
type schemaMode string

const (
	schemaModeAdditive             schemaMode = "additive"
	schemaModeRenameField          schemaMode = "rename_field"
	schemaModeTypeChange           schemaMode = "type_change"
	schemaModeMissingRequiredField schemaMode = "missing_required_field"
)

func parseSchemaMode(value string) (schemaMode, bool) {
	switch mode := schemaMode(value); mode {
	case schemaModeAdditive, schemaModeRenameField, schemaModeTypeChange, schemaModeMissingRequiredField:
		return mode, true
	}
	return "", false
}

//go:embed seed.json
var seedFile []byte

type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
	ConfidenceLevel  *float64  `json:"confidence_level,omitempty"`
}

type seedData struct {
	Reports []VolcanicReport `json:"reports"`
}

type service struct {
	mu                     sync.RWMutex
	reports                []VolcanicReport
	includeConfidenceLevel bool
	mode                   schemaMode
	isOutage               bool
	nextSequence           uint64
	delay                  func()
	now                    func() time.Time
}

func main() {
	port := getenv("PORT", "8082")
	token := strings.TrimSpace(os.Getenv("PVMBG_TOKEN"))
	if token == "" {
		log.Fatal("PVMBG_TOKEN must be configured")
	}

	data, err := loadSeedData()
	if err != nil {
		log.Fatalf("load PVMBG seed data: %v", err)
	}

	svc := newService(data)
	minDelay := envMillis("PVMBG_DELAY_MIN_MS", 500)
	maxDelay := envMillis("PVMBG_DELAY_MAX_MS", 3000)
	if maxDelay < minDelay {
		log.Fatalf("PVMBG_DELAY_MAX_MS (%s) must not be lower than PVMBG_DELAY_MIN_MS (%s)", maxDelay, minDelay)
	}
	svc.delay = func() { time.Sleep(randomDuration(minDelay, maxDelay)) }
	log.Printf("PVMBG response delay configured: min=%s max=%s", minDelay, maxDelay)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.runGenerator(ctx)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      svc.routes(token),
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
	cancel()
	log.Println("[mock-pvmbg] Shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[mock-pvmbg] Forced shutdown: %v", err)
	} else {
		log.Println("[mock-pvmbg] Server stopped cleanly")
	}
}

func loadSeedData() (seedData, error) {
	var data seedData
	if err := json.Unmarshal(seedFile, &data); err != nil {
		return seedData{}, err
	}
	if len(data.Reports) < 20 {
		return seedData{}, fmt.Errorf("seed must contain at least 20 volcanic reports, got %d", len(data.Reports))
	}
	return data, nil
}

func newService(data seedData) *service {
	return &service{
		reports: append([]VolcanicReport(nil), data.Reports...),
		mode:    schemaModeAdditive,
		delay:   func() { time.Sleep(randomDuration(500*time.Millisecond, 3*time.Second)) },
		now:     time.Now,
	}
}

func (s *service) routes(token string) http.Handler {
	logger := newJSONLogger("mock-pvmbg")
	reg := newRegistry()
	tracer := newTracer("mock-pvmbg", logger)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", reg.Handler())
	mux.Handle("GET /volcanic-reports", requireBearerToken(token, http.HandlerFunc(s.handleReports)))
	mux.Handle("POST /admin/schema-version", requireBearerToken(token, http.HandlerFunc(s.handleSchemaVersion)))
	mux.Handle("POST /admin/outage", requireBearerToken(token, http.HandlerFunc(s.handleOutage)))
	return correlationMiddleware(observabilityMiddleware("mock-pvmbg", reg, tracer, logger, mux))
}

func (s *service) runGenerator(ctx context.Context) {
	ticker := time.NewTicker(reportGenerationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.generateReport()
		case <-ctx.Done():
			return
		}
	}
}

func (s *service) generateReport() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextSequence++
	sequence := s.nextSequence
	now := s.now().UTC()
	volcanoes := []struct {
		id    string
		level string
	}{
		{"VLC-001", "Siaga"},
		{"VLC-002", "Waspada"},
		{"VLC-003", "Siaga"},
		{"VLC-004", "Waspada"},
		{"VLC-005", "Waspada"},
		{"VLC-006", "Awas"},
	}
	volcano := volcanoes[(sequence-1)%uint64(len(volcanoes))]
	s.reports = append(s.reports, VolcanicReport{
		ReportID:         fmt.Sprintf("pvmbg-rpt-live-%s-%06d", now.Format("20060102-150405"), sequence),
		VolcanoID:        volcano.id,
		AlertLevel:       volcano.level,
		EruptionCount24h: int(sequence % 15),
		AshColumnHeightM: float64(150 + (sequence%9)*100),
		ReportedAt:       now,
	})
}

func (s *service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if s.outageActive() {
		writeJSON(w, http.StatusServiceUnavailable, HealthResponse{Status: "DOWN", Service: "mock-pvmbg"})
		return
	}
	writeJSON(w, http.StatusOK, HealthResponse{Status: "UP", Service: "mock-pvmbg"})
}

func (s *service) handleReports(w http.ResponseWriter, r *http.Request) {
	if s.outageActive() {
		writeError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "PVMBG service temporarily unreachable")
		return
	}
	s.delay()

	since, err := parseSince(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_QUERY", err.Error())
		return
	}

	s.mu.RLock()
	includeConfidence := s.includeConfidenceLevel
	mode := s.mode
	reports := filterReports(s.reports, since)
	s.mu.RUnlock()
	if includeConfidence {
		for i := range reports {
			c := 0.80 + float64(i%20)*0.01
			reports[i].ConfidenceLevel = &c
		}
	}
	if mode == schemaModeAdditive {
		writeJSON(w, http.StatusOK, reports)
		return
	}
	payload, err := applySchemaMode(reports, mode)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to serialize reports")
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

// applySchemaMode rewrites serialized reports for an incompatible test mode.
// Stored reports are untouched, so switching back to additive serves them normally again.
func applySchemaMode(reports []VolcanicReport, mode schemaMode) ([]map[string]json.RawMessage, error) {
	payload := make([]map[string]json.RawMessage, 0, len(reports))
	for _, report := range reports {
		raw, err := json.Marshal(report)
		if err != nil {
			return nil, err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
		switch mode {
		case schemaModeRenameField:
			fields["event_id"] = fields["report_id"]
			delete(fields, "report_id")
		case schemaModeTypeChange:
			count, err := json.Marshal(strconv.Itoa(report.EruptionCount24h))
			if err != nil {
				return nil, err
			}
			fields["eruption_count_24h"] = count
		case schemaModeMissingRequiredField:
			delete(fields, "volcano_id")
		}
		payload = append(payload, fields)
	}
	return payload, nil
}

func (s *service) handleSchemaVersion(w http.ResponseWriter, r *http.Request) {
	var request schemaVersionRequest
	if err := decodeJSON(r, &request); err != nil || (request.IncludeConfidenceLevel == nil && request.Mode == nil) {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "include_confidence_level must be a boolean and/or mode must be a supported schema mode")
		return
	}
	// Omitting mode selects additive so legacy M1 requests keep their original behavior.
	mode := schemaModeAdditive
	if request.Mode != nil {
		parsed, ok := parseSchemaMode(*request.Mode)
		if !ok {
			writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "mode must be one of additive, rename_field, type_change, missing_required_field")
			return
		}
		mode = parsed
	}

	s.mu.Lock()
	if request.IncludeConfidenceLevel != nil {
		s.includeConfidenceLevel = *request.IncludeConfidenceLevel
	}
	s.mode = mode
	includeConfidence := s.includeConfidenceLevel
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":                   "updated",
		"include_confidence_level": includeConfidence,
		"mode":                     string(mode),
		"message":                  "Dynamic schema updated successfully",
	})
}

func (s *service) handleOutage(w http.ResponseWriter, r *http.Request) {
	var request outageRequest
	if err := decodeJSON(r, &request); err != nil || request.IsOutage == nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "is_outage must be a boolean")
		return
	}

	s.mu.Lock()
	s.isOutage = *request.IsOutage
	isOutage := s.isOutage
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "updated",
		"is_outage": isOutage,
		"message":   "Outage mode updated successfully",
	})
}

type schemaVersionRequest struct {
	IncludeConfidenceLevel *bool   `json:"include_confidence_level"`
	Mode                   *string `json:"mode"`
}

type outageRequest struct {
	IsOutage *bool `json:"is_outage"`
}

func (s *service) outageActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isOutage
}

func filterReports(reports []VolcanicReport, since *time.Time) []VolcanicReport {
	filtered := make([]VolcanicReport, 0, len(reports))
	for _, report := range reports {
		if since != nil && report.ReportedAt.Before(*since) {
			continue
		}
		filtered = append(filtered, report)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ReportedAt.Before(filtered[j].ReportedAt) })
	return filtered
}

func requireBearerToken(expected string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		header := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(header, prefix) || strings.TrimSpace(strings.TrimPrefix(header, prefix)) != expected {
			writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing or invalid Bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(r *http.Request, destination any) error {
	return json.NewDecoder(r.Body).Decode(destination)
}

func parseSince(r *http.Request) (*time.Time, error) {
	value := strings.TrimSpace(r.URL.Query().Get("since"))
	if value == "" {
		return nil, nil
	}
	timestamp, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("since must be a valid RFC3339 timestamp")
	}
	timestamp = timestamp.UTC()
	return &timestamp, nil
}

func randomDuration(minimum, maximum time.Duration) time.Duration {
	if maximum <= minimum {
		return minimum
	}
	return minimum + time.Duration(rand.Int63n(int64(maximum-minimum)+1))
}

// envMillis reads a non-negative integer number of milliseconds from the environment.
func envMillis(name string, fallbackMS int) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return time.Duration(fallbackMS) * time.Millisecond
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		log.Fatalf("%s must be a non-negative integer number of milliseconds, got %q", name, raw)
	}
	return time.Duration(value) * time.Millisecond
}

func getenv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"error": code, "message": message})
}
