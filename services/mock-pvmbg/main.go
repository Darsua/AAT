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
	"strings"
	"sync"
	"syscall"
	"time"
)

const reportGenerationInterval = 10 * time.Second

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
		delay:   func() { time.Sleep(randomDuration(500*time.Millisecond, 3*time.Second)) },
		now:     time.Now,
	}
}

func (s *service) routes(token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("GET /volcanic-reports", requireBearerToken(token, http.HandlerFunc(s.handleReports)))
	mux.Handle("POST /admin/schema-version", requireBearerToken(token, http.HandlerFunc(s.handleSchemaVersion)))
	mux.Handle("POST /admin/outage", requireBearerToken(token, http.HandlerFunc(s.handleOutage)))
	return mux
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
	reports := filterReports(s.reports, since)
	s.mu.RUnlock()
	if includeConfidence {
		for i := range reports {
			c := 0.80 + float64(i%20)*0.01
			reports[i].ConfidenceLevel = &c
		}
	}
	writeJSON(w, http.StatusOK, reports)
}

func (s *service) handleSchemaVersion(w http.ResponseWriter, r *http.Request) {
	var request schemaVersionRequest
	if err := decodeJSON(r, &request); err != nil || request.IncludeConfidenceLevel == nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "include_confidence_level must be a boolean")
		return
	}

	s.mu.Lock()
	s.includeConfidenceLevel = *request.IncludeConfidenceLevel
	includeConfidence := s.includeConfidenceLevel
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":                   "updated",
		"include_confidence_level": includeConfidence,
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
	IncludeConfidenceLevel *bool `json:"include_confidence_level"`
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
