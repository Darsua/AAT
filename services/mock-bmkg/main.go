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

const eventGenerationInterval = 10 * time.Second

//go:embed seed.json
var seedFile []byte

type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

type seedData struct {
	SeismicEvents   []SeismicEvent   `json:"seismic_events"`
	TsunamiWarnings []TsunamiWarning `json:"tsunami_warnings"`
}

type service struct {
	mu              sync.RWMutex
	seismicEvents   []SeismicEvent
	tsunamiWarnings []TsunamiWarning
	nextSequence    uint64
	delay           func()
	now             func() time.Time
}

func main() {
	port := getenv("PORT", "8081")
	apiKey := strings.TrimSpace(os.Getenv("BMKG_API_KEY"))
	if apiKey == "" {
		log.Fatal("BMKG_API_KEY must be configured")
	}

	data, err := loadSeedData()
	if err != nil {
		log.Fatalf("load BMKG seed data: %v", err)
	}

	svc := newService(data)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.runGenerator(ctx)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      svc.routes(apiKey),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[mock-bmkg] Server listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[mock-bmkg] Server error: %v", err)
		}
	}()

	<-stop
	cancel()
	log.Println("[mock-bmkg] Shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[mock-bmkg] Forced shutdown: %v", err)
	} else {
		log.Println("[mock-bmkg] Server stopped cleanly")
	}
}

func loadSeedData() (seedData, error) {
	var data seedData
	if err := json.Unmarshal(seedFile, &data); err != nil {
		return seedData{}, err
	}
	if len(data.SeismicEvents) < 20 {
		return seedData{}, fmt.Errorf("seed must contain at least 20 seismic events, got %d", len(data.SeismicEvents))
	}
	return data, nil
}

func newService(data seedData) *service {
	return &service{
		seismicEvents:   append([]SeismicEvent(nil), data.SeismicEvents...),
		tsunamiWarnings: cloneWarnings(data.TsunamiWarnings),
		delay:           func() { time.Sleep(randomDuration(50*time.Millisecond, 150*time.Millisecond)) },
		now:             time.Now,
	}
}

func (s *service) routes(apiKey string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("GET /seismic-events", requireAPIKey(apiKey, http.HandlerFunc(s.handleSeismicEvents)))
	mux.Handle("GET /tsunami-warnings", requireAPIKey(apiKey, http.HandlerFunc(s.handleTsunamiWarnings)))
	return mux
}

func (s *service) runGenerator(ctx context.Context) {
	ticker := time.NewTicker(eventGenerationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.generateEvent()
		case <-ctx.Done():
			return
		}
	}
}

func (s *service) generateEvent() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextSequence++
	sequence := s.nextSequence
	now := s.now().UTC()
	locations := []struct {
		lat    float64
		lon    float64
		region string
	}{
		{-5.1477, 119.4327, "Pesisir Makassar-Sulawesi Selatan"},
		{-0.9471, 100.4172, "Pesisir Padang-Sumatera Barat"},
		{-2.5489, 118.0149, "Mamuju-Sulawesi Barat"},
		{-8.4095, 115.1889, "Selatan Bali"},
		{-2.2136, 113.9108, "Palangka Raya-Kalimantan Tengah"},
	}
	location := locations[(sequence-1)%uint64(len(locations))]

	s.seismicEvents = append(s.seismicEvents, SeismicEvent{
		EventID:          fmt.Sprintf("bmkg-seis-live-%s-%06d", now.Format("20060102-150405"), sequence),
		Magnitude:        4.5 + float64(sequence%16)/10,
		DepthKM:          8 + float64(sequence%35),
		EpicenterLat:     location.lat,
		EpicenterLon:     location.lon,
		RegionName:       location.region,
		OccurredAt:       now,
		PotentialTsunami: sequence%7 == 0,
	})
}

func (s *service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{Status: "UP", Service: "mock-bmkg"})
}

func (s *service) handleSeismicEvents(w http.ResponseWriter, r *http.Request) {
	s.delay()
	since, err := parseSince(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_QUERY", err.Error())
		return
	}

	s.mu.RLock()
	events := filterSeismicEvents(s.seismicEvents, since)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, events)
}

func (s *service) handleTsunamiWarnings(w http.ResponseWriter, r *http.Request) {
	s.delay()
	since, err := parseSince(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_QUERY", err.Error())
		return
	}

	s.mu.RLock()
	warnings := filterTsunamiWarnings(s.tsunamiWarnings, since)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, warnings)
}

func filterSeismicEvents(events []SeismicEvent, since *time.Time) []SeismicEvent {
	filtered := make([]SeismicEvent, 0, len(events))
	for _, event := range events {
		if since != nil && event.OccurredAt.Before(*since) {
			continue
		}
		filtered = append(filtered, event)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].OccurredAt.Before(filtered[j].OccurredAt) })
	return filtered
}

func filterTsunamiWarnings(warnings []TsunamiWarning, since *time.Time) []TsunamiWarning {
	filtered := make([]TsunamiWarning, 0, len(warnings))
	for _, warning := range warnings {
		if since != nil && warning.EstimatedArrival.Before(*since) {
			continue
		}
		warning.AffectedZones = append([]string(nil), warning.AffectedZones...)
		filtered = append(filtered, warning)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].EstimatedArrival.Before(filtered[j].EstimatedArrival)
	})
	return filtered
}

func requireAPIKey(expected string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-BMKG-Key") != expected {
			writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing or invalid X-BMKG-Key header")
			return
		}
		next.ServeHTTP(w, r)
	})
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

func cloneWarnings(warnings []TsunamiWarning) []TsunamiWarning {
	cloned := make([]TsunamiWarning, len(warnings))
	for i, warning := range warnings {
		cloned[i] = warning
		cloned[i].AffectedZones = append([]string(nil), warning.AffectedZones...)
	}
	return cloned
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
