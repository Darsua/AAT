package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeMessageReader struct {
	mu        sync.Mutex
	messages  []kafka.Message
	committed []int64
}

func (f *fakeMessageReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.messages) == 0 {
		return kafka.Message{}, context.Canceled
	}
	msg := f.messages[0]
	f.messages = f.messages[1:]
	return msg, nil
}

func (f *fakeMessageReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range msgs {
		f.committed = append(f.committed, m.Offset)
	}
	return nil
}

func testConfig() Config {
	return Config{
		Brokers:              []string{"localhost:19092"},
		Topic:                "bnpb.hazard-events",
		GroupID:              "bnpb-summarizer-group",
		Port:                 "8087",
		AIEnabled:            true,
		AIModel:              "qwen3.5:9b",
		AITimeout:            200 * time.Millisecond,
		AIMaxTokens:          220,
		AIMaxCallsPerMin:     12,
		AICBFailureThreshold: 3,
		AICBCooldown:         200 * time.Millisecond,
		MaxRecentEvents:      5,
		MaxSummaries:         10,
		SummaryQueueSize:     4,
		SummaryMinInterval:   0,
	}
}

func TestMultiSourceExtractionAndCorrelation(t *testing.T) {
	cfg := testConfig()
	cfg.AIEnabled = false
	cfg.MaxRecentEvents = 5

	engine := NewSummarizerEngine(cfg, nil)

	now := time.Date(2026, 11, 17, 1, 0, 0, 0, time.UTC)
	events := []HazardEvent{
		{
			HazardID:   "haz-bmkg-1",
			Source:     "BMKG",
			HazardType: "SEISMIC",
			Severity:   "SIAGA",
			AreaName:   "Selat Sunda, Banten",
			OccurredAt: now,
			Attributes: map[string]any{
				"magnitude":         6.4,
				"depth_km":          18.0,
				"potential_tsunami": true,
				"tsunami_warning": map[string]any{
					"threat_level":      "Siaga",
					"estimated_arrival": "2026-11-17T01:15:00Z",
				},
			},
		},
		{
			HazardID:   "haz-pantai-1",
			Source:     "SENSOR_PANTAI",
			HazardType: "COASTAL",
			Severity:   "AWAS",
			AreaName:   "Pelabuhan Anyer, Banten",
			OccurredAt: now.Add(time.Second),
			Attributes: map[string]any{
				"station_id":          "stn-anyer-01",
				"sea_level_anomaly_m": 1.45,
				"wave_height_m":       3.2,
				"quality":             "ok",
				"critical":            true,
			},
		},
		{
			HazardID:   "haz-pvmbg-1",
			Source:     "PVMBG",
			HazardType: "VOLCANIC",
			Severity:   "WASPADA",
			AreaName:   "Gunung Merapi, DI Yogyakarta",
			OccurredAt: now.Add(2 * time.Second),
			Attributes: map[string]any{
				"volcano_id":          "VLC-001",
				"eruption_count_24h":  3.0,
				"ash_column_height_m": 1500.0,
				"confidence_level":    0.92,
			},
		},
		{
			HazardID:   "haz-satelit-1",
			Source:     "SATELIT",
			HazardType: "WILDFIRE",
			Severity:   "WASPADA",
			AreaName:   "Palangkaraya, Kalimantan Tengah",
			OccurredAt: now.Add(3 * time.Second),
			Attributes: map[string]any{
				"detection_count": 14.0,
				"max_confidence":  "h",
				"total_frp_mw":    182.5,
				"satellites":      []any{"N20", "N21"},
			},
		},
		{
			HazardID:   "haz-fr-1",
			Source:     "LAPORAN_LAPANGAN",
			HazardType: "LAPORAN_LAPANGAN",
			Severity:   "SIAGA",
			AreaName:   "Serang, Banten",
			OccurredAt: now.Add(4 * time.Second),
			Attributes: map[string]any{
				"description": "Coastal surge observed near pier",
			},
		},
		// 6th distinct event should evict oldest (haz-bmkg-1) from bounded window of 5
		{
			HazardID:   "haz-extra-1",
			Source:     "BMKG",
			HazardType: "SEISMIC",
			Severity:   "NORMAL",
			AreaName:   "Maluku Utara",
			OccurredAt: now.Add(5 * time.Second),
			Attributes: map[string]any{"magnitude": 4.1, "depth_km": 22.0},
		},
	}

	for _, ev := range events {
		if ok := engine.Ingest(ev, "corr-multi"); !ok {
			t.Fatalf("expected Ingest(%s) = true", ev.HazardID)
		}
	}

	// Duplicate same-severity event should be skipped
	if ok := engine.Ingest(events[5], "corr-dup"); ok {
		t.Fatal("expected duplicate event without escalation to be skipped")
	}

	summary := engine.GenerateOnce(context.Background(), "corr-multi")
	if !summary.Generated || summary.GeneratorMode != "extractive-fallback" || summary.DegradedReason != "disabled" {
		t.Fatalf("unexpected fallback summary metadata: %+v", summary)
	}
	if !strings.Contains(summary.Text, "station=stn-anyer-01") && !strings.Contains(summary.Text, "hotspots=14") {
		t.Fatalf("expected source attributes in summary text, got: %s", summary.Text)
	}

	snap := engine.Snapshot(true)
	if snap.Metrics.WindowEvents != 5 {
		t.Fatalf("window_events = %d, want bounded 5", snap.Metrics.WindowEvents)
	}
	if snap.Metrics.DuplicatesSkippedTotal != 1 {
		t.Fatalf("duplicates_skipped_total = %d, want 1", snap.Metrics.DuplicatesSkippedTotal)
	}
	if len(snap.ActiveCorrelations) == 0 || snap.ActiveCorrelations[0].Region != "Banten" {
		t.Fatalf("expected Banten compound cluster first, got: %+v", snap.ActiveCorrelations)
	}
	if !snap.ActiveCorrelations[0].MultiHazard || !snap.ActiveCorrelations[0].MultiSource {
		t.Fatalf("expected Banten cluster to be multi-source and multi-hazard: %+v", snap.ActiveCorrelations[0])
	}
}

func TestLLMGenerationAndGroundingValidation(t *testing.T) {
	var mode atomic.Int32 // 0 = valid grounded, 1 = ungrounded
	var gotAuth atomic.Value
	var gotThinkingDisabled atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		body, _ := io.ReadAll(r.Body)
		var req chatCompletionRequest
		_ = json.Unmarshal(body, &req)
		if req.ChatTemplateKwargs != nil && req.ChatTemplateKwargs["enable_thinking"] == false {
			gotThinkingDisabled.Store(true)
		}

		content := "BNPB Brief: BMKG recorded M6.1 earthquake near Sulawesi Utara (AWAS)."
		if mode.Load() == 1 {
			content = "Everything looks completely unrelated with invented Mars weather."
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"content": content}},
			},
		})
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.AIAPIURL = srv.URL
	cfg.AIAPIKey = "test-secret-token"
	llm := NewOpenAIChatClient(cfg)
	engine := NewSummarizerEngine(cfg, llm)

	engine.Ingest(HazardEvent{
		HazardID:   "haz-bmkg-sulut-1",
		Source:     "BMKG",
		HazardType: "SEISMIC",
		Severity:   "AWAS",
		AreaName:   "Sulawesi Utara",
		OccurredAt: time.Now().UTC(),
		Attributes: map[string]any{"magnitude": 6.1, "depth_km": 12.0},
	}, "corr-llm-1")

	s1 := engine.GenerateOnce(context.Background(), "corr-llm-1")
	if !s1.Generated || s1.GeneratorMode != "llm" || s1.Degraded {
		t.Fatalf("expected clean LLM summary, got: %+v", s1)
	}
	if gotAuth.Load() != "Bearer test-secret-token" {
		t.Fatalf("Authorization header = %v", gotAuth.Load())
	}
	if !gotThinkingDisabled.Load() {
		t.Fatal("expected chat_template_kwargs.enable_thinking=false in request")
	}

	mode.Store(1)
	s2 := engine.GenerateOnce(context.Background(), "corr-llm-2")
	if s2.GeneratorMode != "extractive-fallback" || !s2.Degraded || s2.DegradedReason != "validation_failed" {
		t.Fatalf("expected validation_failed fallback, got: %+v", s2)
	}
}

func TestNonBlockingIsolationUnderSlowFailedAndQuotaExhaustedAI(t *testing.T) {
	// 1. Slow AI upstream: verify consume() ingests & commits 50 messages immediately without waiting
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(350 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	defer slowSrv.Close()

	cfg := testConfig()
	cfg.AIAPIURL = slowSrv.URL
	cfg.AITimeout = 40 * time.Millisecond
	cfg.SummaryQueueSize = 4
	cfg.AIMaxCallsPerMin = 100
	cfg.AICBFailureThreshold = 2

	llm := NewOpenAIChatClient(cfg)
	engine := NewSummarizerEngine(cfg, llm)

	msgs := make([]kafka.Message, 50)
	for i := 0; i < 50; i++ {
		ev := HazardEvent{
			HazardID:   fmt.Sprintf("haz-burst-%03d", i),
			Source:     "BMKG",
			HazardType: "SEISMIC",
			Severity:   "WASPADA",
			AreaName:   "Jawa Barat",
			OccurredAt: time.Now().UTC(),
		}
		raw, _ := json.Marshal(ev)
		msgs[i] = kafka.Message{
			Offset: int64(i),
			Value:  raw,
			Headers: []kafka.Header{
				{Key: correlationHeader, Value: []byte(fmt.Sprintf("corr-%d", i))},
			},
		}
	}
	reader := &fakeMessageReader{messages: msgs}

	start := time.Now()
	err := consume(context.Background(), reader, engine, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled at end of slice, got %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("consume() took %v for 50 messages; expected non-blocking <100ms", elapsed)
	}
	if len(reader.committed) != 50 {
		t.Fatalf("committed = %d messages, want 50", len(reader.committed))
	}
	if snap := engine.Snapshot(false); snap.Metrics.CoalescedTotal == 0 {
		t.Fatalf("expected queued bursts to coalesce when queue size=4, metrics=%+v", snap.Metrics)
	}

	// 2. Drive timeout -> circuit open transitions without stalling worker
	sTimeout1 := engine.GenerateOnce(context.Background(), "corr-t1")
	if sTimeout1.DegradedReason != "timeout" {
		t.Fatalf("expected timeout fallback, got %+v", sTimeout1)
	}
	_ = engine.GenerateOnce(context.Background(), "corr-t2")
	if llm.CircuitState() != "open" {
		t.Fatalf("expected circuit_state=open after 2 timeouts, got %s", llm.CircuitState())
	}
	sOpen := engine.GenerateOnce(context.Background(), "corr-open")
	if sOpen.DegradedReason != "circuit_open" {
		t.Fatalf("expected circuit_open fallback, got %+v", sOpen)
	}

	// 3. Quota exhaustion triggers immediate fallback without dialing upstream
	quotaCfg := testConfig()
	quotaCfg.AIAPIURL = slowSrv.URL
	quotaCfg.AIMaxCallsPerMin = 1
	quotaLLM := NewOpenAIChatClient(quotaCfg)
	quotaEngine := NewSummarizerEngine(quotaCfg, quotaLLM)
	quotaEngine.Ingest(HazardEvent{
		HazardID:   "haz-q-1",
		Source:     "PVMBG",
		HazardType: "VOLCANIC",
		Severity:   "SIAGA",
		AreaName:   "Gunung Semeru, Jawa Timur",
		OccurredAt: time.Now().UTC(),
	}, "corr-q1")

	_ = quotaEngine.GenerateOnce(context.Background(), "corr-q1") // consumes the 1 allowed call/min
	sQuota := quotaEngine.GenerateOnce(context.Background(), "corr-q2")
	if sQuota.DegradedReason != "quota_exhausted" {
		t.Fatalf("expected quota_exhausted fallback, got %+v", sQuota)
	}
}

func TestHTTPInspectionAndPrometheusMetrics(t *testing.T) {
	cfg := testConfig()
	cfg.AIEnabled = false
	engine := NewSummarizerEngine(cfg, nil)
	engine.Ingest(HazardEvent{
		HazardID:   "haz-http-1",
		Source:     "SATELIT",
		HazardType: "WILDFIRE",
		Severity:   "SIAGA",
		AreaName:   "Riau",
		OccurredAt: time.Now().UTC(),
		Attributes: map[string]any{"detection_count": 9.0, "total_frp_mw": 42.1},
	}, "corr-http")
	engine.GenerateOnce(context.Background(), "corr-http")

	mux := buildHTTPMux(engine)

	// /health
	recHealth := httptest.NewRecorder()
	mux.ServeHTTP(recHealth, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recHealth.Code != http.StatusOK || !strings.Contains(recHealth.Body.String(), `"summarizer-consumer"`) {
		t.Fatalf("unexpected /health: code=%d body=%s", recHealth.Code, recHealth.Body.String())
	}

	// /summaries and /summaries/latest
	for _, path := range []string{"/summaries", "/summaries/latest"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s returned status %d", path, rec.Code)
		}
		var resp SummariesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode %s error: %v", path, err)
		}
		if !resp.Generated || resp.Latest == nil || resp.Latest.ID == "" {
			t.Fatalf("%s unexpected payload: %+v", path, resp)
		}
		if path == "/summaries" && len(resp.Summaries) != 1 {
			t.Fatalf("/summaries expected history len=1, got %d", len(resp.Summaries))
		}
		if path == "/summaries/latest" && len(resp.Summaries) != 0 {
			t.Fatalf("/summaries/latest should omit history slice, got %d", len(resp.Summaries))
		}
	}

	// /metrics
	recMetrics := httptest.NewRecorder()
	mux.ServeHTTP(recMetrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recMetrics.Body.String()
	if recMetrics.Code != http.StatusOK ||
		!strings.Contains(body, `bnpb_summarizer_events_consumed_total 1`) ||
		!strings.Contains(body, `bnpb_summarizer_summaries_generated_total{mode="extractive-fallback"} 1`) {
		t.Fatalf("unexpected /metrics output:\n%s", body)
	}
}

func TestDisabledFallbackWhenAIAPIURLEmpty(t *testing.T) {
	t.Setenv("AI_API_URL", "")
	t.Setenv("AI_ENABLED", "")

	cfg := loadConfig()
	if cfg.AIAPIURL != "" {
		t.Fatalf("expected empty default AIAPIURL, got %q", cfg.AIAPIURL)
	}
	cfg.SummaryMinInterval = 0
	engine := NewSummarizerEngine(cfg, NewOpenAIChatClient(cfg))
	engine.Ingest(HazardEvent{
		HazardID:   "haz-empty-url-1",
		Source:     "BMKG",
		HazardType: "SEISMIC",
		Severity:   "WASPADA",
		AreaName:   "Jawa Barat",
		OccurredAt: time.Now().UTC(),
	}, "corr-empty-url")

	summary := engine.GenerateOnce(context.Background(), "corr-empty-url")
	if summary.GeneratorMode != "extractive-fallback" || !summary.Degraded || summary.DegradedReason != "disabled" {
		t.Fatalf("expected disabled fallback when AI_API_URL is empty, got: %+v", summary)
	}
	if snap := engine.Snapshot(false); snap.CircuitState != "disabled" {
		t.Fatalf("expected CircuitState=disabled when AI_API_URL is empty, got %q", snap.CircuitState)
	}
}
