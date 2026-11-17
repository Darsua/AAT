package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultBrokers    = "localhost:19092"
	defaultTopic      = "bnpb.hazard-events"
	defaultGroupID    = "bnpb-summarizer-group"
	defaultPort       = "8087"
	defaultAIAPIURL   = ""
	defaultAIModel    = "qwen3.5:9b"
	correlationHeader = "X-Correlation-ID"
)

// Config holds bounded worker, Kafka consumer, and AI endpoint configuration.
type Config struct {
	Brokers              []string
	Topic                string
	FieldReportsTopic    string
	GroupID              string
	Port                 string
	AIEnabled            bool
	AIAPIURL             string
	AIModel              string
	AIAPIKey             string
	AIUseEnvProxy        bool
	AITimeout            time.Duration
	AIMaxTokens          int
	AIMaxCallsPerMin     int
	AICBFailureThreshold int
	AICBCooldown         time.Duration
	MaxRecentEvents      int
	MaxSummaries         int
	SummaryQueueSize     int
	SummaryMinInterval   time.Duration
}

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

func loadConfig() Config {
	return Config{
		Brokers:              splitAndTrim(getEnv("REDPANDA_BROKERS", defaultBrokers)),
		Topic:                getEnv("TOPIC", defaultTopic),
		FieldReportsTopic:    strings.TrimSpace(os.Getenv("FIELD_REPORTS_TOPIC")),
		GroupID:              getEnv("CONSUMER_GROUP", defaultGroupID),
		Port:                 getEnv("PORT", defaultPort),
		AIEnabled:            getBoolEnv("AI_ENABLED", true),
		AIAPIURL:             getEnv("AI_API_URL", defaultAIAPIURL),
		AIModel:              getEnv("AI_MODEL", defaultAIModel),
		AIAPIKey:             strings.TrimSpace(os.Getenv("AI_API_KEY")),
		AIUseEnvProxy:        getBoolEnv("AI_USE_ENV_PROXY", false),
		AITimeout:            time.Duration(getIntEnv("AI_TIMEOUT_MS", 20000)) * time.Millisecond,
		AIMaxTokens:          getIntEnv("AI_MAX_TOKENS", 160),
		AIMaxCallsPerMin:     getIntEnv("AI_MAX_CALLS_PER_MIN", 12),
		AICBFailureThreshold: getIntEnv("AI_CB_FAILURE_THRESHOLD", 3),
		AICBCooldown:         time.Duration(getIntEnv("AI_CB_COOLDOWN_SECS", 30)) * time.Second,
		MaxRecentEvents:      getIntEnv("MAX_RECENT_EVENTS", 200),
		MaxSummaries:         getIntEnv("MAX_SUMMARIES", 30),
		SummaryQueueSize:     getIntEnv("SUMMARY_QUEUE_SIZE", 16),
		SummaryMinInterval:   time.Duration(getIntEnv("SUMMARY_MIN_INTERVAL_MS", 2000)) * time.Millisecond,
	}
}

func getEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func getIntEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func getBoolEnv(name string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch raw {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return fallback
	}
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func decodeEvent(data []byte) (HazardEvent, error) {
	var event HazardEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return HazardEvent{}, fmt.Errorf("decode hazard event: %w", err)
	}
	if event.HazardID == "" || event.HazardType == "" || event.Severity == "" {
		return HazardEvent{}, errors.New("hazard_id, hazard_type, and severity are required")
	}
	if event.Source == "" {
		event.Source = "UNKNOWN"
	}
	return event, nil
}

func decodeFieldReportOrEvent(data []byte) (HazardEvent, error) {
	if ev, err := decodeEvent(data); err == nil {
		return ev, nil
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return HazardEvent{}, fmt.Errorf("decode field report: %w", err)
	}
	id := strAttr(raw, "report_id")
	if id == "" {
		id = strAttr(raw, "hazard_id")
	}
	area := strAttr(raw, "area_name")
	sev := strings.ToUpper(strAttr(raw, "severity"))
	if sev == "" {
		sev = "WASPADA"
	}
	if id == "" || area == "" {
		return HazardEvent{}, errors.New("field report missing report_id or area_name")
	}
	ht := strings.ToUpper(strAttr(raw, "hazard_type"))
	if ht == "" {
		ht = "LAPORAN_LAPANGAN"
	}
	return HazardEvent{
		HazardID:   id,
		Source:     "LAPORAN_LAPANGAN",
		HazardType: ht,
		Severity:   sev,
		AreaName:   area,
		Attributes: map[string]any{"description": strAttr(raw, "description")},
		OccurredAt: time.Now().UTC(),
		IngestedAt: time.Now().UTC(),
	}, nil
}

func messageHeader(message kafka.Message, name string) string {
	for _, header := range message.Headers {
		if strings.EqualFold(header.Key, name) {
			return string(header.Value)
		}
	}
	return ""
}

// consume reads events, updates the bounded window, triggers async summarization
// non-blockingly, and commits offsets immediately without waiting for AI inference.
func consume(ctx context.Context, reader messageReader, engine *SummarizerEngine, fieldReports bool) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		var ev HazardEvent
		if fieldReports {
			ev, err = decodeFieldReportOrEvent(message.Value)
		} else {
			ev, err = decodeEvent(message.Value)
		}
		correlationID := messageHeader(message, correlationHeader)
		if err != nil {
			log.Printf(
				"[summarizer] correlation_id=%q skipped invalid message partition=%d offset=%d error=%q",
				correlationID,
				message.Partition,
				message.Offset,
				err,
			)
			if err := reader.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf("commit invalid message: %w", err)
			}
			continue
		}

		engine.Ingest(ev, correlationID)
		if err := reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit message: %w", err)
		}
	}
}

func main() {
	cfg := loadConfig()
	llm := NewOpenAIChatClient(cfg)
	engine := NewSummarizerEngine(cfg, llm)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go engine.RunWorker(ctx)
	startHTTPServer(ctx, cfg.Port, engine)

	if cfg.FieldReportsTopic != "" {
		frReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.Brokers,
			Topic:       cfg.FieldReportsTopic,
			GroupID:     cfg.GroupID + "-field-reports",
			StartOffset: kafka.FirstOffset,
		})
		go func() {
			defer frReader.Close()
			if err := consume(ctx, frReader, engine, true); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[summarizer] field-reports reader stopped: %v", err)
			}
		}()
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	log.Printf(
		"[summarizer] consuming topic=%s group=%s brokers=%s ai_enabled=%t model=%s",
		cfg.Topic,
		cfg.GroupID,
		strings.Join(cfg.Brokers, ","),
		cfg.AIEnabled,
		cfg.AIModel,
	)

	if err := consume(ctx, reader, engine, false); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[summarizer] stopped: %v", err)
		return
	}
	log.Println("[summarizer] stopped")
}
