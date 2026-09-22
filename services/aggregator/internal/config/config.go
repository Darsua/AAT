package config

import (
	"os"
	"strconv"
	"time"
)

// Config represents the complete runtime configuration for the Aggregator service.
type Config struct {
	Port             string
	DatabaseURL      string
	DatabaseMaxConns int
	DatabaseMinConns int
	RedpandaBrokers  string
	Topic            string
	BMKGURL          string
	BMKGKey          string
	PVMBGURL         string
	PVMBGToken       string
	PollIntervalSecs time.Duration
	LogLevel         string
}

// Load loads configuration from environment variables with safe defaults matching .env.example.
func Load() *Config {
	pollSecs := getEnvInt("POLL_INTERVAL_SECS", 3)
	if pollSecs <= 0 {
		pollSecs = 3
	}

	maxConns := getEnvInt("DB_MAX_CONNS", 25)
	if maxConns <= 0 {
		maxConns = 25
	}

	minConns := getEnvInt("DB_MIN_CONNS", 5)
	if minConns < 0 {
		minConns = 5
	}

	return &Config{
		Port:             getEnv("PORT", "8083"),
		DatabaseURL:      getEnv("DATABASE_URL", "postgres://bnpb_user:bnpb_secret@canonical-db:5432/bnpb_hazards?sslmode=disable"),
		DatabaseMaxConns: maxConns,
		DatabaseMinConns: minConns,
		RedpandaBrokers:  getEnv("REDPANDA_BROKERS", "redpanda:9092"),
		Topic:            getEnv("TOPIC", "bnpb.hazard-events"),
		BMKGURL:          getEnv("BMKG_URL", "http://mock-bmkg:8081"),
		BMKGKey:          getEnv("BMKG_KEY", "bmkg-secret-key-2026"),
		PVMBGURL:         getEnv("PVMBG_URL", "http://mock-pvmbg:8082"),
		PVMBGToken:       getEnv("PVMBG_TOKEN", "pvmbg-bearer-token-2026"),
		PollIntervalSecs: time.Duration(pollSecs) * time.Second,
		LogLevel:         getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	valStr, ok := os.LookupEnv(key)
	if !ok || valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}
