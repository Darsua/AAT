package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Set explicit blank values for test predictability using t.Setenv
	t.Setenv("PORT", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("POLL_INTERVAL_SECS", "")

	cfg := Load()

	if cfg.Port != "8083" {
		t.Errorf("expected default Port 8083, got %s", cfg.Port)
	}
	if cfg.PollIntervalSecs != 3*time.Second {
		t.Errorf("expected default PollIntervalSecs 3s, got %v", cfg.PollIntervalSecs)
	}
	if cfg.DatabaseURL != "" {
		t.Errorf("expected default DatabaseURL to be empty, got %s", cfg.DatabaseURL)
	}
	if cfg.DatabaseMaxConns != 25 {
		t.Errorf("expected default DatabaseMaxConns 25, got %d", cfg.DatabaseMaxConns)
	}
}

func TestLoad_CustomEnv(t *testing.T) {
	t.Setenv("PORT", "9999")
	t.Setenv("DATABASE_URL", "postgres://custom:custom@localhost:5432/db")
	t.Setenv("POLL_INTERVAL_SECS", "10")
	t.Setenv("DB_MAX_CONNS", "50")
	t.Setenv("DB_MIN_CONNS", "10")

	cfg := Load()

	if cfg.Port != "9999" {
		t.Errorf("expected Port 9999, got %s", cfg.Port)
	}
	if cfg.DatabaseURL != "postgres://custom:custom@localhost:5432/db" {
		t.Errorf("expected custom DatabaseURL, got %s", cfg.DatabaseURL)
	}
	if cfg.PollIntervalSecs != 10*time.Second {
		t.Errorf("expected PollIntervalSecs 10s, got %v", cfg.PollIntervalSecs)
	}
	if cfg.DatabaseMaxConns != 50 {
		t.Errorf("expected DatabaseMaxConns 50, got %d", cfg.DatabaseMaxConns)
	}
	if cfg.DatabaseMinConns != 10 {
		t.Errorf("expected DatabaseMinConns 10, got %d", cfg.DatabaseMinConns)
	}
}
