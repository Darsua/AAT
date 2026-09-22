package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Ensure clean environment for test
	os.Unsetenv("PORT")
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("POLL_INTERVAL_SECS")

	cfg := Load()

	if cfg.Port != "8083" {
		t.Errorf("expected default Port 8083, got %s", cfg.Port)
	}
	if cfg.PollIntervalSecs != 3*time.Second {
		t.Errorf("expected default PollIntervalSecs 3s, got %v", cfg.PollIntervalSecs)
	}
	if cfg.DatabaseURL == "" {
		t.Error("expected default DatabaseURL to not be empty")
	}
}

func TestLoad_CustomEnv(t *testing.T) {
	os.Setenv("PORT", "9999")
	os.Setenv("POLL_INTERVAL_SECS", "10")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("POLL_INTERVAL_SECS")
	}()

	cfg := Load()

	if cfg.Port != "9999" {
		t.Errorf("expected Port 9999, got %s", cfg.Port)
	}
	if cfg.PollIntervalSecs != 10*time.Second {
		t.Errorf("expected PollIntervalSecs 10s, got %v", cfg.PollIntervalSecs)
	}
}
