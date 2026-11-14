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
	t.Setenv("INSTANCE_ID", "")
	t.Setenv("HOSTNAME", "")
	t.Setenv("LEASE_TTL_SECS", "")
	t.Setenv("LEASE_RENEW_INTERVAL_SECS", "")
	t.Setenv("DRAIN_DELAY_MS", "")

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
	if cfg.AuditToken != "" {
		t.Errorf("expected default AuditToken empty, got %s", cfg.AuditToken)
	}
	if cfg.AuditAllSourcesLabel != "ALL" {
		t.Errorf("expected default AuditAllSourcesLabel ALL, got %s", cfg.AuditAllSourcesLabel)
	}
	if cfg.NodeName != "local" {
		t.Errorf("expected default NodeName local, got %s", cfg.NodeName)
	}
	if cfg.StorageUnitID != "shard-1" {
		t.Errorf("expected default StorageUnitID shard-1, got %s", cfg.StorageUnitID)
	}
	if cfg.InstanceID == "" {
		t.Error("expected non-empty fallback InstanceID")
	}
	if cfg.LeaseTTL != 15*time.Second {
		t.Errorf("expected default LeaseTTL 15s, got %v", cfg.LeaseTTL)
	}
	if cfg.LeaseRenewInterval != 5*time.Second {
		t.Errorf("expected default LeaseRenewInterval 5s, got %v", cfg.LeaseRenewInterval)
	}
	if cfg.DrainDelay != 0 {
		t.Errorf("expected default DrainDelay 0, got %v", cfg.DrainDelay)
	}
}

func TestLoad_CustomEnv(t *testing.T) {
	t.Setenv("PORT", "9999")
	t.Setenv("DATABASE_URL", "postgres://custom:custom@localhost:5432/db")
	t.Setenv("POLL_INTERVAL_SECS", "10")
	t.Setenv("DB_MAX_CONNS", "50")
	t.Setenv("DB_MIN_CONNS", "10")
	t.Setenv("AUDIT_TOKEN", "audit-tok-123")
	t.Setenv("AUDIT_ALL_SOURCES_LABEL", "*")
	t.Setenv("NODE_NAME", "aqua")
	t.Setenv("STORAGE_UNIT_ID", "shard-2")
	t.Setenv("SHARD_CONFIG", "shard-1=shard-1|aqua|primary|postgres://a,shard-1-r1|ruby|replica|postgres://b")
	t.Setenv("INSTANCE_ID", "aggregator-pod-1")
	t.Setenv("LEASE_TTL_SECS", "20")
	t.Setenv("LEASE_RENEW_INTERVAL_SECS", "4")
	t.Setenv("DRAIN_DELAY_MS", "250")

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
	if cfg.AuditToken != "audit-tok-123" {
		t.Errorf("expected AuditToken audit-tok-123, got %s", cfg.AuditToken)
	}
	if cfg.AuditAllSourcesLabel != "*" {
		t.Errorf("expected AuditAllSourcesLabel *, got %s", cfg.AuditAllSourcesLabel)
	}
	if cfg.NodeName != "aqua" {
		t.Errorf("expected NodeName aqua, got %s", cfg.NodeName)
	}
	if cfg.StorageUnitID != "shard-2" {
		t.Errorf("expected StorageUnitID shard-2, got %s", cfg.StorageUnitID)
	}
	if cfg.ShardConfig == "" {
		t.Error("expected non-empty ShardConfig")
	}
	if cfg.InstanceID != "aggregator-pod-1" {
		t.Errorf("expected InstanceID aggregator-pod-1, got %s", cfg.InstanceID)
	}
	if cfg.LeaseTTL != 20*time.Second {
		t.Errorf("expected LeaseTTL 20s, got %v", cfg.LeaseTTL)
	}
	if cfg.LeaseRenewInterval != 4*time.Second {
		t.Errorf("expected LeaseRenewInterval 4s, got %v", cfg.LeaseRenewInterval)
	}
	if cfg.DrainDelay != 250*time.Millisecond {
		t.Errorf("expected DrainDelay 250ms, got %v", cfg.DrainDelay)
	}
}
