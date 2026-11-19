package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config represents the complete runtime configuration for the Aggregator service.
type Config struct {
	Port                 string
	DatabaseURL          string
	DatabaseMaxConns     int
	DatabaseMinConns     int
	RedpandaBrokers      string
	Topic                string
	BMKGURL              string
	BMKGKey              string
	PVMBGURL             string
	PVMBGToken           string
	PollIntervalSecs     time.Duration
	LogLevel             string
	AuditToken           string
	AuditAllSourcesLabel string
	NodeName             string
	StorageUnitID        string
	ShardTopologyJSON    string
	ShardConfig          string
	InstanceID           string
	LeaseTTL             time.Duration
	LeaseRenewInterval   time.Duration
	DrainDelay           time.Duration

	// Satellite wildfire stream (#28); disabled unless explicitly enabled.
	SatelliteEnabled  bool
	SatelliteURL      string
	SatelliteToken    string
	SatelliteLateness time.Duration
	// JSON array of {"name","latitude","longitude"} reference-province centroids for area_name.
	SatelliteProvinceFile string

	// PVMBG schema evolution policy (#26); defaults reject all incompatible changes.
	PVMBGFieldAliases         string
	PVMBGCoerceNumericStrings bool
	DLQRetryBudget            int
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
	if minConns <= 0 {
		minConns = 5
	}

	nodeName := getEnv("NODE_NAME", "local")
	instanceID := getEnv("INSTANCE_ID", "")
	if instanceID == "" {
		instanceID = getEnv("HOSTNAME", "")
	}
	if instanceID == "" {
		instanceID = fmt.Sprintf("%s-%d", nodeName, os.Getpid())
	}

	leaseTTLSecs := getEnvInt("LEASE_TTL_SECS", 15)
	if leaseTTLSecs <= 0 {
		leaseTTLSecs = 15
	}

	leaseRenewSecs := getEnvInt("LEASE_RENEW_INTERVAL_SECS", 5)
	if leaseRenewSecs <= 0 {
		leaseRenewSecs = 5
	}

	drainDelayMs := getEnvInt("DRAIN_DELAY_MS", 0)
	if drainDelayMs < 0 {
		drainDelayMs = 0
	}

	cfg := &Config{
		Port:                 getEnv("PORT", "8083"),
		DatabaseURL:          getEnv("DATABASE_URL", ""),
		DatabaseMaxConns:     maxConns,
		DatabaseMinConns:     minConns,
		RedpandaBrokers:      getEnv("REDPANDA_BROKERS", "redpanda:9092"),
		Topic:                getEnv("TOPIC", "bnpb.hazard-events"),
		BMKGURL:              getEnv("BMKG_URL", "http://mock-bmkg:8081"),
		BMKGKey:              getEnv("BMKG_KEY", ""),
		PVMBGURL:             getEnv("PVMBG_URL", "http://mock-pvmbg:8082"),
		PVMBGToken:           getEnv("PVMBG_TOKEN", ""),
		PollIntervalSecs:     time.Duration(pollSecs) * time.Second,
		LogLevel:             getEnv("LOG_LEVEL", "info"),
		AuditToken:           getEnv("AUDIT_TOKEN", ""),
		AuditAllSourcesLabel: getEnv("AUDIT_ALL_SOURCES_LABEL", "ALL"),
		NodeName:             nodeName,
		StorageUnitID:        getEnv("STORAGE_UNIT_ID", "shard-1"),
		ShardTopologyJSON:    getEnv("SHARD_TOPOLOGY_JSON", ""),
		ShardConfig:          getEnv("SHARD_CONFIG", ""),
		InstanceID:           instanceID,
		LeaseTTL:             time.Duration(leaseTTLSecs) * time.Second,
		LeaseRenewInterval:   time.Duration(leaseRenewSecs) * time.Second,
		DrainDelay:           time.Duration(drainDelayMs) * time.Millisecond,
	}

	cfg.SatelliteEnabled = getEnv("SATELLITE_ENABLED", "false") == "true"
	cfg.SatelliteURL = getEnv("SATELLITE_URL", "https://satellite.naufarrel.dev")
	cfg.SatelliteToken = getEnv("SATELLITE_GROUP_TOKEN", "")
	cfg.SatelliteProvinceFile = getEnv("SATELLITE_PROVINCE_CENTROIDS_FILE", "")
	if mins := getEnvInt("SATELLITE_LATENESS_MINUTES", 0); mins > 0 {
		cfg.SatelliteLateness = time.Duration(mins) * time.Minute
	}

	cfg.PVMBGFieldAliases = getEnv("PVMBG_FIELD_ALIASES", "")
	cfg.PVMBGCoerceNumericStrings = getEnv("PVMBG_COERCE_NUMERIC_STRINGS", "false") == "true"
	cfg.DLQRetryBudget = getEnvInt("DLQ_RETRY_BUDGET", 5)
	if cfg.DLQRetryBudget <= 0 {
		cfg.DLQRetryBudget = 5
	}
	return cfg
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
