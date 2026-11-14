package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/api"
	"bnpb/aggregator/internal/config"
	"bnpb/aggregator/internal/logger"
	"bnpb/aggregator/internal/normalizer"
	"bnpb/aggregator/internal/repository"
	"bnpb/aggregator/internal/status"
	"bnpb/aggregator/internal/worker"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load()
	log := logger.Setup(cfg.LogLevel)

	log.Info("starting bnpb aggregator service",
		slog.String("port", cfg.Port),
		slog.Duration("poll_interval", cfg.PollIntervalSecs),
	)

	// Context for bootstrap operations
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer bootCancel()

	shardSpecs, err := repository.ParseShardTopology(
		cfg.ShardTopologyJSON,
		cfg.ShardConfig,
		cfg.DatabaseURL,
		cfg.NodeName,
		cfg.StorageUnitID,
	)
	if err != nil {
		log.Error("fatal: invalid storage shard configuration", "error", err)
		os.Exit(1)
	}

	// Connect to configured canonical storage shards with retry backoff
	hazardRepo, primaryPool, err := connectShardsWithRetry(bootCtx, shardSpecs, cfg.DatabaseMaxConns, cfg.DatabaseMinConns, log)
	if err != nil {
		log.Error("fatal: could not connect to canonical database", "error", err)
		os.Exit(1)
	}
	defer hazardRepo.Close()

	// Initialize volcano reference cache from a healthy storage pool
	volcanoRepo := repository.NewVolcanoRepository(primaryPool)
	if err := volcanoRepo.LoadCache(bootCtx); err != nil {
		log.Warn("warning: could not pre-populate volcano reference cache", "error", err)
	} else {
		log.Info("volcano catalog cached into memory successfully")
	}

	brokerPinger := repository.NewTCPBrokerPinger(cfg.RedpandaBrokers)

	// Initialize circuit status tracker and normalizer
	statusTracker := status.NewTracker()
	norm := normalizer.NewNormalizer(volcanoRepo)

	// Initialize upstream adapters
	bmkgAdapter := adapter.NewBMKGAdapter(cfg.BMKGURL, cfg.BMKGKey, 10*time.Second)
	pvmbgAdapter := adapter.NewPVMBGAdapter(cfg.PVMBGURL, cfg.PVMBGToken, 10*time.Second)

	// Initialize Kafka message writer for Redpanda
	rawBrokers := strings.Split(cfg.RedpandaBrokers, ",")
	brokers := make([]string, 0, len(rawBrokers))
	for _, b := range rawBrokers {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  3,
		BatchSize:    50,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		RequiredAcks: kafka.RequireOne,
	}
	defer kafkaWriter.Close()

	// Initialize background polling and outbox relay workers
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	bmkgPoller := worker.NewBMKGPoller(bmkgAdapter, norm, hazardRepo, statusTracker, cfg.PollIntervalSecs, log).
		WithLeaseConfig(cfg.InstanceID, cfg.LeaseTTL, cfg.LeaseRenewInterval)
	pvmbgPoller := worker.NewPVMBGPoller(pvmbgAdapter, norm, hazardRepo, statusTracker, cfg.PollIntervalSecs, log).
		WithLeaseConfig(cfg.InstanceID, cfg.LeaseTTL, cfg.LeaseRenewInterval)
	outboxRelay := worker.NewOutboxRelay(hazardRepo, kafkaWriter, 500*time.Millisecond, log)

	var workerWg sync.WaitGroup
	workerWg.Add(3)
	go func() {
		defer workerWg.Done()
		bmkgPoller.Start(workerCtx)
	}()
	go func() {
		defer workerWg.Done()
		pvmbgPoller.Start(workerCtx)
	}()
	go func() {
		defer workerWg.Done()
		outboxRelay.Start(workerCtx)
	}()

	if hasReplicaUnits(shardSpecs) {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			runReplicaSyncLoop(workerCtx, hazardRepo, 30*time.Second, log)
		}()
	}

	// Register HTTP routes
	mux := http.NewServeMux()

	healthHandler := api.NewHealthHandler(hazardRepo, brokerPinger)
	hazardHandler := api.NewHazardHandler(hazardRepo, statusTracker)
	auditHandler := api.NewAuditHandler(hazardRepo, cfg.AuditToken, cfg.AuditAllSourcesLabel)

	mux.Handle("GET /health", healthHandler)
	mux.Handle("GET /internal/hazards", hazardHandler)
	mux.Handle("/internal/audit/events", auditHandler)
	mux.Handle("/internal/audit/checksum", auditHandler)
	mux.Handle("/internal/audit/location", auditHandler)
	mux.Handle("/internal/audit/unprocessed", auditHandler)

	// Middleware chaining: CorrelationID (outermost) -> Logging -> Recovery (innermost) -> Router
	var handler http.Handler = mux
	handler = api.RecoveryMiddleware(handler)
	handler = api.LoggingMiddleware(handler)
	handler = api.CorrelationIDMiddleware(handler)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Info("aggregator internal HTTP server listening", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for termination signal
	sig := <-stopChan
	log.Info("shutting down aggregator server gracefully",
		slog.String("signal", sig.String()),
		slog.String("instance_id", cfg.InstanceID),
	)

	// 1. Mark readiness probe unhealthy immediately so ingress drains new requests before shutdown
	healthHandler.SetDraining(true)

	// 2. Drain and stop background workers (releasing leader leases cleanly)
	workerCancel()
	workerWg.Wait()
	log.Info("background polling and outbox workers stopped cleanly")

	if cfg.DrainDelay > 0 {
		time.Sleep(cfg.DrainDelay)
	}

	// 3. Stop HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("forced shutdown error", "error", err)
	} else {
		log.Info("aggregator HTTP server stopped cleanly")
	}
}

func hasReplicaUnits(specs []repository.ShardSpec) bool {
	for _, s := range specs {
		if len(s.Units) > 1 {
			return true
		}
	}
	return false
}

func runReplicaSyncLoop(ctx context.Context, repo *repository.HazardRepository, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := repo.SyncShardReplicas(ctx); err != nil {
				log.Warn("replica sync cycle finished with degraded units", "error", err)
			}
		}
	}
}

func connectShardsWithRetry(
	ctx context.Context,
	specs []repository.ShardSpec,
	maxConns, minConns int,
	log *slog.Logger,
) (*repository.HazardRepository, *pgxpool.Pool, error) {
	var err error

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for attempt := 1; attempt <= 15; attempt++ {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		var repo *repository.HazardRepository
		var primaryPool *pgxpool.Pool
		repo, primaryPool, err = repository.DialShardedRepository(ctx, specs, maxConns, minConns, log)
		if err == nil {
			log.Info("successfully established canonical postgres storage shards",
				slog.Int("attempt", attempt),
				slog.Int("shard_count", len(specs)),
			)
			return repo, primaryPool, nil
		}

		log.Warn("database connection attempt failed, retrying in 2s...",
			slog.Int("attempt", attempt),
			slog.String("error", err.Error()),
		)

		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-ticker.C:
		}
	}

	return nil, nil, err
}
