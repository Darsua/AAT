package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bnpb/aggregator/internal/api"
	"bnpb/aggregator/internal/config"
	"bnpb/aggregator/internal/logger"
	"bnpb/aggregator/internal/repository"

	"github.com/jackc/pgx/v5/pgxpool"
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

	// Connect to PostgreSQL with retry backoff
	pool, err := connectDBWithRetry(bootCtx, cfg.DatabaseURL, cfg.DatabaseMaxConns, cfg.DatabaseMinConns, log)
	if err != nil {
		log.Error("fatal: could not connect to canonical database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Apply database migrations idempotently
	log.Info("applying database migrations")
	if err := repository.RunMigrations(bootCtx, pool); err != nil {
		log.Error("fatal: database migration failed", "error", err)
		os.Exit(1)
	}
	log.Info("database migrations applied successfully")

	// Initialize repositories
	volcanoRepo := repository.NewVolcanoRepository(pool)
	if err := volcanoRepo.LoadCache(bootCtx); err != nil {
		log.Warn("warning: could not pre-populate volcano reference cache", "error", err)
	} else {
		log.Info("volcano catalog cached into memory successfully")
	}

	hazardRepo := repository.NewHazardRepository(pool)

	// Register HTTP routes
	mux := http.NewServeMux()

	healthHandler := api.NewHealthHandler(pool)
	hazardHandler := api.NewHazardHandler(hazardRepo)

	mux.Handle("GET /health", healthHandler)
	mux.Handle("GET /internal/hazards", hazardHandler)

	// Middleware chaining: Recovery -> CorrelationID -> Logging -> Router
	var handler http.Handler = mux
	handler = api.LoggingMiddleware(handler)
	handler = api.CorrelationIDMiddleware(handler)
	handler = api.RecoveryMiddleware(handler)

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
	log.Info("shutting down aggregator server gracefully", slog.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("forced shutdown error", "error", err)
	} else {
		log.Info("aggregator HTTP server stopped cleanly")
	}
}

func connectDBWithRetry(ctx context.Context, connStr string, maxConns, minConns int, log *slog.Logger) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	var err error

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for attempt := 1; attempt <= 15; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		pool, err = repository.NewPostgresPool(ctx, connStr, maxConns, minConns)
		if err == nil {
			log.Info("successfully established canonical postgres connection pool", slog.Int("attempt", attempt))
			return pool, nil
		}

		log.Warn("database connection attempt failed, retrying in 2s...",
			slog.Int("attempt", attempt),
			slog.String("error", err.Error()),
		)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}

	return nil, err
}
