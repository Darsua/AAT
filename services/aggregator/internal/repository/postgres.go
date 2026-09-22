package repository

import (
	"context"
	"fmt"
	"time"

	"bnpb/aggregator/migrations"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPostgresPool establishes a connection pool to PostgreSQL with production-tuned settings.
func NewPostgresPool(ctx context.Context, connString string, maxConns, minConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database configuration: %w", err)
	}

	// Concurrency and resource management settings
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	} else {
		cfg.MaxConns = 25
	}
	if minConns > 0 {
		cfg.MinConns = int32(minConns)
	} else {
		cfg.MinConns = 5
	}
	cfg.MaxConnLifetime = 1 * time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	cfg.HealthCheckPeriod = 15 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	// Verify connectivity
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	return pool, nil
}

// RunMigrations applies the embedded 001_init.sql schema and seed data idempotently
// using PostgreSQL's simple query protocol to safely execute multi-statement SQL.
func RunMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if migrations.InitSQL == "" {
		return fmt.Errorf("embedded migration SQL is empty")
	}
	if pool == nil {
		return fmt.Errorf("database connection pool cannot be nil")
	}

	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := pool.Acquire(execCtx)
	if err != nil {
		return fmt.Errorf("failed to acquire database connection for migrations: %w", err)
	}
	defer conn.Release()

	_, err = conn.Conn().PgConn().Exec(execCtx, migrations.InitSQL).ReadAll()
	if err != nil {
		return fmt.Errorf("failed to execute migrations: %w", err)
	}

	return nil
}
