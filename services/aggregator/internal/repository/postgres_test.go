package repository

import (
	"context"
	"testing"
	"time"

	"bnpb/aggregator/migrations"
)

func TestNewPostgresPool_InvalidConnString(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := NewPostgresPool(ctx, "invalid-postgres-url", 5, 2)
	if err == nil {
		t.Fatal("expected error when initializing pool with invalid connection string, got nil")
	}
}

func TestRunMigrations_EmptySQL(t *testing.T) {
	originalSQL := migrations.InitSQL
	migrations.InitSQL = ""
	defer func() {
		migrations.InitSQL = originalSQL
	}()

	err := RunMigrations(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error when migration SQL is empty, got nil")
	}
}

func TestRunMigrations_NilPool(t *testing.T) {
	err := RunMigrations(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error when pool is nil, got nil")
	}
}
