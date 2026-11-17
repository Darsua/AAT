package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
)

// TestSatelliteCheckpointPostgres runs against a disposable database when
// AAT_TEST_DATABASE_URL is set; it is skipped otherwise (CI has no Postgres).
func TestSatelliteCheckpointPostgres(t *testing.T) {
	dsn := os.Getenv("AAT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AAT_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := NewPostgresPool(ctx, dsn, 4, 1)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM satellite_checkpoints WHERE stream = 'test'`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	repo := NewHazardRepository(pool)

	if cp, err := repo.LoadSatelliteCheckpoint(ctx, "test"); err != nil || cp != nil {
		t.Fatalf("empty load = %+v, %v", cp, err)
	}
	if err := repo.SaveSatelliteCheckpoint(ctx, SatelliteCheckpoint{Stream: "test", LastOffset: 5, State: []byte(`{"detections":[]}`)}, nil); err != nil {
		t.Fatalf("save 5: %v", err)
	}
	if err := repo.SaveSatelliteCheckpoint(ctx, SatelliteCheckpoint{Stream: "test", LastOffset: 3, State: []byte(`{"stale":true}`)}, nil); err != nil {
		t.Fatalf("save stale: %v", err)
	}
	cp, err := repo.LoadSatelliteCheckpoint(ctx, "test")
	if err != nil || cp == nil || cp.LastOffset != 5 || string(cp.State) != `{"detections": []}` {
		t.Fatalf("checkpoint must not move backwards: %+v (%s), %v", cp, cp.State, err)
	}

	lat, lon := -2.5, 113.8
	h := &domain.HazardEvent{
		HazardID: "haz-satelit-fire-test", Source: "SATELIT", SourceRefID: "fire-test", HazardType: "WILDFIRE",
		Severity: domain.SeveritySiaga, AreaName: "test", OccurredAt: time.Date(2026, 11, 10, 1, 0, 0, 0, time.UTC),
		Latitude: &lat, Longitude: &lon,
		Attributes: map[string]any{"detection_count": 6, "lifecycle_status": "active"},
	}
	if err := repo.SaveHazardWithOutbox(ctx, h, "satelit-1"); err != nil {
		t.Fatalf("save wildfire hazard: %v", err)
	}
	got, err := repo.GetHazardBySourceRef(ctx, "SATELIT", "fire-test")
	if err != nil || got == nil || got.HazardType != "WILDFIRE" {
		t.Fatalf("stored wildfire = %+v, %v", got, err)
	}
}
