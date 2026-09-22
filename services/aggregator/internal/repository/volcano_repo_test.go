package repository

import (
	"context"
	"testing"

	"bnpb/aggregator/internal/domain"
)

func TestVolcanoRepository_CacheHit(t *testing.T) {
	repo := NewVolcanoRepository(nil) // pool is nil, but cache will be pre-populated

	repo.mu.Lock()
	repo.cache["VLC-001"] = domain.VolcanoReference{
		VolcanoID:   "VLC-001",
		VolcanoName: "Gunung Merapi",
		Latitude:    -7.5407,
		Longitude:   110.4456,
	}
	repo.mu.Unlock()

	volcano, err := repo.GetVolcanoByID(context.Background(), "VLC-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if volcano == nil {
		t.Fatal("expected volcano reference, got nil")
	}

	if volcano.VolcanoName != "Gunung Merapi" {
		t.Errorf("expected Gunung Merapi, got %s", volcano.VolcanoName)
	}
}

func TestVolcanoRepository_GetAllVolcanoes_FromCache(t *testing.T) {
	repo := NewVolcanoRepository(nil)

	repo.mu.Lock()
	repo.cache["VLC-001"] = domain.VolcanoReference{
		VolcanoID:   "VLC-001",
		VolcanoName: "Gunung Merapi",
		Latitude:    -7.5407,
		Longitude:   110.4456,
	}
	repo.cache["VLC-002"] = domain.VolcanoReference{
		VolcanoID:   "VLC-002",
		VolcanoName: "Gunung Anak Krakatau",
		Latitude:    -6.1020,
		Longitude:   105.4230,
	}
	repo.mu.Unlock()

	all, err := repo.GetAllVolcanoes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(all) != 2 {
		t.Errorf("expected 2 volcanoes, got %d", len(all))
	}
}
