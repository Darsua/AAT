package repository

import (
	"context"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
)

func TestSatelliteCheckpointOnCoordinatorUnits(t *testing.T) {
	ctx := context.Background()
	primary := NewMemoryUnitBackend()
	replica := NewMemoryUnitBackend()
	repo, err := NewShardedHazardRepository([]ShardGroup{{
		ShardID: "shard-1",
		Units: []StorageUnit{
			{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: primary},
			{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: replica},
		},
	}})
	if err != nil {
		t.Fatalf("NewShardedHazardRepository: %v", err)
	}

	lease, ok, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceSatelit, "agg-a", 0, time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire lease: %v %v", ok, err)
	}
	fence := &domain.WriteFence{
		LeaseKey: LeaseKeyForSource(domain.SourceSatelit), Source: domain.SourceSatelit,
		HolderID: "agg-a", FencingToken: lease.FencingToken,
	}

	if cp, err := repo.LoadSatelliteCheckpoint(ctx, "satellite"); err != nil || cp != nil {
		t.Fatalf("empty load = %+v, %v", cp, err)
	}
	if err := repo.SaveSatelliteCheckpoint(ctx, SatelliteCheckpoint{Stream: "satellite", LastOffset: 5, State: []byte(`{"n":5}`)}, fence); err != nil {
		t.Fatalf("save 5: %v", err)
	}

	// The checkpoint survives the loss of the coordinator primary.
	primary.SetHealthy(false)
	cp, err := repo.LoadSatelliteCheckpoint(ctx, "satellite")
	if err != nil || cp == nil || cp.LastOffset != 5 || cp.HolderID != "agg-a" || cp.FencingToken != lease.FencingToken {
		t.Fatalf("load with primary down = %+v, %v", cp, err)
	}
	if err := repo.SaveSatelliteCheckpoint(ctx, SatelliteCheckpoint{Stream: "satellite", LastOffset: 7, State: []byte(`{"n":7}`)}, fence); err != nil {
		t.Fatalf("save 7 with primary down: %v", err)
	}
	primary.SetHealthy(true)

	// The furthest copy wins even though the primary missed offset 7.
	cp, err = repo.LoadSatelliteCheckpoint(ctx, "satellite")
	if err != nil || cp == nil || cp.LastOffset != 7 {
		t.Fatalf("load after primary returns = %+v, %v", cp, err)
	}

	// A checkpoint never moves backwards.
	if err := repo.SaveSatelliteCheckpoint(ctx, SatelliteCheckpoint{Stream: "satellite", LastOffset: 6, State: []byte(`{"n":6}`)}, fence); err != nil {
		t.Fatalf("save 6: %v", err)
	}
	if cp, _ = repo.LoadSatelliteCheckpoint(ctx, "satellite"); cp.LastOffset != 7 {
		t.Fatalf("checkpoint moved backwards to %d", cp.LastOffset)
	}

	// A holder whose token was superseded cannot write a checkpoint.
	stale := *fence
	stale.HolderID, stale.FencingToken = "agg-old", lease.FencingToken-1
	err = repo.SaveSatelliteCheckpoint(ctx, SatelliteCheckpoint{Stream: "satellite", LastOffset: 9}, &stale)
	if err == nil {
		t.Fatalf("stale fence accepted")
	}
	if cp, _ = repo.LoadSatelliteCheckpoint(ctx, "satellite"); cp.LastOffset != 7 {
		t.Fatalf("stale holder moved the checkpoint to %d", cp.LastOffset)
	}
}
