package repository

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"bnpb/aggregator/internal/domain"

	"github.com/jackc/pgx/v5"
)

// SatelliteCheckpoint is the durable replay boundary of the satellite stream.
type SatelliteCheckpoint struct {
	Stream       string
	LastOffset   int64
	State        []byte // JSON snapshot of the wildfire engine
	FencingToken int64
	HolderID     string
	UpdatedAt    time.Time
}

// LoadSatelliteCheckpoint returns the furthest checkpoint held by the reachable
// coordinator units, or nil when none exists yet.
func (r *HazardRepository) LoadSatelliteCheckpoint(ctx context.Context, stream string) (*SatelliteCheckpoint, error) {
	coordUnits := r.coordinatorUnits()
	if len(coordUnits) == 0 {
		return nil, ErrAllStorageUnavailable
	}

	var best *SatelliteCheckpoint
	reachable := 0
	var firstErr error
	for _, u := range coordUnits {
		qCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		cp, err := u.Backend.GetSatelliteCheckpoint(qCtx, stream)
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		reachable++
		if cp == nil {
			continue
		}
		if best == nil || cp.LastOffset > best.LastOffset ||
			(cp.LastOffset == best.LastOffset && cp.FencingToken > best.FencingToken) {
			best = cp
		}
	}
	if reachable == 0 {
		return nil, fmt.Errorf("load satellite checkpoint: %w: %v", ErrAllStorageUnavailable, firstErr)
	}
	return best, nil
}

// SaveSatelliteCheckpoint stores the snapshot on the coordinator units. With a fence it is
// accepted only while the caller still holds the SATELIT lease. A stored checkpoint never
// moves backwards, so a stale writer cannot rewind the replay boundary.
func (r *HazardRepository) SaveSatelliteCheckpoint(ctx context.Context, cp SatelliteCheckpoint, fence *domain.WriteFence) error {
	if fence != nil {
		if err := r.validateCoordinatorFence(ctx, *fence); err != nil {
			return err
		}
		cp.FencingToken = fence.FencingToken
		cp.HolderID = fence.HolderID
	}

	coordUnits := r.coordinatorUnits()
	saved := 0
	var firstErr error
	for _, u := range coordUnits {
		wCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
		err := u.Backend.SaveSatelliteCheckpoint(wCtx, cp)
		cancel()
		if err != nil {
			r.markUnitStale(u.Unit, true)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		saved++
	}
	if saved == 0 {
		return fmt.Errorf("save satellite checkpoint: %w: %v", ErrAllStorageUnavailable, firstErr)
	}
	return nil
}

func (p *postgresUnitBackend) GetSatelliteCheckpoint(ctx context.Context, stream string) (*SatelliteCheckpoint, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}
	cp := SatelliteCheckpoint{Stream: stream}
	err = pool.QueryRow(ctx, `
		SELECT last_offset, state, fencing_token, holder_id, updated_at
		FROM satellite_checkpoints
		WHERE stream = $1;
	`, stream).Scan(&cp.LastOffset, &cp.State, &cp.FencingToken, &cp.HolderID, &cp.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load satellite checkpoint: %w", err)
	}
	cp.UpdatedAt = cp.UpdatedAt.UTC()
	return &cp, nil
}

func (p *postgresUnitBackend) SaveSatelliteCheckpoint(ctx context.Context, cp SatelliteCheckpoint) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	tag, err := pool.Exec(ctx, `
		INSERT INTO satellite_checkpoints (stream, last_offset, state, fencing_token, holder_id, updated_at)
		VALUES ($1, $2, $3, $4, $5, clock_timestamp())
		ON CONFLICT (stream) DO UPDATE SET
			last_offset = EXCLUDED.last_offset,
			state = EXCLUDED.state,
			fencing_token = EXCLUDED.fencing_token,
			holder_id = EXCLUDED.holder_id,
			updated_at = clock_timestamp()
		WHERE satellite_checkpoints.last_offset <= EXCLUDED.last_offset
		  AND satellite_checkpoints.fencing_token <= EXCLUDED.fencing_token;
	`, cp.Stream, cp.LastOffset, cp.State, cp.FencingToken, cp.HolderID)
	if err != nil {
		return fmt.Errorf("save satellite checkpoint: %w", err)
	}
	if tag.RowsAffected() == 0 && cp.FencingToken > 0 {
		return ErrStaleFencingToken
	}
	return nil
}

func (m *MemoryUnitBackend) GetSatelliteCheckpoint(ctx context.Context, stream string) (*SatelliteCheckpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkHealth(ctx); err != nil {
		return nil, err
	}
	cp, ok := m.satelliteCheckpoints[stream]
	if !ok {
		return nil, nil
	}
	cp.State = slices.Clone(cp.State)
	return &cp, nil
}

func (m *MemoryUnitBackend) SaveSatelliteCheckpoint(ctx context.Context, cp SatelliteCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkHealth(ctx); err != nil {
		return err
	}
	if existing, ok := m.satelliteCheckpoints[cp.Stream]; ok &&
		(existing.LastOffset > cp.LastOffset || existing.FencingToken > cp.FencingToken) {
		return nil
	}
	cp.State = slices.Clone(cp.State)
	cp.UpdatedAt = m.nowLocked()
	m.satelliteCheckpoints[cp.Stream] = cp
	return nil
}
