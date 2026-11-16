package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// SatelliteCheckpoint is the durable replay boundary of the satellite stream.
type SatelliteCheckpoint struct {
	LastOffset int64
	State      []byte // JSON snapshot of the wildfire engine
	UpdatedAt  time.Time
}

// LoadSatelliteCheckpoint returns the stored checkpoint, or nil when none exists yet.
func (r *HazardRepository) LoadSatelliteCheckpoint(ctx context.Context, stream string) (*SatelliteCheckpoint, error) {
	var cp SatelliteCheckpoint
	err := r.pool.QueryRow(ctx,
		`SELECT last_offset, state, updated_at FROM satellite_checkpoints WHERE stream = $1`, stream,
	).Scan(&cp.LastOffset, &cp.State, &cp.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load satellite checkpoint: %w", err)
	}
	return &cp, nil
}

// SaveSatelliteCheckpoint stores the snapshot for lastOffset. It never moves a stored
// checkpoint backwards, so a stale writer cannot rewind the replay boundary.
func (r *HazardRepository) SaveSatelliteCheckpoint(ctx context.Context, stream string, lastOffset int64, state []byte) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO satellite_checkpoints (stream, last_offset, state, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (stream) DO UPDATE SET
			last_offset = EXCLUDED.last_offset,
			state = EXCLUDED.state,
			updated_at = NOW()
		WHERE satellite_checkpoints.last_offset <= EXCLUDED.last_offset;
	`, stream, lastOffset, state)
	if err != nil {
		return fmt.Errorf("save satellite checkpoint: %w", err)
	}
	return nil
}
