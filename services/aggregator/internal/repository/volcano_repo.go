package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"bnpb/aggregator/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// VolcanoRepository manages access to the static volcano catalog with an in-memory read cache.
type VolcanoRepository struct {
	pool  *pgxpool.Pool
	mu    sync.RWMutex
	cache map[string]domain.VolcanoReference
}

// NewVolcanoRepository instantiates a new VolcanoRepository.
func NewVolcanoRepository(pool *pgxpool.Pool) *VolcanoRepository {
	return &VolcanoRepository{
		pool:  pool,
		cache: make(map[string]domain.VolcanoReference),
	}
}

// LoadCache populates the in-memory cache with all volcano references from PostgreSQL.
func (r *VolcanoRepository) LoadCache(ctx context.Context) error {
	query := `SELECT volcano_id, volcano_name, latitude, longitude FROM volcano_references;`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query volcano references: %w", err)
	}
	defer rows.Close()

	tempMap := make(map[string]domain.VolcanoReference)
	for rows.Next() {
		var v domain.VolcanoReference
		if err := rows.Scan(&v.VolcanoID, &v.VolcanoName, &v.Latitude, &v.Longitude); err != nil {
			return fmt.Errorf("failed to scan volcano reference row: %w", err)
		}
		tempMap[v.VolcanoID] = v
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating volcano references: %w", err)
	}

	r.mu.Lock()
	r.cache = tempMap
	r.mu.Unlock()

	return nil
}

// GetVolcanoByID retrieves a volcano reference by ID, checking the in-memory cache first.
func (r *VolcanoRepository) GetVolcanoByID(ctx context.Context, id string) (*domain.VolcanoReference, error) {
	r.mu.RLock()
	val, found := r.cache[id]
	r.mu.RUnlock()

	if found {
		return &val, nil
	}

	// Fallback to database lookup if cache miss occurs
	query := `SELECT volcano_id, volcano_name, latitude, longitude FROM volcano_references WHERE volcano_id = $1;`
	var v domain.VolcanoReference
	err := r.pool.QueryRow(ctx, query, id).Scan(&v.VolcanoID, &v.VolcanoName, &v.Latitude, &v.Longitude)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Not found
		}
		return nil, fmt.Errorf("failed to query volcano by id %q: %w", id, err)
	}

	// Update cache
	r.mu.Lock()
	r.cache[id] = v
	r.mu.Unlock()

	return &v, nil
}

// GetAllVolcanoes returns all volcano references currently cached.
func (r *VolcanoRepository) GetAllVolcanoes(ctx context.Context) ([]domain.VolcanoReference, error) {
	r.mu.RLock()
	cacheLen := len(r.cache)
	r.mu.RUnlock()

	if cacheLen == 0 {
		if err := r.LoadCache(ctx); err != nil {
			return nil, err
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]domain.VolcanoReference, 0, len(r.cache))
	for _, v := range r.cache {
		result = append(result, v)
	}
	return result, nil
}
