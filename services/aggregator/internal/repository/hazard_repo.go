package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"bnpb/aggregator/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HazardFilter defines query filters for retrieving canonical hazard events.
type HazardFilter struct {
	Since    *time.Time
	Type     *domain.HazardType
	Severity *domain.Severity
	Limit    int
	Offset   int
}

// HazardRepository provides database operations for canonical hazards.
type HazardRepository struct {
	pool *pgxpool.Pool
}

// NewHazardRepository creates a new HazardRepository.
func NewHazardRepository(pool *pgxpool.Pool) *HazardRepository {
	return &HazardRepository{pool: pool}
}

// QueryHazards retrieves paginated hazard events matching the given criteria, along with the total count.
func (r *HazardRepository) QueryHazards(ctx context.Context, filter HazardFilter) ([]domain.HazardEvent, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	} else if filter.Limit > 500 {
		filter.Limit = 500
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	whereClauses := []string{"1=1"}
	var args []any
	argIdx := 1

	if filter.Since != nil && !filter.Since.IsZero() {
		whereClauses = append(whereClauses, fmt.Sprintf("occurred_at >= $%d", argIdx))
		args = append(args, *filter.Since)
		argIdx++
	}

	if filter.Type != nil && *filter.Type != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("hazard_type = $%d", argIdx))
		args = append(args, string(*filter.Type))
		argIdx++
	}

	if filter.Severity != nil && *filter.Severity != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("severity = $%d", argIdx))
		args = append(args, string(*filter.Severity))
		argIdx++
	}

	whereSQL := strings.Join(whereClauses, " AND ")

	// 1. Total matching count
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM hazards WHERE %s;", whereSQL)
	var total int
	if err := r.pool.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count hazards: %w", err)
	}

	if total == 0 {
		return []domain.HazardEvent{}, 0, nil
	}

	// 2. Paginated data fetch
	dataSQL := fmt.Sprintf(`
		SELECT 
			hazard_id, source, source_ref_id, hazard_type, severity, 
			area_name, latitude, longitude, attributes, occurred_at, ingested_at
		FROM hazards
		WHERE %s
		ORDER BY occurred_at DESC
		LIMIT $%d OFFSET $%d;
	`, whereSQL, argIdx, argIdx+1)

	queryArgs := make([]any, 0, len(args)+2)
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, filter.Limit, filter.Offset)

	rows, err := r.pool.Query(ctx, dataSQL, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query hazards: %w", err)
	}
	defer rows.Close()

	events := make([]domain.HazardEvent, 0, filter.Limit)
	for rows.Next() {
		var h domain.HazardEvent
		var rawAttrs []byte
		var srcStr, typeStr, sevStr string

		if err := rows.Scan(
			&h.HazardID,
			&srcStr,
			&h.SourceRefID,
			&typeStr,
			&sevStr,
			&h.AreaName,
			&h.Latitude,
			&h.Longitude,
			&rawAttrs,
			&h.OccurredAt,
			&h.IngestedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("failed to scan hazard row: %w", err)
		}

		h.Source = domain.Source(srcStr)
		h.HazardType = domain.HazardType(typeStr)
		h.Severity = domain.Severity(sevStr)

		if len(rawAttrs) > 0 {
			if err := json.Unmarshal(rawAttrs, &h.Attributes); err != nil {
				return nil, 0, fmt.Errorf("failed to decode hazard attributes for %s: %w", h.HazardID, err)
			}
		} else {
			h.Attributes = make(map[string]any)
		}

		events = append(events, h)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error during hazards iteration: %w", err)
	}

	return events, total, nil
}

// SaveHazard persists or updates a hazard record idempotently using its (source, source_ref_id) constraint.
func (r *HazardRepository) SaveHazard(ctx context.Context, h *domain.HazardEvent) error {
	if h.Attributes == nil {
		h.Attributes = make(map[string]any)
	}

	attrsJSON, err := json.Marshal(h.Attributes)
	if err != nil {
		return fmt.Errorf("failed to marshal hazard attributes: %w", err)
	}

	sql := `
		INSERT INTO hazards (
			hazard_id, source, source_ref_id, hazard_type, severity, 
			area_name, latitude, longitude, attributes, occurred_at, ingested_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (source, source_ref_id) DO UPDATE SET
			severity = EXCLUDED.severity,
			area_name = EXCLUDED.area_name,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			attributes = EXCLUDED.attributes,
			occurred_at = EXCLUDED.occurred_at,
			ingested_at = EXCLUDED.ingested_at;
	`

	ingestedAt := h.IngestedAt
	if ingestedAt.IsZero() {
		ingestedAt = time.Now().UTC()
	}

	_, err = r.pool.Exec(ctx, sql,
		h.HazardID,
		string(h.Source),
		h.SourceRefID,
		string(h.HazardType),
		string(h.Severity),
		h.AreaName,
		h.Latitude,
		h.Longitude,
		attrsJSON,
		h.OccurredAt,
		ingestedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to save hazard %q: %w", h.HazardID, err)
	}

	return nil
}

// GetHazardByID retrieves a single hazard by its primary key ID.
func (r *HazardRepository) GetHazardByID(ctx context.Context, hazardID string) (*domain.HazardEvent, error) {
	sql := `
		SELECT 
			hazard_id, source, source_ref_id, hazard_type, severity, 
			area_name, latitude, longitude, attributes, occurred_at, ingested_at
		FROM hazards
		WHERE hazard_id = $1;
	`

	var h domain.HazardEvent
	var rawAttrs []byte
	var srcStr, typeStr, sevStr string

	err := r.pool.QueryRow(ctx, sql, hazardID).Scan(
		&h.HazardID,
		&srcStr,
		&h.SourceRefID,
		&typeStr,
		&sevStr,
		&h.AreaName,
		&h.Latitude,
		&h.Longitude,
		&rawAttrs,
		&h.OccurredAt,
		&h.IngestedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query hazard by id %q: %w", hazardID, err)
	}

	h.Source = domain.Source(srcStr)
	h.HazardType = domain.HazardType(typeStr)
	h.Severity = domain.Severity(sevStr)

	if len(rawAttrs) > 0 {
		if err := json.Unmarshal(rawAttrs, &h.Attributes); err != nil {
			return nil, fmt.Errorf("failed to decode hazard attributes for %s: %w", h.HazardID, err)
		}
	} else {
		h.Attributes = make(map[string]any)
	}

	return &h, nil
}
