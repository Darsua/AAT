package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"bnpb/aggregator/internal/domain"

	"github.com/jackc/pgx/v5"
)

// LeaseKeyForSource returns the canonical leader_leases primary key for an ingestion source.
func LeaseKeyForSource(source domain.Source) string {
	return "source:" + string(source)
}

type leaseMirror interface {
	GetLease(ctx context.Context, leaseKey string) (*domain.LeaderLease, error)
	MirrorLease(ctx context.Context, lease domain.LeaderLease) error
}

func (r *HazardRepository) coordinatorUnits() []StorageUnit {
	r.mu.RLock()
	if len(r.shardIDs) == 0 {
		r.mu.RUnlock()
		return nil
	}
	group, ok := r.shards[r.shardIDs[0]]
	if !ok {
		r.mu.RUnlock()
		return nil
	}
	units := slices.Clone(group.Units)
	staleSnapshot := make(map[string]bool, len(units))
	for _, u := range units {
		staleSnapshot[u.Unit] = r.staleUnits[u.Unit]
	}
	r.mu.RUnlock()

	slices.SortStableFunc(units, func(a, b StorageUnit) int {
		aStale := staleSnapshot[a.Unit]
		bStale := staleSnapshot[b.Unit]
		if aStale != bStale {
			if !aStale {
				return -1
			}
			return 1
		}
		if a.Role != b.Role {
			if a.Role == domain.StorageRolePrimary {
				return -1
			}
			return 1
		}
		if a.Unit < b.Unit {
			return -1
		}
		if a.Unit > b.Unit {
			return 1
		}
		return 0
	})
	return units
}

func betterLease(a, b domain.LeaderLease) bool {
	if a.FencingToken != b.FencingToken {
		return a.FencingToken > b.FencingToken
	}
	if !a.RenewedAt.Equal(b.RenewedAt) {
		return a.RenewedAt.After(b.RenewedAt)
	}
	return a.ExpiresAt.Before(b.ExpiresAt)
}

func (r *HazardRepository) reconcileCoordinatorLease(ctx context.Context, coordUnits []StorageUnit, leaseKey string) {
	var best *domain.LeaderLease
	for _, u := range coordUnits {
		m, ok := u.Backend.(leaseMirror)
		if !ok {
			continue
		}
		qCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		l, err := m.GetLease(qCtx, leaseKey)
		cancel()
		if err != nil {
			r.markUnitStale(u.Unit, true)
			continue
		}
		if l != nil && (best == nil || betterLease(*l, *best)) {
			cp := *l
			best = &cp
		}
	}
	if best == nil {
		return
	}
	for _, u := range coordUnits {
		m, ok := u.Backend.(leaseMirror)
		if !ok {
			continue
		}
		mCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		err := m.MirrorLease(mCtx, *best)
		cancel()
		if err != nil {
			r.markUnitStale(u.Unit, true)
		} else {
			r.markUnitStale(u.Unit, false)
		}
	}
}

// AcquireOrRenewSourceLease acquires or renews the distributed leader lease for a polling source using coordinator storage time.
func (r *HazardRepository) AcquireOrRenewSourceLease(
	ctx context.Context,
	source domain.Source,
	holderID string,
	currentToken int64,
	ttl time.Duration,
) (domain.LeaderLease, bool, error) {
	coordUnits := r.coordinatorUnits()
	if len(coordUnits) == 0 {
		return domain.LeaderLease{}, false, ErrAllStorageUnavailable
	}

	minToken := currentToken
	if observed := r.maxObservedSourceFence(ctx, source); observed > minToken {
		minToken = observed
	}

	leaseKey := LeaseKeyForSource(source)
	r.reconcileCoordinatorLease(ctx, coordUnits, leaseKey)
	coordUnits = r.coordinatorUnits()

	var (
		lease    domain.LeaderLease
		acquired bool
		granted  bool
		lastErr  error
	)

	for _, u := range coordUnits {
		callCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		l, ok, err := u.Backend.AcquireOrRenewLease(callCtx, leaseKey, holderID, minToken, ttl)
		cancel()
		if err != nil {
			r.markUnitStale(u.Unit, true)
			lastErr = err
			continue
		}
		lease = l
		acquired = ok
		granted = true
		break
	}
	if !granted {
		return domain.LeaderLease{}, false, fmt.Errorf("%w: %v", ErrAllStorageUnavailable, lastErr)
	}

	for _, u := range coordUnits {
		if m, ok := u.Backend.(leaseMirror); ok {
			mCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
			mErr := m.MirrorLease(mCtx, lease)
			cancel()
			if mErr != nil {
				r.markUnitStale(u.Unit, true)
			} else {
				r.markUnitStale(u.Unit, false)
			}
		}
	}

	if !acquired {
		return lease, false, nil
	}

	// Advance monotonic fence high-water mark across every reachable storage unit so stale leaders cannot commit anywhere.
	for _, u := range r.allUnits() {
		fCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
		fErr := u.Backend.AdvanceSourceFence(fCtx, source, lease.FencingToken, holderID)
		cancel()
		if errors.Is(fErr, ErrStaleFencingToken) {
			return domain.LeaderLease{}, false, ErrStaleFencingToken
		}
	}

	return lease, true, nil
}

func (r *HazardRepository) maxObservedSourceFence(ctx context.Context, source domain.Source) int64 {
	var maxToken int64
	for _, u := range r.allUnits() {
		qCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		tok, err := u.Backend.GetMaxSourceFence(qCtx, source)
		cancel()
		if err == nil && tok > maxToken {
			maxToken = tok
		}
	}
	return maxToken
}

// ReleaseSourceLease voluntarily relinquishes an active lease so standby replicas can take over immediately.
func (r *HazardRepository) ReleaseSourceLease(ctx context.Context, source domain.Source, holderID string, fencingToken int64) error {
	leaseKey := LeaseKeyForSource(source)
	coordUnits := r.coordinatorUnits()
	if len(coordUnits) == 0 {
		return nil
	}
	var lastErr error
	released := 0
	for _, u := range coordUnits {
		rCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
		err := u.Backend.ReleaseLease(rCtx, leaseKey, holderID, fencingToken)
		cancel()
		if err == nil {
			released++
		} else {
			lastErr = err
		}
	}
	if released == 0 && lastErr != nil {
		return lastErr
	}
	return nil
}

func (r *HazardRepository) validateCoordinatorFence(ctx context.Context, fence domain.WriteFence) error {
	coordUnits := r.coordinatorUnits()
	if len(coordUnits) == 0 {
		return ErrAllStorageUnavailable
	}
	r.reconcileCoordinatorLease(ctx, coordUnits, fence.LeaseKey)
	coordUnits = r.coordinatorUnits()
	var lastErr error
	for _, u := range coordUnits {
		vCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		err := u.Backend.ValidateLeaseFence(vCtx, fence)
		cancel()
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrStaleFencingToken) {
			return ErrStaleFencingToken
		}
		lastErr = err
	}
	return fmt.Errorf("%w: %v", ErrAllStorageUnavailable, lastErr)
}

// SaveHazardWithFence validates lease authority and persists a hazard + transactional outbox atomically if and only if
// the caller's fencing token is still current across both coordinator lease state and owning shard storage.
func (r *HazardRepository) SaveHazardWithFence(
	ctx context.Context,
	h *domain.HazardEvent,
	correlationID string,
	fence domain.WriteFence,
) error {
	if h == nil {
		return errors.New("hazard event cannot be nil")
	}
	if fence.Source == "" {
		fence.Source = h.Source
	}
	if fence.LeaseKey == "" {
		fence.LeaseKey = LeaseKeyForSource(fence.Source)
	}
	if err := r.validateCoordinatorFence(ctx, fence); err != nil {
		return err
	}

	if h.Attributes == nil {
		h.Attributes = make(map[string]any)
	}
	h.OccurredAt = h.OccurredAt.UTC()

	group, ok := r.shardForKey(h.HazardID)
	if !ok {
		return ErrAllStorageUnavailable
	}
	r.resolveConsistentIngestedAt(ctx, group, h)

	type writeOutcome struct {
		unit StorageUnit
		err  error
	}
	outcomes := make([]writeOutcome, len(group.Units))
	var wg sync.WaitGroup
	wg.Add(len(group.Units))

	for i, u := range group.Units {
		go func(idx int, unit StorageUnit) {
			defer wg.Done()
			wCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
			defer cancel()

			copyEvent := cloneHazardEvent(*h)
			withOutbox := unit.Role == domain.StorageRolePrimary
			err := unit.Backend.SaveHazardWithFence(wCtx, &copyEvent, correlationID, fence, withOutbox)
			outcomes[idx] = writeOutcome{unit: unit, err: err}
		}(i, u)
	}
	wg.Wait()

	var succeeded []StorageUnit
	var failed []StorageUnit
	primaryOutboxWritten := false
	var firstErr error

	for _, out := range outcomes {
		if errors.Is(out.err, ErrStaleFencingToken) {
			return ErrStaleFencingToken
		}
		if out.err == nil {
			succeeded = append(succeeded, out.unit)
			if out.unit.Role == domain.StorageRolePrimary {
				primaryOutboxWritten = true
			}
		} else {
			failed = append(failed, out.unit)
			if firstErr == nil {
				firstErr = out.err
			}
		}
	}

	if len(succeeded) == 0 {
		return fmt.Errorf("all units in shard %q failed fenced write for %q: %w", group.ShardID, h.HazardID, firstErr)
	}

	if !primaryOutboxWritten {
		promoted := succeeded[0]
		wCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
		copyEvent := cloneHazardEvent(*h)
		pErr := promoted.Backend.SaveHazardWithFence(wCtx, &copyEvent, correlationID, fence, true)
		cancel()
		if pErr != nil {
			return fmt.Errorf("replica promotion fenced outbox write failed on unit %q: %w", promoted.Unit, pErr)
		}
	}

	for _, u := range succeeded {
		r.markUnitStale(u.Unit, false)
	}
	for _, u := range failed {
		r.markUnitStale(u.Unit, true)
	}
	return nil
}

// GetSourceCheckpoint retrieves the most recent durable source checkpoint across reachable storage units.
func (r *HazardRepository) GetSourceCheckpoint(ctx context.Context, source domain.Source) (*domain.SourceCheckpoint, error) {
	coordUnits := r.coordinatorUnits()
	if len(coordUnits) == 0 {
		return nil, ErrAllStorageUnavailable
	}

	var best *domain.SourceCheckpoint
	reachable := 0
	var firstErr error
	for _, u := range coordUnits {
		qCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		cp, err := u.Backend.GetSourceCheckpoint(qCtx, source)
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
		if best == nil || cp.FencingToken > best.FencingToken ||
			(cp.FencingToken == best.FencingToken && cp.LastOccurredAt.After(best.LastOccurredAt)) {
			copyCP := *cp
			best = &copyCP
		}
	}
	if reachable == 0 && firstErr != nil {
		return nil, firstErr
	}
	return best, nil
}

// SaveSourceCheckpoint persists a fenced durable source checkpoint on the coordinator shard units.
func (r *HazardRepository) SaveSourceCheckpoint(ctx context.Context, cp domain.SourceCheckpoint) error {
	fence := domain.WriteFence{
		LeaseKey:     LeaseKeyForSource(cp.Source),
		Source:       cp.Source,
		HolderID:     cp.HolderID,
		FencingToken: cp.FencingToken,
	}
	if err := r.validateCoordinatorFence(ctx, fence); err != nil {
		return err
	}

	coordUnits := r.coordinatorUnits()
	saved := 0
	var firstErr error
	for _, u := range coordUnits {
		wCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
		err := u.Backend.SaveSourceCheckpoint(wCtx, cp)
		cancel()
		if errors.Is(err, ErrStaleFencingToken) {
			return ErrStaleFencingToken
		}
		if err == nil {
			saved++
		} else if firstErr == nil {
			firstErr = err
		}
	}
	if saved == 0 {
		return fmt.Errorf("failed saving checkpoint for %s: %w", cp.Source, firstErr)
	}
	return nil
}

// ==============================================================================
// PostgreSQL UnitBackend Lease, Fencing & Checkpoint Implementation
// ==============================================================================

func (p *postgresUnitBackend) AcquireOrRenewLease(
	ctx context.Context,
	leaseKey, holderID string,
	minToken int64,
	ttl time.Duration,
) (domain.LeaderLease, bool, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return domain.LeaderLease{}, false, err
	}
	ttlMs := ttl.Milliseconds()
	if ttlMs < 1 {
		ttlMs = 1
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return domain.LeaderLease{}, false, fmt.Errorf("begin lease tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var existing domain.LeaderLease
	var expired bool
	qErr := tx.QueryRow(ctx, `
		SELECT lease_key, holder_id, fencing_token, acquired_at, renewed_at, expires_at, (expires_at < clock_timestamp()) AS expired
		FROM leader_leases
		WHERE lease_key = $1
		FOR UPDATE;
	`, leaseKey).Scan(
		&existing.LeaseKey,
		&existing.HolderID,
		&existing.FencingToken,
		&existing.AcquiredAt,
		&existing.RenewedAt,
		&existing.ExpiresAt,
		&expired,
	)

	var out domain.LeaderLease
	if errors.Is(qErr, pgx.ErrNoRows) {
		nextToken := minToken + 1
		if nextToken < 1 {
			nextToken = 1
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO leader_leases (lease_key, holder_id, fencing_token, acquired_at, renewed_at, expires_at)
			VALUES ($1, $2, $3, clock_timestamp(), clock_timestamp(), clock_timestamp() + ($4 * INTERVAL '1 millisecond'))
			RETURNING lease_key, holder_id, fencing_token, acquired_at, renewed_at, expires_at;
		`, leaseKey, holderID, nextToken, ttlMs).Scan(
			&out.LeaseKey, &out.HolderID, &out.FencingToken, &out.AcquiredAt, &out.RenewedAt, &out.ExpiresAt,
		)
		if err != nil {
			return domain.LeaderLease{}, false, fmt.Errorf("insert leader lease: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.LeaderLease{}, false, err
		}
		return out, true, nil
	}
	if qErr != nil {
		return domain.LeaderLease{}, false, fmt.Errorf("query leader lease: %w", qErr)
	}

	if expired {
		nextToken := existing.FencingToken + 1
		if minToken >= nextToken {
			nextToken = minToken + 1
		}
		err = tx.QueryRow(ctx, `
			UPDATE leader_leases
			SET holder_id = $2,
				fencing_token = $3,
				acquired_at = clock_timestamp(),
				renewed_at = clock_timestamp(),
				expires_at = clock_timestamp() + ($4 * INTERVAL '1 millisecond')
			WHERE lease_key = $1
			RETURNING lease_key, holder_id, fencing_token, acquired_at, renewed_at, expires_at;
		`, leaseKey, holderID, nextToken, ttlMs).Scan(
			&out.LeaseKey, &out.HolderID, &out.FencingToken, &out.AcquiredAt, &out.RenewedAt, &out.ExpiresAt,
		)
		if err != nil {
			return domain.LeaderLease{}, false, fmt.Errorf("takeover expired leader lease: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.LeaderLease{}, false, err
		}
		return out, true, nil
	}

	if existing.HolderID == holderID && (minToken == 0 || existing.FencingToken == minToken) {
		err = tx.QueryRow(ctx, `
			UPDATE leader_leases
			SET renewed_at = clock_timestamp(),
				expires_at = clock_timestamp() + ($2 * INTERVAL '1 millisecond')
			WHERE lease_key = $1
			RETURNING lease_key, holder_id, fencing_token, acquired_at, renewed_at, expires_at;
		`, leaseKey, ttlMs).Scan(
			&out.LeaseKey, &out.HolderID, &out.FencingToken, &out.AcquiredAt, &out.RenewedAt, &out.ExpiresAt,
		)
		if err != nil {
			return domain.LeaderLease{}, false, fmt.Errorf("renew leader lease: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.LeaderLease{}, false, err
		}
		return out, true, nil
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.LeaderLease{}, false, err
	}
	return existing, false, nil
}

func (p *postgresUnitBackend) GetLease(ctx context.Context, leaseKey string) (*domain.LeaderLease, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}
	var out domain.LeaderLease
	err = pool.QueryRow(ctx, `
		SELECT lease_key, holder_id, fencing_token, acquired_at, renewed_at, expires_at
		FROM leader_leases
		WHERE lease_key = $1;
	`, leaseKey).Scan(
		&out.LeaseKey,
		&out.HolderID,
		&out.FencingToken,
		&out.AcquiredAt,
		&out.RenewedAt,
		&out.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *postgresUnitBackend) MirrorLease(ctx context.Context, lease domain.LeaderLease) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO leader_leases (lease_key, holder_id, fencing_token, acquired_at, renewed_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (lease_key) DO UPDATE SET
			holder_id = EXCLUDED.holder_id,
			fencing_token = EXCLUDED.fencing_token,
			acquired_at = EXCLUDED.acquired_at,
			renewed_at = EXCLUDED.renewed_at,
			expires_at = EXCLUDED.expires_at
		WHERE leader_leases.fencing_token <= EXCLUDED.fencing_token;
	`, lease.LeaseKey, lease.HolderID, lease.FencingToken, lease.AcquiredAt.UTC(), lease.RenewedAt.UTC(), lease.ExpiresAt.UTC())
	return err
}

func (p *postgresUnitBackend) ReleaseLease(ctx context.Context, leaseKey, holderID string, fencingToken int64) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
		UPDATE leader_leases
		SET expires_at = clock_timestamp() - INTERVAL '1 millisecond'
		WHERE lease_key = $1 AND holder_id = $2 AND ($3 = 0 OR fencing_token = $3);
	`, leaseKey, holderID, fencingToken)
	return err
}

func (p *postgresUnitBackend) ValidateLeaseFence(ctx context.Context, fence domain.WriteFence) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	if fence.FencingToken <= 0 {
		return ErrStaleFencingToken
	}

	var ok int
	err = pool.QueryRow(ctx, `
		SELECT 1
		FROM leader_leases
		WHERE lease_key = $1 AND holder_id = $2 AND fencing_token = $3 AND expires_at >= clock_timestamp();
	`, fence.LeaseKey, fence.HolderID, fence.FencingToken).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStaleFencingToken
	}
	if err != nil {
		return err
	}

	if fence.Source != "" {
		maxTok, mErr := p.GetMaxSourceFence(ctx, fence.Source)
		if mErr != nil {
			return mErr
		}
		if maxTok > fence.FencingToken {
			return ErrStaleFencingToken
		}
	}
	return nil
}

func (p *postgresUnitBackend) AdvanceSourceFence(ctx context.Context, source domain.Source, fencingToken int64, holderID string) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	if fencingToken <= 0 {
		return ErrStaleFencingToken
	}
	tag, err := pool.Exec(ctx, `
		INSERT INTO source_fences (source, max_fencing_token, holder_id, updated_at)
		VALUES ($1, $2, $3, clock_timestamp())
		ON CONFLICT (source) DO UPDATE SET
			max_fencing_token = EXCLUDED.max_fencing_token,
			holder_id = EXCLUDED.holder_id,
			updated_at = clock_timestamp()
		WHERE source_fences.max_fencing_token <= EXCLUDED.max_fencing_token;
	`, string(source), fencingToken, holderID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleFencingToken
	}
	return nil
}

func (p *postgresUnitBackend) GetMaxSourceFence(ctx context.Context, source domain.Source) (int64, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return 0, err
	}
	var token int64
	err = pool.QueryRow(ctx, `SELECT max_fencing_token FROM source_fences WHERE source = $1;`, string(source)).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return token, err
}

func (p *postgresUnitBackend) SaveHazardWithFence(
	ctx context.Context,
	h *domain.HazardEvent,
	correlationID string,
	fence domain.WriteFence,
	withOutbox bool,
) error {
	if fence.FencingToken <= 0 {
		return ErrStaleFencingToken
	}
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	if h.Attributes == nil {
		h.Attributes = make(map[string]any)
	}
	attrsJSON, err := json.Marshal(h.Attributes)
	if err != nil {
		return fmt.Errorf("failed to marshal hazard attributes: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fenced save tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	fenceTag, err := tx.Exec(ctx, `
		INSERT INTO source_fences (source, max_fencing_token, holder_id, updated_at)
		VALUES ($1, $2, $3, clock_timestamp())
		ON CONFLICT (source) DO UPDATE SET
			max_fencing_token = EXCLUDED.max_fencing_token,
			holder_id = EXCLUDED.holder_id,
			updated_at = clock_timestamp()
		WHERE source_fences.max_fencing_token <= EXCLUDED.max_fencing_token;
	`, string(h.Source), fence.FencingToken, fence.HolderID)
	if err != nil {
		return fmt.Errorf("enforce source fence: %w", err)
	}
	if fenceTag.RowsAffected() == 0 {
		return ErrStaleFencingToken
	}

	var existingIngestedAt time.Time
	err = tx.QueryRow(ctx,
		"SELECT ingested_at FROM hazards WHERE hazard_id = $1 OR (source = $2 AND source_ref_id = $3) LIMIT 1;",
		h.HazardID, string(h.Source), h.SourceRefID,
	).Scan(&existingIngestedAt)
	if err == nil && !existingIngestedAt.IsZero() {
		h.IngestedAt = existingIngestedAt.UTC()
	} else if h.IngestedAt.IsZero() {
		h.IngestedAt = time.Now().UTC()
	} else {
		h.IngestedAt = h.IngestedAt.UTC()
	}

	hazardTag, err := tx.Exec(ctx, `
		INSERT INTO hazards (
			hazard_id, source, source_ref_id, hazard_type, severity,
			area_name, latitude, longitude, attributes, occurred_at, ingested_at, fencing_token
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (source, source_ref_id) DO UPDATE SET
			severity = EXCLUDED.severity,
			area_name = EXCLUDED.area_name,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			attributes = EXCLUDED.attributes,
			occurred_at = EXCLUDED.occurred_at,
			fencing_token = EXCLUDED.fencing_token
		WHERE hazards.fencing_token <= EXCLUDED.fencing_token;
	`,
		h.HazardID,
		string(h.Source),
		h.SourceRefID,
		string(h.HazardType),
		string(h.Severity),
		h.AreaName,
		h.Latitude,
		h.Longitude,
		attrsJSON,
		h.OccurredAt.UTC(),
		h.IngestedAt,
		fence.FencingToken,
	)
	if err != nil {
		return fmt.Errorf("fenced upsert hazard %q: %w", h.HazardID, err)
	}
	if hazardTag.RowsAffected() == 0 {
		return ErrStaleFencingToken
	}

	if withOutbox {
		payloadJSON, err := json.Marshal(h)
		if err != nil {
			return fmt.Errorf("failed to marshal fenced outbox payload: %w", err)
		}
		outboxEventID := formatOutboxEventID(h.Source, h.SourceRefID)
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (event_id, event_type, payload, correlation_id, published)
			VALUES ($1, $2, $3, $4, FALSE)
			ON CONFLICT (event_id) DO UPDATE SET
				payload = EXCLUDED.payload,
				correlation_id = EXCLUDED.correlation_id,
				published = FALSE,
				published_at = NULL,
				claim_expires_at = NULL
			WHERE outbox_events.payload != EXCLUDED.payload;
		`, outboxEventID, "hazard_event", payloadJSON, correlationID)
		if err != nil {
			return fmt.Errorf("failed to upsert outbox event for %q: %w", h.HazardID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fenced write for %q: %w", h.HazardID, err)
	}
	return nil
}

func (p *postgresUnitBackend) GetSourceCheckpoint(ctx context.Context, source domain.Source) (*domain.SourceCheckpoint, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}
	var cp domain.SourceCheckpoint
	var srcStr string
	err = pool.QueryRow(ctx, `
		SELECT source, last_occurred_at, last_source_ref_id, fencing_token, holder_id, updated_at
		FROM source_checkpoints
		WHERE source = $1;
	`, string(source)).Scan(
		&srcStr,
		&cp.LastOccurredAt,
		&cp.LastSourceRefID,
		&cp.FencingToken,
		&cp.HolderID,
		&cp.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cp.Source = domain.Source(srcStr)
	cp.LastOccurredAt = cp.LastOccurredAt.UTC()
	cp.UpdatedAt = cp.UpdatedAt.UTC()
	return &cp, nil
}

func (p *postgresUnitBackend) SaveSourceCheckpoint(ctx context.Context, cp domain.SourceCheckpoint) error {
	if cp.FencingToken <= 0 {
		return ErrStaleFencingToken
	}
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	fenceTag, err := tx.Exec(ctx, `
		INSERT INTO source_fences (source, max_fencing_token, holder_id, updated_at)
		VALUES ($1, $2, $3, clock_timestamp())
		ON CONFLICT (source) DO UPDATE SET
			max_fencing_token = EXCLUDED.max_fencing_token,
			holder_id = EXCLUDED.holder_id,
			updated_at = clock_timestamp()
		WHERE source_fences.max_fencing_token <= EXCLUDED.max_fencing_token;
	`, string(cp.Source), cp.FencingToken, cp.HolderID)
	if err != nil {
		return err
	}
	if fenceTag.RowsAffected() == 0 {
		return ErrStaleFencingToken
	}

	cpTag, err := tx.Exec(ctx, `
		INSERT INTO source_checkpoints (source, last_occurred_at, last_source_ref_id, fencing_token, holder_id, updated_at)
		VALUES ($1, $2, $3, $4, $5, clock_timestamp())
		ON CONFLICT (source) DO UPDATE SET
			last_occurred_at = GREATEST(source_checkpoints.last_occurred_at, EXCLUDED.last_occurred_at),
			last_source_ref_id = CASE
				WHEN EXCLUDED.last_occurred_at >= source_checkpoints.last_occurred_at THEN EXCLUDED.last_source_ref_id
				ELSE source_checkpoints.last_source_ref_id
			END,
			fencing_token = EXCLUDED.fencing_token,
			holder_id = EXCLUDED.holder_id,
			updated_at = clock_timestamp()
		WHERE source_checkpoints.fencing_token <= EXCLUDED.fencing_token;
	`, string(cp.Source), cp.LastOccurredAt.UTC(), cp.LastSourceRefID, cp.FencingToken, cp.HolderID)
	if err != nil {
		return err
	}
	if cpTag.RowsAffected() == 0 {
		return ErrStaleFencingToken
	}
	return tx.Commit(ctx)
}
