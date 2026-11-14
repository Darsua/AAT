package repository

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
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

// AuditEventsFilter defines query parameters for GET /internal/audit/events.
type AuditEventsFilter struct {
	Source *domain.Source
	Since  *time.Time
	Limit  int
	Cursor string
}

// AuditEventsResult holds paginated canonical events and degradation metadata.
type AuditEventsResult struct {
	Items            []domain.HazardEvent `json:"items"`
	NextCursor       *string              `json:"next_cursor"`
	Partial          bool                 `json:"partial"`
	UnavailableUnits []string             `json:"unavailable_units"`
}

// AuditChecksumResult holds canonical source checksum computation and degradation metadata.
type AuditChecksumResult struct {
	Source           string    `json:"source"`
	Count            int       `json:"count"`
	Checksum         string    `json:"checksum"`
	Algorithm        string    `json:"algorithm"`
	AsOf             time.Time `json:"as_of"`
	Partial          bool      `json:"partial"`
	UnavailableUnits []string  `json:"unavailable_units"`
}

// AuditLocationResult holds replica/primary placement metadata for a single hazard_id.
type AuditLocationResult struct {
	HazardID  string                   `json:"hazard_id"`
	Locations []domain.StorageLocation `json:"locations"`
	Partial   bool                     `json:"partial"`
}

// AuditUnprocessedFilter defines query parameters for GET /internal/audit/unprocessed.
type AuditUnprocessedFilter struct {
	Source *domain.Source
	Limit  int
	Cursor string
}

// AuditUnprocessedResult holds paginated unprocessed DLQ entries.
type AuditUnprocessedResult struct {
	Items      []domain.UnprocessedMessage `json:"items"`
	NextCursor *string                     `json:"next_cursor"`
}

type auditCursorToken struct {
	Timestamp *time.Time `json:"t,omitempty"`
	ID        string     `json:"id,omitempty"`
	Offset    *int       `json:"o,omitempty"`
}

// ErrInvalidCursor indicates an opaque audit cursor string could not be parsed.
var ErrInvalidCursor = errors.New("invalid cursor")

func encodeKeysetCursor(ts time.Time, id string) string {
	utc := ts.UTC()
	raw, _ := json.Marshal(auditCursorToken{
		Timestamp: &utc,
		ID:        id,
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeAuditCursor(encoded string) (*auditCursorToken, error) {
	if encoded == "" {
		return nil, nil
	}
	var raw []byte
	var err error
	for _, enc := range []*base64.Encoding{base64.RawURLEncoding, base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding} {
		raw, err = enc.DecodeString(encoded)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var tok auditCursorToken
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, ErrInvalidCursor
	}
	if tok.Offset != nil {
		if *tok.Offset < 0 {
			return nil, ErrInvalidCursor
		}
		return &tok, nil
	}
	if tok.Timestamp == nil || tok.Timestamp.IsZero() {
		return nil, ErrInvalidCursor
	}
	return &tok, nil
}

// HazardRepository provides sharded database operations for canonical hazards, outbox, and audit APIs.
type HazardRepository struct {
	mu               sync.RWMutex
	shards           map[string]ShardGroup
	shardIDs         []string
	ring             *ConsistentHashRing
	staleUnits       map[string]bool
	lastPartial      bool
	lastUnavailable  []string
	lastPartialSince *time.Time
}

// NewHazardRepository wraps a single PostgreSQL pool as a one-shard repository for M1 compatibility.
func NewHazardRepository(pool *pgxpool.Pool) *HazardRepository {
	node := strings.TrimSpace(os.Getenv("NODE_NAME"))
	if node == "" {
		node = "local"
	}
	unit := strings.TrimSpace(os.Getenv("STORAGE_UNIT_ID"))
	if unit == "" {
		unit = "shard-1"
	}
	group := ShardGroup{
		ShardID: unit,
		Units: []StorageUnit{
			{
				Unit:    unit,
				Node:    node,
				Role:    domain.StorageRolePrimary,
				Backend: NewPostgresUnitBackend(pool),
			},
		},
	}
	repo, _ := NewShardedHazardRepository([]ShardGroup{group})
	return repo
}

// NewShardedHazardRepository constructs a HazardRepository across one or more configured ShardGroups.
func NewShardedHazardRepository(groups []ShardGroup) (*HazardRepository, error) {
	if len(groups) == 0 {
		return nil, errors.New("at least one shard group is required")
	}
	shards := make(map[string]ShardGroup, len(groups))
	shardIDs := make([]string, 0, len(groups))
	for _, g := range groups {
		if g.ShardID == "" || len(g.Units) == 0 {
			return nil, fmt.Errorf("shard %q must have non-empty id and units", g.ShardID)
		}
		if _, exists := shards[g.ShardID]; exists {
			return nil, fmt.Errorf("duplicate shard id %q", g.ShardID)
		}
		shards[g.ShardID] = g
		shardIDs = append(shardIDs, g.ShardID)
	}
	slices.Sort(shardIDs)

	return &HazardRepository{
		shards:     shards,
		shardIDs:   shardIDs,
		ring:       NewConsistentHashRing(shardIDs, defaultVirtualNodes),
		staleUnits: make(map[string]bool),
	}, nil
}

// DialShardedRepository connects to configured shard units, applies migrations to reachable pools,
// and keeps lazy backends for any temporarily unreachable units so partial reads can continue.
func DialShardedRepository(
	ctx context.Context,
	specs []ShardSpec,
	maxConns, minConns int,
	log *slog.Logger,
) (*HazardRepository, *pgxpool.Pool, error) {
	if len(specs) == 0 {
		return nil, nil, errors.New("no shard specs provided")
	}

	type unitDialResult struct {
		shardIdx int
		unitIdx  int
		backend  *postgresUnitBackend
		pool     *pgxpool.Pool
		err      error
	}

	var results []unitDialResult
	var resMu sync.Mutex
	var wg sync.WaitGroup

	for sIdx, shard := range specs {
		for uIdx, unit := range shard.Units {
			wg.Add(1)
			go func(sI, uI int, spec UnitSpec) {
				defer wg.Done()
				dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				pool, err := NewPostgresPool(dialCtx, spec.DSN, maxConns, minConns)
				if err == nil {
					if mErr := RunMigrations(dialCtx, pool); mErr != nil {
						pool.Close()
						pool = nil
						err = mErr
					}
				}

				var backend *postgresUnitBackend
				if err == nil && pool != nil {
					backend = &postgresUnitBackend{
						pool:     pool,
						dsn:      spec.DSN,
						maxConns: maxConns,
						minConns: minConns,
						log:      log,
					}
				} else {
					if log != nil {
						log.Warn("storage unit unreachable at startup; registering lazy backend",
							slog.String("unit", spec.Unit),
							slog.String("node", spec.Node),
							slog.String("role", string(spec.Role)),
							slog.String("error", err.Error()),
						)
					}
					backend = NewLazyPostgresUnitBackend(spec.DSN, maxConns, minConns, log)
				}

				resMu.Lock()
				results = append(results, unitDialResult{
					shardIdx: sI,
					unitIdx:  uI,
					backend:  backend,
					pool:     pool,
					err:      err,
				})
				resMu.Unlock()
			}(sIdx, uIdx, unit)
		}
	}
	wg.Wait()

	groups := make([]ShardGroup, len(specs))
	for i, s := range specs {
		groups[i] = ShardGroup{
			ShardID: s.ShardID,
			Units:   make([]StorageUnit, len(s.Units)),
		}
	}

	var firstPool *pgxpool.Pool
	healthyCount := 0
	var firstErr error

	for _, r := range results {
		spec := specs[r.shardIdx].Units[r.unitIdx]
		groups[r.shardIdx].Units[r.unitIdx] = StorageUnit{
			Unit:    spec.Unit,
			Node:    spec.Node,
			Role:    spec.Role,
			Backend: r.backend,
		}
		if r.err == nil && r.pool != nil {
			healthyCount++
			if firstPool == nil {
				firstPool = r.pool
			}
		} else if firstErr == nil && r.err != nil {
			firstErr = r.err
		}
	}

	if healthyCount == 0 {
		return nil, nil, fmt.Errorf("all configured storage units unreachable: %w", firstErr)
	}

	repo, err := NewShardedHazardRepository(groups)
	if err != nil {
		return nil, nil, err
	}
	return repo, firstPool, nil
}

// Close closes all active PostgreSQL connection pools held by the repository.
func (r *HazardRepository) Close() {
	groups, _ := r.snapshotTopology()
	for _, g := range groups {
		for _, u := range g.Units {
			if pg, ok := u.Backend.(*postgresUnitBackend); ok {
				pg.Close()
			}
		}
	}
}

func (r *HazardRepository) snapshotTopology() ([]ShardGroup, *ConsistentHashRing) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	groups := make([]ShardGroup, 0, len(r.shardIDs))
	for _, id := range r.shardIDs {
		g := r.shards[id]
		unitsCopy := slices.Clone(g.Units)
		groups = append(groups, ShardGroup{
			ShardID: g.ShardID,
			Units:   unitsCopy,
		})
	}
	return groups, r.ring
}

func (r *HazardRepository) allUnits() []StorageUnit {
	groups, _ := r.snapshotTopology()
	var units []StorageUnit
	for _, g := range groups {
		units = append(units, g.Units...)
	}
	return units
}

func (r *HazardRepository) shardForKey(key string) (ShardGroup, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.ring == nil || len(r.shards) == 0 {
		return ShardGroup{}, false
	}
	shardID := r.ring.Locate(key)
	g, ok := r.shards[shardID]
	if !ok {
		return ShardGroup{}, false
	}
	return ShardGroup{ShardID: g.ShardID, Units: slices.Clone(g.Units)}, true
}

func (r *HazardRepository) LocateShard(hazardID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.ring == nil {
		return ""
	}
	return r.ring.Locate(hazardID)
}

func (r *HazardRepository) markUnitStale(unitID string, stale bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if stale {
		r.staleUnits[unitID] = true
	} else {
		delete(r.staleUnits, unitID)
	}
}

func (r *HazardRepository) isUnitStale(unitID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.staleUnits[unitID]
}

func (r *HazardRepository) recordPartialState(unavailable []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(unavailable) == 0 {
		r.lastPartial = false
		r.lastUnavailable = nil
		r.lastPartialSince = nil
		return
	}
	if !r.lastPartial || r.lastPartialSince == nil {
		now := time.Now().UTC()
		r.lastPartialSince = &now
	}
	r.lastPartial = true
	r.lastUnavailable = slices.Clone(unavailable)
}

// StorageDegradedStatus reports the most recent observed partial-storage state from scatter-gather reads.
func (r *HazardRepository) StorageDegradedStatus() (bool, []string, *time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.lastPartial {
		return false, []string{}, nil
	}
	var sinceCopy *time.Time
	if r.lastPartialSince != nil {
		t := *r.lastPartialSince
		sinceCopy = &t
	}
	return true, slices.Clone(r.lastUnavailable), sinceCopy
}

// Ping verifies that at least one configured storage unit across the cluster is reachable.
func (r *HazardRepository) Ping(ctx context.Context) error {
	units := r.allUnits()
	if len(units) == 0 {
		return ErrAllStorageUnavailable
	}

	probeCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
	defer cancel()

	errs := make(chan error, len(units))
	for _, u := range units {
		go func(unit StorageUnit) {
			errs <- unit.Backend.Ping(probeCtx)
		}(u)
	}

	var firstErr error
	for range units {
		err := <-errs
		if err == nil {
			return nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return fmt.Errorf("%w: %v", ErrAllStorageUnavailable, firstErr)
}

type unitHazardResult struct {
	unit   StorageUnit
	stale  bool
	events []domain.HazardEvent
	err    error
}

type dedupCandidate struct {
	event domain.HazardEvent
	stale bool
	role  domain.StorageRole
}

func shouldReplaceCandidate(existing, incoming dedupCandidate) bool {
	if existing.stale != incoming.stale {
		return !incoming.stale
	}
	if existing.role != incoming.role {
		return incoming.role == domain.StorageRolePrimary
	}
	return incoming.event.OccurredAt.After(existing.event.OccurredAt)
}

func (r *HazardRepository) scatterGatherHazards(ctx context.Context, q UnitHazardQuery) ([]domain.HazardEvent, []string, bool, error) {
	units := r.allUnits()
	if len(units) == 0 {
		return nil, []string{}, false, ErrAllStorageUnavailable
	}

	results := make([]unitHazardResult, len(units))
	var wg sync.WaitGroup
	wg.Add(len(units))

	for i, u := range units {
		go func(idx int, unit StorageUnit) {
			defer wg.Done()
			readCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
			defer cancel()

			events, err := unit.Backend.QueryHazards(readCtx, q)
			results[idx] = unitHazardResult{
				unit:   unit,
				stale:  r.isUnitStale(unit.Unit),
				events: events,
				err:    err,
			}
		}(i, u)
	}
	wg.Wait()

	unavailable := make([]string, 0)
	reachableCount := 0
	byID := make(map[string]dedupCandidate)

	for _, res := range results {
		if res.err != nil {
			unavailable = append(unavailable, res.unit.Unit)
			continue
		}
		reachableCount++
		for _, ev := range res.events {
			cand := dedupCandidate{
				event: cloneHazardEvent(ev),
				stale: res.stale,
				role:  res.unit.Role,
			}
			if prev, exists := byID[ev.HazardID]; !exists || shouldReplaceCandidate(prev, cand) {
				byID[ev.HazardID] = cand
			}
		}
	}

	slices.Sort(unavailable)
	r.recordPartialState(unavailable)

	if reachableCount == 0 {
		return nil, unavailable, true, ErrAllStorageUnavailable
	}

	deduped := make([]domain.HazardEvent, 0, len(byID))
	for _, c := range byID {
		deduped = append(deduped, c.event)
	}

	return deduped, unavailable, len(unavailable) > 0, nil
}

// QueryHazards retrieves paginated hazard events across shards with deterministic tie-breaking.
func (r *HazardRepository) QueryHazards(ctx context.Context, filter HazardFilter) ([]domain.HazardEvent, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	} else if filter.Limit > 500 {
		filter.Limit = 500
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	events, _, _, err := r.scatterGatherHazards(ctx, UnitHazardQuery{
		OccurredSince: filter.Since,
		Type:          filter.Type,
		Severity:      filter.Severity,
	})
	if err != nil {
		return nil, 0, err
	}

	sort.Slice(events, func(i, j int) bool {
		if !events[i].OccurredAt.Equal(events[j].OccurredAt) {
			return events[i].OccurredAt.After(events[j].OccurredAt)
		}
		return events[i].HazardID < events[j].HazardID
	})

	total := len(events)
	if total == 0 || filter.Offset >= total {
		return []domain.HazardEvent{}, total, nil
	}
	end := filter.Offset + filter.Limit
	if end > total {
		end = total
	}
	return events[filter.Offset:end], total, nil
}

// SaveHazard persists or updates a hazard record across the owning shard's primary and replica units.
func (r *HazardRepository) SaveHazard(ctx context.Context, h *domain.HazardEvent) error {
	return r.writeToTargetShard(ctx, h, "", false)
}

// SaveHazardWithOutbox persists or updates a hazard and its transactional outbox entry synchronously across shard units,
// promoting a reachable replica to carry the outbox entry if the primary is down.
func (r *HazardRepository) SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, correlationID string) error {
	return r.writeToTargetShard(ctx, h, correlationID, true)
}

func (r *HazardRepository) writeToTargetShard(ctx context.Context, h *domain.HazardEvent, correlationID string, withOutbox bool) error {
	if h == nil {
		return errors.New("hazard event cannot be nil")
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
			writeCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
			defer cancel()

			copyEvent := cloneHazardEvent(*h)
			var err error
			if withOutbox && unit.Role == domain.StorageRolePrimary {
				err = unit.Backend.SaveHazardWithOutbox(writeCtx, &copyEvent, correlationID)
			} else {
				err = unit.Backend.SaveHazard(writeCtx, &copyEvent)
			}
			outcomes[idx] = writeOutcome{unit: unit, err: err}
		}(i, u)
	}
	wg.Wait()

	var succeeded []StorageUnit
	var failed []StorageUnit
	primaryOutboxWritten := false
	var firstErr error

	for _, out := range outcomes {
		if out.err == nil {
			succeeded = append(succeeded, out.unit)
			if withOutbox && out.unit.Role == domain.StorageRolePrimary {
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
		return fmt.Errorf("all units in shard %q failed write for %q: %w", group.ShardID, h.HazardID, firstErr)
	}

	// Promote the first healthy replica to persist the outbox entry when the primary could not.
	if withOutbox && !primaryOutboxWritten {
		promoted := succeeded[0]
		writeCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
		copyEvent := cloneHazardEvent(*h)
		pErr := promoted.Backend.SaveHazardWithOutbox(writeCtx, &copyEvent, correlationID)
		cancel()
		if pErr != nil {
			return fmt.Errorf("replica promotion outbox write failed on unit %q: %w", promoted.Unit, pErr)
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

func (r *HazardRepository) resolveConsistentIngestedAt(ctx context.Context, group ShardGroup, h *domain.HazardEvent) {
	if !h.IngestedAt.IsZero() {
		h.IngestedAt = h.IngestedAt.UTC()
		return
	}
	for _, u := range group.Units {
		checkCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		ts, found, err := u.Backend.GetExistingIngestedAt(checkCtx, h.HazardID, h.Source, h.SourceRefID)
		cancel()
		if err == nil && found && !ts.IsZero() {
			h.IngestedAt = ts.UTC()
			return
		}
	}
	h.IngestedAt = time.Now().UTC()
}

// GetHazardByID retrieves a single hazard across its owning shard (falling back across all units during rebalance).
func (r *HazardRepository) GetHazardByID(ctx context.Context, hazardID string) (*domain.HazardEvent, error) {
	var orderedUnits []StorageUnit
	seen := make(map[string]struct{})
	if group, ok := r.shardForKey(hazardID); ok {
		for _, u := range group.Units {
			orderedUnits = append(orderedUnits, u)
			seen[u.Unit] = struct{}{}
		}
	}
	for _, u := range r.allUnits() {
		if _, ok := seen[u.Unit]; !ok {
			orderedUnits = append(orderedUnits, u)
		}
	}

	var best *dedupCandidate
	reachable := 0
	var firstErr error
	for _, u := range orderedUnits {
		readCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		h, err := u.Backend.GetHazardByID(readCtx, hazardID)
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		reachable++
		if h == nil {
			continue
		}
		cand := dedupCandidate{
			event: cloneHazardEvent(*h),
			stale: r.isUnitStale(u.Unit),
			role:  u.Role,
		}
		if best == nil || shouldReplaceCandidate(*best, cand) {
			best = &cand
		}
	}

	if best != nil {
		res := best.event
		return &res, nil
	}
	if reachable == 0 {
		return nil, fmt.Errorf("%w: %v", ErrAllStorageUnavailable, firstErr)
	}
	return nil, nil
}

// GetHazardBySourceRef retrieves a hazard record across shards by its agency source and native ID.
func (r *HazardRepository) GetHazardBySourceRef(ctx context.Context, source domain.Source, sourceRefID string) (*domain.HazardEvent, error) {
	units := r.allUnits()
	if len(units) == 0 {
		return nil, ErrAllStorageUnavailable
	}

	type refResult struct {
		unit StorageUnit
		ev   *domain.HazardEvent
		err  error
	}
	resCh := make(chan refResult, len(units))
	for _, u := range units {
		go func(unit StorageUnit) {
			readCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
			defer cancel()
			ev, err := unit.Backend.GetHazardBySourceRef(readCtx, source, sourceRefID)
			resCh <- refResult{unit: unit, ev: ev, err: err}
		}(u)
	}

	var best *dedupCandidate
	reachable := 0
	var firstErr error
	for range units {
		res := <-resCh
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		reachable++
		if res.ev == nil {
			continue
		}
		cand := dedupCandidate{
			event: cloneHazardEvent(*res.ev),
			stale: r.isUnitStale(res.unit.Unit),
			role:  res.unit.Role,
		}
		if best == nil || shouldReplaceCandidate(*best, cand) {
			best = &cand
		}
	}

	if best != nil {
		res := best.event
		return &res, nil
	}
	if reachable == 0 {
		return nil, fmt.Errorf("%w: %v", ErrAllStorageUnavailable, firstErr)
	}
	return nil, nil
}

// GetLatestOccurredAt retrieves the maximum occurred_at timestamp across reachable shards for a source.
func (r *HazardRepository) GetLatestOccurredAt(ctx context.Context, source domain.Source) (*time.Time, error) {
	units := r.allUnits()
	if len(units) == 0 {
		return nil, nil
	}

	var latest *time.Time
	reachable := 0
	var firstErr error
	for _, u := range units {
		readCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
		ts, err := u.Backend.GetLatestOccurredAt(readCtx, source)
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		reachable++
		if ts != nil && (latest == nil || ts.After(*latest)) {
			utc := ts.UTC()
			latest = &utc
		}
	}
	if reachable == 0 && firstErr != nil {
		return nil, firstErr
	}
	return latest, nil
}

// RelayOutboxBatch relays pending outbox events from reachable shard units.
func (r *HazardRepository) RelayOutboxBatch(
	ctx context.Context,
	limit int,
	relayFn func(records []domain.OutboxEvent) (published []domain.OutboxEvent, quarantined []domain.OutboxEvent, err error),
) error {
	units := r.allUnits()
	if len(units) == 0 {
		return ErrAllStorageUnavailable
	}

	reachable := 0
	var firstUnitErr error
	for _, u := range units {
		err := u.Backend.RelayOutboxBatch(ctx, limit, relayFn)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if firstUnitErr == nil {
				firstUnitErr = err
			}
			continue
		}
		reachable++
	}
	if reachable == 0 && firstUnitErr != nil {
		return firstUnitErr
	}
	return nil
}

// FetchUnprocessedOutbox returns pending outbox records from the primary unit of shard 0 (single-pool M1 helper).
func (r *HazardRepository) FetchUnprocessedOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	units := r.allUnits()
	for _, u := range units {
		if u.Role == domain.StorageRolePrimary {
			return u.Backend.FetchUnprocessedOutbox(ctx, limit)
		}
	}
	return nil, ErrAllStorageUnavailable
}

// MarkOutboxPublished marks outbox records published on reachable units.
func (r *HazardRepository) MarkOutboxPublished(ctx context.Context, records []domain.OutboxEvent) error {
	for _, u := range r.allUnits() {
		if u.Role == domain.StorageRolePrimary {
			if err := u.Backend.MarkOutboxPublished(ctx, records); err == nil {
				return nil
			}
		}
	}
	return ErrAllStorageUnavailable
}

// MarkOutboxQuarantined quarantines an outbox record on reachable primary units.
func (r *HazardRepository) MarkOutboxQuarantined(ctx context.Context, id int64, reason string) error {
	for _, u := range r.allUnits() {
		if u.Role == domain.StorageRolePrimary {
			if err := u.Backend.MarkOutboxQuarantined(ctx, id, reason); err == nil {
				return nil
			}
		}
	}
	return ErrAllStorageUnavailable
}

// QueryAuditEvents returns paginated canonical events sorted ascending by (ingested_at, hazard_id) for GET /internal/audit/events.
func (r *HazardRepository) QueryAuditEvents(ctx context.Context, filter AuditEventsFilter) (AuditEventsResult, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	} else if limit > 1000 {
		limit = 1000
	}

	cur, err := decodeAuditCursor(filter.Cursor)
	if err != nil {
		return AuditEventsResult{}, err
	}

	events, unavailable, partial, err := r.scatterGatherHazards(ctx, UnitHazardQuery{
		IngestedSince: filter.Since,
		Source:        filter.Source,
	})
	if err != nil {
		return AuditEventsResult{}, err
	}

	sort.Slice(events, func(i, j int) bool {
		if !events[i].IngestedAt.Equal(events[j].IngestedAt) {
			return events[i].IngestedAt.Before(events[j].IngestedAt)
		}
		return events[i].HazardID < events[j].HazardID
	})

	startIdx := 0
	if cur != nil {
		if cur.Offset != nil {
			startIdx = *cur.Offset
		} else if cur.Timestamp != nil {
			startIdx = sort.Search(len(events), func(i int) bool {
				if events[i].IngestedAt.After(*cur.Timestamp) {
					return true
				}
				return events[i].IngestedAt.Equal(*cur.Timestamp) && events[i].HazardID > cur.ID
			})
		}
	}

	if startIdx >= len(events) {
		return AuditEventsResult{
			Items:            []domain.HazardEvent{},
			NextCursor:       nil,
			Partial:          partial,
			UnavailableUnits: unavailable,
		}, nil
	}

	endIdx := startIdx + limit
	var nextCursor *string
	if endIdx < len(events) {
		last := events[endIdx-1]
		tok := encodeKeysetCursor(last.IngestedAt, last.HazardID)
		nextCursor = &tok
	} else {
		endIdx = len(events)
	}

	return AuditEventsResult{
		Items:            events[startIdx:endIdx],
		NextCursor:       nextCursor,
		Partial:          partial,
		UnavailableUnits: unavailable,
	}, nil
}

// ComputeAuditChecksum calculates SHA-256 over raw-byte sorted source_ref_id values joined by '\n' with no trailing newline.
func (r *HazardRepository) ComputeAuditChecksum(ctx context.Context, source *domain.Source, allSourcesLabel string) (AuditChecksumResult, error) {
	events, unavailable, partial, err := r.scatterGatherHazards(ctx, UnitHazardQuery{
		Source: source,
	})
	if err != nil {
		return AuditChecksumResult{}, err
	}

	refs := make([]string, len(events))
	for i, ev := range events {
		refs[i] = ev.SourceRefID
	}
	slices.Sort(refs)

	joined := strings.Join(refs, "\n")
	sum := sha256.Sum256([]byte(joined))

	scope := allSourcesLabel
	if scope == "" {
		scope = "ALL"
	}
	if source != nil && *source != "" {
		scope = string(*source)
	}

	return AuditChecksumResult{
		Source:           scope,
		Count:            len(refs),
		Checksum:         hex.EncodeToString(sum[:]),
		Algorithm:        "sha256",
		AsOf:             time.Now().UTC(),
		Partial:          partial,
		UnavailableUnits: unavailable,
	}, nil
}

// LocateHazard probes storage units to report exact node, unit, and primary/replica placement for hazard_id.
// Distinguishes proven absence (404) from inaccessible owning shards (503).
func (r *HazardRepository) LocateHazard(ctx context.Context, hazardID string) (AuditLocationResult, error) {
	groups, ring := r.snapshotTopology()
	var ownerShardID string
	if ring != nil {
		ownerShardID = ring.Locate(hazardID)
	}

	type unitProbe struct {
		shardID string
		unit    StorageUnit
		found   bool
		err     error
	}

	var allUnits []unitProbe
	for _, g := range groups {
		for _, u := range g.Units {
			allUnits = append(allUnits, unitProbe{shardID: g.ShardID, unit: u})
		}
	}
	if len(allUnits) == 0 {
		return AuditLocationResult{}, ErrAllStorageUnavailable
	}

	results := make([]unitProbe, len(allUnits))
	var wg sync.WaitGroup
	wg.Add(len(allUnits))
	for i, p := range allUnits {
		go func(idx int, probe unitProbe) {
			defer wg.Done()
			probeCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
			defer cancel()

			ev, err := probe.unit.Backend.GetHazardByID(probeCtx, hazardID)
			probe.found = (err == nil && ev != nil)
			probe.err = err
			results[idx] = probe
		}(i, p)
	}
	wg.Wait()

	locations := make([]domain.StorageLocation, 0)
	partial := false
	ownerTotalUnits := 0
	ownerReachable := 0
	ownerAuthoritativeReachable := 0
	totalReachable := 0

	for _, res := range results {
		if res.shardID == ownerShardID {
			ownerTotalUnits++
		}
		if res.err != nil {
			partial = true
			continue
		}
		totalReachable++
		if res.shardID == ownerShardID {
			ownerReachable++
			if !r.isUnitStale(res.unit.Unit) {
				ownerAuthoritativeReachable++
			}
		}
		if res.found {
			locations = append(locations, domain.StorageLocation{
				Node: res.unit.Node,
				Unit: res.unit.Unit,
				Role: res.unit.Role,
			})
		}
	}

	sort.Slice(locations, func(i, j int) bool {
		if locations[i].Role != locations[j].Role {
			return locations[i].Role == domain.StorageRolePrimary
		}
		return locations[i].Unit < locations[j].Unit
	})

	if len(locations) > 0 {
		return AuditLocationResult{
			HazardID:  hazardID,
			Locations: locations,
			Partial:   partial,
		}, nil
	}

	if totalReachable == 0 || ownerReachable == 0 || (ownerReachable < ownerTotalUnits && ownerAuthoritativeReachable == 0) {
		return AuditLocationResult{}, ErrAllStorageUnavailable
	}

	return AuditLocationResult{}, ErrHazardNotFound
}

// SaveUnprocessedMessage persists or updates a failed DLQ record on the owning shard.
func (r *HazardRepository) SaveUnprocessedMessage(ctx context.Context, msg domain.UnprocessedMessage) error {
	group, ok := r.shardForKey(msg.MessageID)
	if !ok {
		return ErrAllStorageUnavailable
	}
	var firstErr error
	saved := 0
	for _, u := range group.Units {
		writeCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
		err := u.Backend.SaveUnprocessedMessage(writeCtx, msg)
		cancel()
		if err == nil {
			saved++
		} else if firstErr == nil {
			firstErr = err
		}
	}
	if saved == 0 {
		return fmt.Errorf("failed to persist unprocessed message %q: %w", msg.MessageID, firstErr)
	}
	return nil
}

// QueryAuditUnprocessed returns active retrying/parked DLQ messages sorted by (received_at ASC, message_id ASC).
func (r *HazardRepository) QueryAuditUnprocessed(ctx context.Context, filter AuditUnprocessedFilter) (AuditUnprocessedResult, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	} else if limit > 1000 {
		limit = 1000
	}

	cur, err := decodeAuditCursor(filter.Cursor)
	if err != nil {
		return AuditUnprocessedResult{}, err
	}

	units := r.allUnits()
	if len(units) == 0 {
		return AuditUnprocessedResult{}, ErrAllStorageUnavailable
	}

	type unitDLQResult struct {
		msgs []domain.UnprocessedMessage
		err  error
	}
	results := make([]unitDLQResult, len(units))
	var wg sync.WaitGroup
	wg.Add(len(units))
	for i, u := range units {
		go func(idx int, unit StorageUnit) {
			defer wg.Done()
			readCtx, cancel := context.WithTimeout(ctx, unitReadTimeout)
			defer cancel()
			msgs, qErr := unit.Backend.QueryUnprocessed(readCtx, filter.Source)
			results[idx] = unitDLQResult{msgs: msgs, err: qErr}
		}(i, u)
	}
	wg.Wait()

	byID := make(map[string]domain.UnprocessedMessage)
	reachable := 0
	for _, res := range results {
		if res.err != nil {
			continue
		}
		reachable++
		for _, msg := range res.msgs {
			if prev, ok := byID[msg.MessageID]; !ok || msg.Attempts > prev.Attempts {
				byID[msg.MessageID] = msg
			}
		}
	}
	if reachable == 0 {
		return AuditUnprocessedResult{}, ErrAllStorageUnavailable
	}

	items := make([]domain.UnprocessedMessage, 0, len(byID))
	for _, msg := range byID {
		items = append(items, msg)
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].ReceivedAt.Equal(items[j].ReceivedAt) {
			return items[i].ReceivedAt.Before(items[j].ReceivedAt)
		}
		return items[i].MessageID < items[j].MessageID
	})

	startIdx := 0
	if cur != nil {
		if cur.Offset != nil {
			startIdx = *cur.Offset
		} else if cur.Timestamp != nil {
			startIdx = sort.Search(len(items), func(i int) bool {
				if items[i].ReceivedAt.After(*cur.Timestamp) {
					return true
				}
				return items[i].ReceivedAt.Equal(*cur.Timestamp) && items[i].MessageID > cur.ID
			})
		}
	}

	if startIdx >= len(items) {
		return AuditUnprocessedResult{
			Items:      []domain.UnprocessedMessage{},
			NextCursor: nil,
		}, nil
	}

	endIdx := startIdx + limit
	var nextCursor *string
	if endIdx < len(items) {
		last := items[endIdx-1]
		tok := encodeKeysetCursor(last.ReceivedAt, last.MessageID)
		nextCursor = &tok
	} else {
		endIdx = len(items)
	}

	return AuditUnprocessedResult{
		Items:      items[startIdx:endIdx],
		NextCursor: nextCursor,
	}, nil
}

// SyncShardReplicas reconciles missing or stale records across all reachable units within each shard group.
func (r *HazardRepository) SyncShardReplicas(ctx context.Context) error {
	groups, _ := r.snapshotTopology()
	var syncErrs []error

	for _, g := range groups {
		type unitDump struct {
			unit       StorageUnit
			rows       map[string]domain.HazardEvent
			dlq        map[string]domain.UnprocessedMessage
			dlqQueryOK bool
		}
		type dlqCandidate struct {
			msg   domain.UnprocessedMessage
			stale bool
		}

		var dumps []unitDump
		union := make(map[string]dedupCandidate)
		dlqUnion := make(map[string]dlqCandidate)

		for _, u := range g.Units {
			readCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
			events, err := u.Backend.QueryHazards(readCtx, UnitHazardQuery{})
			if err != nil {
				cancel()
				continue
			}
			dlqMsgs, dlqErr := u.Backend.QueryUnprocessed(readCtx, nil)
			cancel()

			stale := r.isUnitStale(u.Unit)
			rowMap := make(map[string]domain.HazardEvent, len(events))
			for _, ev := range events {
				rowMap[ev.HazardID] = ev
				cand := dedupCandidate{
					event: cloneHazardEvent(ev),
					stale: stale,
					role:  u.Role,
				}
				if prev, ok := union[ev.HazardID]; !ok || shouldReplaceCandidate(prev, cand) {
					union[ev.HazardID] = cand
				}
			}

			var dlqMap map[string]domain.UnprocessedMessage
			if dlqErr != nil {
				syncErrs = append(syncErrs, fmt.Errorf("query dlq on unit %s: %w", u.Unit, dlqErr))
			} else {
				dlqMap = make(map[string]domain.UnprocessedMessage, len(dlqMsgs))
				for _, msg := range dlqMsgs {
					dlqMap[msg.MessageID] = msg
					cand := dlqCandidate{msg: msg, stale: stale}
					prev, ok := dlqUnion[msg.MessageID]
					if !ok || (prev.stale != cand.stale && !cand.stale) || (prev.stale == cand.stale && cand.msg.Attempts > prev.msg.Attempts) {
						dlqUnion[msg.MessageID] = cand
					}
				}
			}
			dumps = append(dumps, unitDump{
				unit:       u,
				rows:       rowMap,
				dlq:        dlqMap,
				dlqQueryOK: dlqErr == nil,
			})
		}

		if len(dumps) == 0 {
			syncErrs = append(syncErrs, fmt.Errorf("shard %s has no reachable units to sync", g.ShardID))
			continue
		}

		for _, d := range dumps {
			unitFailed := false
			for hazardID, cand := range union {
				existing, has := d.rows[hazardID]
				if has && !r.isUnitStale(d.unit.Unit) && hazardsEquivalent(existing, cand.event) {
					continue
				}
				writeCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
				cp := cloneHazardEvent(cand.event)
				err := d.unit.Backend.SaveHazard(writeCtx, &cp)
				cancel()
				if err != nil {
					unitFailed = true
					syncErrs = append(syncErrs, fmt.Errorf("sync hazard %s to unit %s: %w", hazardID, d.unit.Unit, err))
					break
				}
			}
			if !unitFailed {
				for msgID, cand := range dlqUnion {
					existing, has := d.dlq[msgID]
					if has && !r.isUnitStale(d.unit.Unit) && dlqMessagesEquivalent(existing, cand.msg) {
						continue
					}
					writeCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
					err := d.unit.Backend.SaveUnprocessedMessage(writeCtx, cand.msg)
					cancel()
					if err != nil {
						unitFailed = true
						syncErrs = append(syncErrs, fmt.Errorf("sync dlq message %s to unit %s: %w", msgID, d.unit.Unit, err))
						break
					}
				}
			}
			if !unitFailed && d.dlqQueryOK {
				r.markUnitStale(d.unit.Unit, false)
			}
		}
	}

	return errors.Join(syncErrs...)
}

func dlqMessagesEquivalent(a, b domain.UnprocessedMessage) bool {
	return a.Source == b.Source &&
		a.Reason == b.Reason &&
		a.Retryable == b.Retryable &&
		a.Attempts == b.Attempts &&
		a.Status == b.Status &&
		a.ReceivedAt.Equal(b.ReceivedAt) &&
		slices.Equal(a.RawPayload, b.RawPayload)
}

func hazardsEquivalent(a, b domain.HazardEvent) bool {
	if a.Severity != b.Severity || a.AreaName != b.AreaName || !a.OccurredAt.Equal(b.OccurredAt) {
		return false
	}
	aJSON, _ := json.Marshal(a)
	bJSON, _ := json.Marshal(b)
	return string(aJSON) == string(bJSON)
}

// RebalanceCluster transitions routing to newGroups without stopping reads or writes,
// copying data to its new owning shard units before removing moved copies from former owners.
func (r *HazardRepository) RebalanceCluster(ctx context.Context, newGroups []ShardGroup) error {
	if len(newGroups) == 0 {
		return errors.New("rebalance requires at least one shard group")
	}

	oldGroups, _ := r.snapshotTopology()
	allEvents, _, _, err := r.scatterGatherHazards(ctx, UnitHazardQuery{})
	if err != nil {
		return fmt.Errorf("gather canonical records before rebalance: %w", err)
	}

	newShardsMap := make(map[string]ShardGroup, len(newGroups))
	newShardIDs := make([]string, 0, len(newGroups))
	for _, g := range newGroups {
		if g.ShardID == "" || len(g.Units) == 0 {
			return fmt.Errorf("invalid target shard group %q", g.ShardID)
		}
		if _, dup := newShardsMap[g.ShardID]; dup {
			return fmt.Errorf("duplicate target shard id %q", g.ShardID)
		}
		newShardsMap[g.ShardID] = g
		newShardIDs = append(newShardIDs, g.ShardID)
	}
	slices.Sort(newShardIDs)
	newRing := NewConsistentHashRing(newShardIDs, defaultVirtualNodes)

	// Copy records to their new target shard units before cutting over routing atomically.
	migrated := make(map[string]map[string]struct{}, len(allEvents))
	for _, ev := range allEvents {
		targetID := newRing.Locate(ev.HazardID)
		group := newShardsMap[targetID]
		unitSet := make(map[string]struct{}, len(group.Units))
		savedCount := 0
		var lastErr error

		for _, u := range group.Units {
			unitSet[u.Unit] = struct{}{}
			writeCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
			cp := cloneHazardEvent(ev)
			wErr := u.Backend.SaveHazard(writeCtx, &cp)
			cancel()
			if wErr == nil {
				savedCount++
			} else {
				lastErr = wErr
			}
		}
		if savedCount == 0 {
			return fmt.Errorf("rebalance failed writing hazard %s to shard %s: %w", ev.HazardID, targetID, lastErr)
		}
		migrated[ev.HazardID] = unitSet
	}

	r.mu.Lock()
	r.shards = newShardsMap
	r.shardIDs = newShardIDs
	r.ring = newRing
	r.mu.Unlock()

	// Clean up moved hazards from units in oldGroups that no longer own them.
	for _, g := range oldGroups {
		for _, u := range g.Units {
			for hazardID, owners := range migrated {
				if _, stillOwned := owners[u.Unit]; stillOwned {
					continue
				}
				delCtx, cancel := context.WithTimeout(ctx, unitWriteTimeout)
				_ = u.Backend.DeleteHazard(delCtx, hazardID)
				cancel()
			}
		}
	}

	return nil
}

// ==============================================================================
// PostgreSQL UnitBackend implementation
// ==============================================================================

type postgresUnitBackend struct {
	mu       sync.Mutex
	pool     *pgxpool.Pool
	dsn      string
	maxConns int
	minConns int
	log      *slog.Logger
}

// NewPostgresUnitBackend wraps an already connected pgxpool.Pool as a UnitBackend.
func NewPostgresUnitBackend(pool *pgxpool.Pool) UnitBackend {
	return &postgresUnitBackend{pool: pool}
}

// NewLazyPostgresUnitBackend registers a PostgreSQL UnitBackend that dials and migrates on demand.
func NewLazyPostgresUnitBackend(dsn string, maxConns, minConns int, log *slog.Logger) *postgresUnitBackend {
	return &postgresUnitBackend{
		dsn:      dsn,
		maxConns: maxConns,
		minConns: minConns,
		log:      log,
	}
}

func (p *postgresUnitBackend) ensurePool(ctx context.Context) (*pgxpool.Pool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pool != nil {
		return p.pool, nil
	}
	if p.dsn == "" {
		return nil, errors.New("postgres unit has no connection pool or DSN")
	}

	pool, err := NewPostgresPool(ctx, p.dsn, p.maxConns, p.minConns)
	if err != nil {
		return nil, err
	}
	if err := RunMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	p.pool = pool
	if p.log != nil {
		p.log.Info("lazy postgres storage unit connected and migrated")
	}
	return p.pool, nil
}

func (p *postgresUnitBackend) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pool != nil {
		p.pool.Close()
		p.pool = nil
	}
}

func (p *postgresUnitBackend) Ping(ctx context.Context) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	return pool.Ping(ctx)
}

func (p *postgresUnitBackend) QueryHazards(ctx context.Context, q UnitHazardQuery) ([]domain.HazardEvent, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}

	whereClauses := []string{"1=1"}
	var args []any
	argIdx := 1

	if q.OccurredSince != nil && !q.OccurredSince.IsZero() {
		whereClauses = append(whereClauses, fmt.Sprintf("occurred_at >= $%d", argIdx))
		args = append(args, *q.OccurredSince)
		argIdx++
	}
	if q.IngestedSince != nil && !q.IngestedSince.IsZero() {
		whereClauses = append(whereClauses, fmt.Sprintf("ingested_at >= $%d", argIdx))
		args = append(args, *q.IngestedSince)
		argIdx++
	}
	if q.Type != nil && *q.Type != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("hazard_type = $%d", argIdx))
		args = append(args, string(*q.Type))
		argIdx++
	}
	if q.Severity != nil && *q.Severity != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("severity = $%d", argIdx))
		args = append(args, string(*q.Severity))
		argIdx++
	}
	if q.Source != nil && *q.Source != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("source = $%d", argIdx))
		args = append(args, string(*q.Source))
		argIdx++
	}

	dataSQL := fmt.Sprintf(`
		SELECT
			hazard_id, source, source_ref_id, hazard_type, severity,
			area_name, latitude, longitude, attributes, occurred_at, ingested_at
		FROM hazards
		WHERE %s
		ORDER BY occurred_at DESC, hazard_id ASC;
	`, strings.Join(whereClauses, " AND "))

	rows, err := pool.Query(ctx, dataSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query hazards: %w", err)
	}
	defer rows.Close()

	var events []domain.HazardEvent
	for rows.Next() {
		h, err := scanHazardRow(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan hazard row: %w", err)
		}
		events = append(events, *h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during hazards iteration: %w", err)
	}
	return events, nil
}

func (p *postgresUnitBackend) SaveHazard(ctx context.Context, h *domain.HazardEvent) error {
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
			occurred_at = EXCLUDED.occurred_at;
	`

	ingestedAt := h.IngestedAt.UTC()
	if ingestedAt.IsZero() {
		ingestedAt = time.Now().UTC()
	}

	_, err = pool.Exec(ctx, sql,
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
		ingestedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to save hazard %q: %w", h.HazardID, err)
	}
	return nil
}

func (p *postgresUnitBackend) SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, correlationID string) error {
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
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var existingIngestedAt time.Time
	err = tx.QueryRow(ctx, "SELECT ingested_at FROM hazards WHERE hazard_id = $1 OR (source = $2 AND source_ref_id = $3) LIMIT 1;", h.HazardID, string(h.Source), h.SourceRefID).Scan(&existingIngestedAt)
	if err == nil && !existingIngestedAt.IsZero() {
		h.IngestedAt = existingIngestedAt.UTC()
	} else if h.IngestedAt.IsZero() {
		h.IngestedAt = time.Now().UTC()
	} else {
		h.IngestedAt = h.IngestedAt.UTC()
	}

	payloadJSON, err := json.Marshal(h)
	if err != nil {
		return fmt.Errorf("failed to marshal hazard payload for outbox: %w", err)
	}

	hazardSQL := `
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
			occurred_at = EXCLUDED.occurred_at;
	`
	_, err = tx.Exec(ctx, hazardSQL,
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
	)
	if err != nil {
		return fmt.Errorf("failed to upsert hazard %q: %w", h.HazardID, err)
	}

	outboxEventID := formatOutboxEventID(h.Source, h.SourceRefID)
	outboxSQL := `
		INSERT INTO outbox_events (event_id, event_type, payload, correlation_id, published)
		VALUES ($1, $2, $3, $4, FALSE)
		ON CONFLICT (event_id) DO UPDATE SET
			payload = EXCLUDED.payload,
			correlation_id = EXCLUDED.correlation_id,
			published = FALSE,
			published_at = NULL,
			claim_expires_at = NULL
		WHERE outbox_events.payload != EXCLUDED.payload;
	`
	_, err = tx.Exec(ctx, outboxSQL, outboxEventID, "hazard_event", payloadJSON, correlationID)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event for %q: %w", h.HazardID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction for hazard %q: %w", h.HazardID, err)
	}
	return nil
}

func (p *postgresUnitBackend) DeleteHazard(ctx context.Context, hazardID string) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, "DELETE FROM hazards WHERE hazard_id = $1;", hazardID)
	return err
}

func (p *postgresUnitBackend) GetHazardByID(ctx context.Context, hazardID string) (*domain.HazardEvent, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}
	sql := `
		SELECT
			hazard_id, source, source_ref_id, hazard_type, severity,
			area_name, latitude, longitude, attributes, occurred_at, ingested_at
		FROM hazards
		WHERE hazard_id = $1;
	`
	h, err := scanHazardRow(pool.QueryRow(ctx, sql, hazardID))
	if err != nil {
		return nil, fmt.Errorf("failed to query hazard by id %q: %w", hazardID, err)
	}
	return h, nil
}

func (p *postgresUnitBackend) GetHazardBySourceRef(ctx context.Context, source domain.Source, sourceRefID string) (*domain.HazardEvent, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}
	sql := `
		SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, attributes, occurred_at, ingested_at
		FROM hazards
		WHERE source = $1 AND source_ref_id = $2;
	`
	return scanHazardRow(pool.QueryRow(ctx, sql, string(source), sourceRefID))
}

func (p *postgresUnitBackend) GetExistingIngestedAt(ctx context.Context, hazardID string, source domain.Source, sourceRefID string) (time.Time, bool, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return time.Time{}, false, err
	}
	var ts time.Time
	err = pool.QueryRow(ctx, "SELECT ingested_at FROM hazards WHERE hazard_id = $1 OR (source = $2 AND source_ref_id = $3) LIMIT 1;", hazardID, string(source), sourceRefID).Scan(&ts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	return ts.UTC(), true, nil
}

func (p *postgresUnitBackend) GetLatestOccurredAt(ctx context.Context, source domain.Source) (*time.Time, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}
	sql := `SELECT MAX(occurred_at) FROM hazards WHERE source = $1;`
	var latest *time.Time
	err = pool.QueryRow(ctx, sql, string(source)).Scan(&latest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query latest occurred_at for %s: %w", source, err)
	}
	if latest != nil {
		utc := latest.UTC()
		latest = &utc
	}
	return latest, nil
}

func scanHazardRow(row pgx.Row) (*domain.HazardEvent, error) {
	var h domain.HazardEvent
	var rawAttrs []byte
	var srcStr, typeStr, sevStr string

	err := row.Scan(
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
		return nil, err
	}

	h.Source = domain.Source(srcStr)
	h.HazardType = domain.HazardType(typeStr)
	h.Severity = domain.Severity(sevStr)
	h.OccurredAt = h.OccurredAt.UTC()
	h.IngestedAt = h.IngestedAt.UTC()

	if len(rawAttrs) > 0 {
		if err := json.Unmarshal(rawAttrs, &h.Attributes); err != nil {
			return nil, fmt.Errorf("failed to decode hazard attributes for %s: %w", h.HazardID, err)
		}
	} else {
		h.Attributes = make(map[string]any)
	}
	return &h, nil
}

// formatOutboxEventID generates a deterministic, collision-resistant outbox event ID bounded to 64 chars.
func formatOutboxEventID(source domain.Source, sourceRefID string) string {
	raw := fmt.Sprintf("outbox-%s-%s", strings.ToLower(string(source)), sourceRefID)
	if len(raw) <= 64 {
		return raw
	}
	h := sha256.Sum256([]byte(raw))
	hashHex := hex.EncodeToString(h[:])[:16]
	prefix := fmt.Sprintf("outbox-%s-", strings.ToLower(string(source)))
	maxRefLen := 64 - len(prefix) - len(hashHex) - 1
	if maxRefLen > len(sourceRefID) {
		maxRefLen = len(sourceRefID)
	}
	return fmt.Sprintf("%s%s-%s", prefix, sourceRefID[:maxRefLen], hashHex)
}

func (p *postgresUnitBackend) RelayOutboxBatch(
	ctx context.Context,
	limit int,
	relayFn func(records []domain.OutboxEvent) (published []domain.OutboxEvent, quarantined []domain.OutboxEvent, err error),
) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	if limit <= 0 {
		limit = 50
	}

	var events []domain.OutboxEvent
	claimTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin outbox claim tx: %w", err)
	}
	defer func() {
		_ = claimTx.Rollback(ctx)
	}()

	claimSQL := `
		WITH claimable AS (
			SELECT id
			FROM outbox_events
			WHERE published = FALSE AND (claim_expires_at IS NULL OR claim_expires_at < NOW())
			ORDER BY id ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events o
		SET claim_expires_at = NOW() + INTERVAL '30 seconds'
		FROM claimable c
		WHERE o.id = c.id
		RETURNING o.id, o.event_id, o.event_type, o.payload, o.correlation_id, o.published, o.created_at, o.published_at;
	`
	rows, err := claimTx.Query(ctx, claimSQL, limit)
	if err != nil {
		return fmt.Errorf("failed to claim unprocessed outbox records: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var o domain.OutboxEvent
		if err := rows.Scan(
			&o.ID,
			&o.EventID,
			&o.EventType,
			&o.Payload,
			&o.CorrelationID,
			&o.Published,
			&o.CreatedAt,
			&o.PublishedAt,
		); err != nil {
			return fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, o)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during outbox claim iteration: %w", err)
	}
	rows.Close()

	if err := claimTx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit outbox claim tx: %w", err)
	}
	if len(events) == 0 {
		return nil
	}

	claimedIDs := make([]int64, len(events))
	for i, e := range events {
		claimedIDs[i] = e.ID
	}

	published, quarantined, err := relayFn(events)
	if err != nil {
		_, _ = pool.Exec(ctx, "UPDATE outbox_events SET claim_expires_at = NULL WHERE id = ANY($1) AND published = FALSE;", claimedIDs)
		return err
	}

	updateTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin outbox update tx: %w", err)
	}
	defer func() {
		_ = updateTx.Rollback(ctx)
	}()

	if len(published) > 0 {
		publishBatch := &pgx.Batch{}
		updateSQL := `
			UPDATE outbox_events
			SET published = TRUE, published_at = NOW(), claim_expires_at = NULL
			WHERE id = $1 AND payload = $2::jsonb;
		`
		for _, rec := range published {
			publishBatch.Queue(updateSQL, rec.ID, rec.Payload)
		}
		br := updateTx.SendBatch(ctx, publishBatch)
		for range published {
			if _, bErr := br.Exec(); bErr != nil {
				_ = br.Close()
				return fmt.Errorf("failed to mark outbox record published: %w", bErr)
			}
		}
		_ = br.Close()
	}

	if len(quarantined) > 0 {
		quarantineBatch := &pgx.Batch{}
		quarantineSQL := `
			UPDATE outbox_events
			SET published = TRUE, event_type = 'hazard_event_quarantined', published_at = NOW(), claim_expires_at = NULL
			WHERE id = $1;
		`
		for _, rec := range quarantined {
			quarantineBatch.Queue(quarantineSQL, rec.ID)
		}
		br := updateTx.SendBatch(ctx, quarantineBatch)
		for range quarantined {
			if _, bErr := br.Exec(); bErr != nil {
				_ = br.Close()
				return fmt.Errorf("failed to quarantine outbox record: %w", bErr)
			}
		}
		_ = br.Close()
	}

	processedSet := make(map[int64]struct{}, len(published)+len(quarantined))
	for _, p := range published {
		processedSet[p.ID] = struct{}{}
	}
	for _, q := range quarantined {
		processedSet[q.ID] = struct{}{}
	}
	var unhandledIDs []int64
	for _, id := range claimedIDs {
		if _, ok := processedSet[id]; !ok {
			unhandledIDs = append(unhandledIDs, id)
		}
	}
	if len(unhandledIDs) > 0 {
		_, _ = updateTx.Exec(ctx, "UPDATE outbox_events SET claim_expires_at = NULL WHERE id = ANY($1);", unhandledIDs)
	}

	if err := updateTx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit outbox update transaction: %w", err)
	}
	return nil
}

func (p *postgresUnitBackend) FetchUnprocessedOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	sql := `
		SELECT id, event_id, event_type, payload, correlation_id, published, created_at, published_at
		FROM outbox_events
		WHERE published = FALSE
		ORDER BY id ASC
		LIMIT $1;
	`
	rows, err := pool.Query(ctx, sql, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query unprocessed outbox: %w", err)
	}
	defer rows.Close()

	var events []domain.OutboxEvent
	for rows.Next() {
		var o domain.OutboxEvent
		if err := rows.Scan(
			&o.ID,
			&o.EventID,
			&o.EventType,
			&o.Payload,
			&o.CorrelationID,
			&o.Published,
			&o.CreatedAt,
			&o.PublishedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during outbox iteration: %w", err)
	}
	return events, nil
}

func (p *postgresUnitBackend) MarkOutboxPublished(ctx context.Context, records []domain.OutboxEvent) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	sql := `
		UPDATE outbox_events
		SET published = TRUE, published_at = NOW()
		WHERE id = $1 AND payload = $2::jsonb;
	`
	for _, rec := range records {
		batch.Queue(sql, rec.ID, rec.Payload)
	}
	br := pool.SendBatch(ctx, batch)
	defer br.Close()
	for range records {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to mark outbox event published: %w", err)
		}
	}
	return nil
}

func (p *postgresUnitBackend) MarkOutboxQuarantined(ctx context.Context, id int64, _ string) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, "UPDATE outbox_events SET published = TRUE, event_type = 'hazard_event_quarantined', published_at = NOW() WHERE id = $1;", id)
	if err != nil {
		return fmt.Errorf("failed to quarantine outbox event %d: %w", id, err)
	}
	return nil
}

func (p *postgresUnitBackend) QueryUnprocessed(ctx context.Context, source *domain.Source) ([]domain.UnprocessedMessage, error) {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return nil, err
	}
	where := "status IN ('retrying', 'parked')"
	var args []any
	if source != nil && *source != "" {
		where += " AND source = $1"
		args = append(args, string(*source))
	}
	sql := fmt.Sprintf(`
		SELECT message_id, source, raw_payload, received_at, reason, retryable, attempts, status
		FROM dlq_messages
		WHERE %s
		ORDER BY received_at ASC, message_id ASC;
	`, where)

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query dlq_messages: %w", err)
	}
	defer rows.Close()

	var msgs []domain.UnprocessedMessage
	for rows.Next() {
		var m domain.UnprocessedMessage
		var srcStr, statusStr string
		if err := rows.Scan(
			&m.MessageID,
			&srcStr,
			&m.RawPayload,
			&m.ReceivedAt,
			&m.Reason,
			&m.Retryable,
			&m.Attempts,
			&statusStr,
		); err != nil {
			return nil, fmt.Errorf("failed to scan dlq row: %w", err)
		}
		m.Source = domain.Source(srcStr)
		m.Status = domain.UnprocessedStatus(statusStr)
		m.ReceivedAt = m.ReceivedAt.UTC()
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating dlq rows: %w", err)
	}
	return msgs, nil
}

func (p *postgresUnitBackend) SaveUnprocessedMessage(ctx context.Context, msg domain.UnprocessedMessage) error {
	pool, err := p.ensurePool(ctx)
	if err != nil {
		return err
	}
	receivedAt := msg.ReceivedAt.UTC()
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	status := msg.Status
	if status == "" {
		status = domain.UnprocessedStatusParked
	}
	sql := `
		INSERT INTO dlq_messages (message_id, source, raw_payload, received_at, reason, retryable, attempts, status, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (message_id) DO UPDATE SET
			source = EXCLUDED.source,
			raw_payload = EXCLUDED.raw_payload,
			received_at = EXCLUDED.received_at,
			reason = EXCLUDED.reason,
			retryable = EXCLUDED.retryable,
			attempts = EXCLUDED.attempts,
			status = EXCLUDED.status,
			updated_at = NOW();
	`
	_, err = pool.Exec(ctx, sql,
		msg.MessageID,
		string(msg.Source),
		msg.RawPayload,
		receivedAt,
		msg.Reason,
		msg.Retryable,
		msg.Attempts,
		string(status),
	)
	return err
}
