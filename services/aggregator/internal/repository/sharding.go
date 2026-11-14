package repository

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"bnpb/aggregator/internal/domain"
)

const (
	defaultVirtualNodes = 64
	unitReadTimeout     = 2500 * time.Millisecond
	unitWriteTimeout    = 3 * time.Second
)

var (
	// ErrAllStorageUnavailable indicates that every storage unit queried failed to respond.
	ErrAllStorageUnavailable = errors.New("all storage units are unavailable")
	// ErrHazardNotFound indicates a record was definitively absent across reachable owning shard units.
	ErrHazardNotFound = errors.New("hazard not found")
)

// UnitHazardQuery filters canonical hazards when querying an individual storage unit.
type UnitHazardQuery struct {
	OccurredSince *time.Time
	IngestedSince *time.Time
	Type          *domain.HazardType
	Severity      *domain.Severity
	Source        *domain.Source
}

// UnitBackend defines the storage operations implemented by PostgreSQL and in-memory shard units.
type UnitBackend interface {
	Ping(ctx context.Context) error
	QueryHazards(ctx context.Context, q UnitHazardQuery) ([]domain.HazardEvent, error)
	SaveHazard(ctx context.Context, h *domain.HazardEvent) error
	SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, correlationID string) error
	DeleteHazard(ctx context.Context, hazardID string) error
	GetHazardByID(ctx context.Context, hazardID string) (*domain.HazardEvent, error)
	GetHazardBySourceRef(ctx context.Context, source domain.Source, sourceRefID string) (*domain.HazardEvent, error)
	GetExistingIngestedAt(ctx context.Context, hazardID string, source domain.Source, sourceRefID string) (time.Time, bool, error)
	GetLatestOccurredAt(ctx context.Context, source domain.Source) (*time.Time, error)
	RelayOutboxBatch(ctx context.Context, limit int, relayFn func(records []domain.OutboxEvent) (published []domain.OutboxEvent, quarantined []domain.OutboxEvent, err error)) error
	FetchUnprocessedOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, records []domain.OutboxEvent) error
	MarkOutboxQuarantined(ctx context.Context, id int64, reason string) error
	QueryUnprocessed(ctx context.Context, source *domain.Source) ([]domain.UnprocessedMessage, error)
	SaveUnprocessedMessage(ctx context.Context, msg domain.UnprocessedMessage) error
}

// UnitSpec describes the configuration of a single primary or replica storage unit.
type UnitSpec struct {
	Unit string             `json:"unit"`
	Node string             `json:"node"`
	Role domain.StorageRole `json:"role"`
	DSN  string             `json:"dsn"`
}

// ShardSpec describes a logical shard and its constituent storage units.
type ShardSpec struct {
	ShardID string     `json:"shard_id"`
	Units   []UnitSpec `json:"units"`
}

// StorageUnit binds a physical/logical storage unit identity to its backend implementation.
type StorageUnit struct {
	Unit    string
	Node    string
	Role    domain.StorageRole
	Backend UnitBackend
}

// ShardGroup groups a shard's primary and replica units together.
type ShardGroup struct {
	ShardID string
	Units   []StorageUnit
}

// ParseShardTopology parses shard definitions from SHARD_TOPOLOGY_JSON, SHARD_CONFIG, or fallback DATABASE_URL.
func ParseShardTopology(topologyJSON, shardConfigStr, fallbackDSN, fallbackNode, fallbackUnit string) ([]ShardSpec, error) {
	if raw := strings.TrimSpace(topologyJSON); raw != "" {
		var specs []ShardSpec
		if err := json.Unmarshal([]byte(raw), &specs); err != nil {
			return nil, fmt.Errorf("invalid SHARD_TOPOLOGY_JSON: %w", err)
		}
		if err := validateShardSpecs(specs); err != nil {
			return nil, err
		}
		return specs, nil
	}

	if raw := strings.TrimSpace(shardConfigStr); raw != "" {
		var specs []ShardSpec
		for _, entry := range strings.Split(raw, ";") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			eqIdx := strings.IndexByte(entry, '=')
			if eqIdx <= 0 || eqIdx == len(entry)-1 {
				return nil, fmt.Errorf("invalid SHARD_CONFIG entry %q: expected shard_id=units", entry)
			}
			shardID := strings.TrimSpace(entry[:eqIdx])
			unitPart := strings.TrimSpace(entry[eqIdx+1:])

			var units []UnitSpec
			for _, uRaw := range strings.Split(unitPart, ",") {
				uRaw = strings.TrimSpace(uRaw)
				if uRaw == "" {
					continue
				}
				fields := strings.Split(uRaw, "|")
				if len(fields) != 4 {
					return nil, fmt.Errorf("invalid SHARD_CONFIG unit %q: expected unit|node|role|dsn", uRaw)
				}
				units = append(units, UnitSpec{
					Unit: strings.TrimSpace(fields[0]),
					Node: strings.TrimSpace(fields[1]),
					Role: domain.StorageRole(strings.TrimSpace(fields[2])),
					DSN:  strings.TrimSpace(fields[3]),
				})
			}
			specs = append(specs, ShardSpec{
				ShardID: shardID,
				Units:   units,
			})
		}
		if err := validateShardSpecs(specs); err != nil {
			return nil, err
		}
		return specs, nil
	}

	if dsn := strings.TrimSpace(fallbackDSN); dsn != "" {
		unit := strings.TrimSpace(fallbackUnit)
		if unit == "" {
			unit = "shard-1"
		}
		node := strings.TrimSpace(fallbackNode)
		if node == "" {
			node = "local"
		}
		return []ShardSpec{
			{
				ShardID: unit,
				Units: []UnitSpec{
					{
						Unit: unit,
						Node: node,
						Role: domain.StorageRolePrimary,
						DSN:  dsn,
					},
				},
			},
		}, nil
	}

	return nil, errors.New("no database or shard configuration provided")
}

func validateShardSpecs(specs []ShardSpec) error {
	if len(specs) == 0 {
		return errors.New("shard topology must define at least one shard")
	}
	seenShards := make(map[string]struct{}, len(specs))
	for _, s := range specs {
		if strings.TrimSpace(s.ShardID) == "" {
			return errors.New("shard_id cannot be empty")
		}
		if _, dup := seenShards[s.ShardID]; dup {
			return fmt.Errorf("duplicate shard_id %q", s.ShardID)
		}
		seenShards[s.ShardID] = struct{}{}
		if len(s.Units) == 0 {
			return fmt.Errorf("shard %q must define at least one storage unit", s.ShardID)
		}
		for _, u := range s.Units {
			if strings.TrimSpace(u.Unit) == "" || strings.TrimSpace(u.Node) == "" || strings.TrimSpace(u.DSN) == "" {
				return fmt.Errorf("shard %q unit %q must specify non-empty unit, node, and dsn", s.ShardID, u.Unit)
			}
			if u.Role != domain.StorageRolePrimary && u.Role != domain.StorageRoleReplica {
				return fmt.Errorf("shard %q unit %q has invalid role %q", s.ShardID, u.Unit, u.Role)
			}
		}
	}
	return nil
}

type ringNode struct {
	hash    uint64
	shardID string
}

// ConsistentHashRing maps canonical keys deterministically across configured shard IDs using virtual nodes.
type ConsistentHashRing struct {
	vnodes int
	ring   []ringNode
}

// NewConsistentHashRing builds a consistent hash ring with 64 virtual nodes per shard.
func NewConsistentHashRing(shardIDs []string, vnodes int) *ConsistentHashRing {
	if vnodes <= 0 {
		vnodes = defaultVirtualNodes
	}
	nodes := make([]ringNode, 0, len(shardIDs)*vnodes)
	for _, shardID := range shardIDs {
		for i := range vnodes {
			h := hashKey(fmt.Sprintf("%s#%d", shardID, i))
			nodes = append(nodes, ringNode{hash: h, shardID: shardID})
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].hash == nodes[j].hash {
			return nodes[i].shardID < nodes[j].shardID
		}
		return nodes[i].hash < nodes[j].hash
	})
	return &ConsistentHashRing{vnodes: vnodes, ring: nodes}
}

// Locate returns the shard ID owning the provided routing key.
func (r *ConsistentHashRing) Locate(key string) string {
	if r == nil || len(r.ring) == 0 {
		return ""
	}
	h := hashKey(key)
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].hash >= h
	})
	if idx == len(r.ring) {
		idx = 0
	}
	return r.ring[idx].shardID
}

func hashKey(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

// PlacementStrategyMetrics captures per-shard counts and skew ratios for a placement policy.
type PlacementStrategyMetrics struct {
	Name          string         `json:"name"`
	CountsByShard map[string]int `json:"counts_by_shard"`
	MinCount      int            `json:"min_count"`
	MaxCount      int            `json:"max_count"`
	SkewRatio     float64        `json:"skew_ratio"`
}

// PlacementComparison compares key distribution skew across placement alternatives.
type PlacementComparison struct {
	TotalEvents              int                      `json:"total_events"`
	ConsistentHashByHazardID PlacementStrategyMetrics `json:"consistent_hash_by_hazard_id"`
	RegionHashByAreaName     PlacementStrategyMetrics `json:"region_hash_by_area_name"`
	AgencyPartitionBySource  PlacementStrategyMetrics `json:"agency_partition_by_source"`
}

// ComparePlacementStrategies evaluates write distribution across 3 placement designs on a dataset.
func ComparePlacementStrategies(events []domain.HazardEvent, shardCount int) PlacementComparison {
	if shardCount <= 0 {
		shardCount = 3
	}
	shardIDs := make([]string, shardCount)
	for i := range shardCount {
		shardIDs[i] = fmt.Sprintf("shard-%d", i+1)
	}
	ring := NewConsistentHashRing(shardIDs, defaultVirtualNodes)

	hashCounts := make(map[string]int, shardCount)
	regionCounts := make(map[string]int, shardCount)
	sourceCounts := make(map[string]int, shardCount)
	for _, id := range shardIDs {
		hashCounts[id] = 0
		regionCounts[id] = 0
		sourceCounts[id] = 0
	}

	for _, ev := range events {
		hashCounts[ring.Locate(ev.HazardID)]++

		regIdx := int(hashKey(strings.ToLower(strings.TrimSpace(ev.AreaName))) % uint64(shardCount))
		regionCounts[shardIDs[regIdx]]++

		srcIdx := int(hashKey(string(ev.Source)) % uint64(shardCount))
		sourceCounts[shardIDs[srcIdx]]++
	}

	return PlacementComparison{
		TotalEvents:              len(events),
		ConsistentHashByHazardID: buildPlacementMetrics("ConsistentHashByHazardID", shardIDs, hashCounts, len(events)),
		RegionHashByAreaName:     buildPlacementMetrics("RegionHashByAreaName", shardIDs, regionCounts, len(events)),
		AgencyPartitionBySource:  buildPlacementMetrics("AgencyPartitionBySource", shardIDs, sourceCounts, len(events)),
	}
}

func buildPlacementMetrics(name string, shardIDs []string, counts map[string]int, total int) PlacementStrategyMetrics {
	if len(shardIDs) == 0 || total == 0 {
		return PlacementStrategyMetrics{Name: name, CountsByShard: counts}
	}
	minC := counts[shardIDs[0]]
	maxC := counts[shardIDs[0]]
	for _, id := range shardIDs[1:] {
		c := counts[id]
		if c < minC {
			minC = c
		}
		if c > maxC {
			maxC = c
		}
	}
	avg := float64(total) / float64(len(shardIDs))
	return PlacementStrategyMetrics{
		Name:          name,
		CountsByShard: counts,
		MinCount:      minC,
		MaxCount:      maxC,
		SkewRatio:     float64(maxC) / avg,
	}
}

// MemoryUnitBackend provides a thread-safe in-memory UnitBackend supporting failure and disk-wipe simulation.
type MemoryUnitBackend struct {
	mu      sync.RWMutex
	healthy bool
	hazards map[string]domain.HazardEvent
	outbox  map[string]domain.OutboxEvent
	dlq     map[string]domain.UnprocessedMessage
	nextSeq int64
}

// NewMemoryUnitBackend creates a healthy, empty in-memory storage unit.
func NewMemoryUnitBackend() *MemoryUnitBackend {
	return &MemoryUnitBackend{
		healthy: true,
		hazards: make(map[string]domain.HazardEvent),
		outbox:  make(map[string]domain.OutboxEvent),
		dlq:     make(map[string]domain.UnprocessedMessage),
	}
}

// SetHealthy toggles simulated node/unit availability.
func (m *MemoryUnitBackend) SetHealthy(healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = healthy
}

// Wipe deletes all persisted hazards, outbox records, and DLQ state on this unit to simulate local disk loss.
func (m *MemoryUnitBackend) Wipe() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hazards = make(map[string]domain.HazardEvent)
	m.outbox = make(map[string]domain.OutboxEvent)
	m.dlq = make(map[string]domain.UnprocessedMessage)
}

// Count returns the number of canonical hazard records currently stored on this unit.
func (m *MemoryUnitBackend) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.hazards)
}

func (m *MemoryUnitBackend) checkHealth(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.healthy {
		return errors.New("storage unit unavailable")
	}
	return nil
}

func (m *MemoryUnitBackend) Ping(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkHealth(ctx)
}

func (m *MemoryUnitBackend) QueryHazards(ctx context.Context, q UnitHazardQuery) ([]domain.HazardEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkHealth(ctx); err != nil {
		return nil, err
	}

	out := make([]domain.HazardEvent, 0, len(m.hazards))
	for _, h := range m.hazards {
		if q.OccurredSince != nil && !q.OccurredSince.IsZero() && h.OccurredAt.Before(*q.OccurredSince) {
			continue
		}
		if q.IngestedSince != nil && !q.IngestedSince.IsZero() && h.IngestedAt.Before(*q.IngestedSince) {
			continue
		}
		if q.Type != nil && *q.Type != "" && h.HazardType != *q.Type {
			continue
		}
		if q.Severity != nil && *q.Severity != "" && h.Severity != *q.Severity {
			continue
		}
		if q.Source != nil && *q.Source != "" && h.Source != *q.Source {
			continue
		}
		out = append(out, cloneHazardEvent(h))
	}
	return out, nil
}

func (m *MemoryUnitBackend) SaveHazard(ctx context.Context, h *domain.HazardEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkHealth(ctx); err != nil {
		return err
	}
	stored := cloneHazardEvent(*h)
	if stored.IngestedAt.IsZero() {
		stored.IngestedAt = time.Now().UTC()
	}
	stored.OccurredAt = stored.OccurredAt.UTC()
	stored.IngestedAt = stored.IngestedAt.UTC()

	m.upsertHazardLocked(stored, true)
	*h = cloneHazardEvent(stored)
	return nil
}

func (m *MemoryUnitBackend) SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, correlationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkHealth(ctx); err != nil {
		return err
	}

	stored := cloneHazardEvent(*h)
	if existing, ok := m.findBySourceRefLocked(stored.HazardID, stored.Source, stored.SourceRefID); ok && !existing.IngestedAt.IsZero() {
		stored.IngestedAt = existing.IngestedAt
	} else if stored.IngestedAt.IsZero() {
		stored.IngestedAt = time.Now().UTC()
	}
	stored.OccurredAt = stored.OccurredAt.UTC()
	stored.IngestedAt = stored.IngestedAt.UTC()

	m.upsertHazardLocked(stored, true)

	payload, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	eventID := formatOutboxEventID(stored.Source, stored.SourceRefID)
	existingOutbox, exists := m.outbox[eventID]
	if !exists || string(existingOutbox.Payload) != string(payload) {
		seq := existingOutbox.ID
		if !exists {
			m.nextSeq++
			seq = m.nextSeq
		}
		m.outbox[eventID] = domain.OutboxEvent{
			ID:            seq,
			EventID:       eventID,
			EventType:     "hazard_event",
			Payload:       payload,
			CorrelationID: correlationID,
			Published:     false,
			CreatedAt:     time.Now().UTC(),
		}
	}

	*h = cloneHazardEvent(stored)
	return nil
}

func (m *MemoryUnitBackend) upsertHazardLocked(h domain.HazardEvent, preserveIngested bool) {
	if existing, ok := m.findBySourceRefLocked(h.HazardID, h.Source, h.SourceRefID); ok {
		if preserveIngested && !existing.IngestedAt.IsZero() {
			h.IngestedAt = existing.IngestedAt
		}
		h.HazardID = existing.HazardID
	}
	m.hazards[h.HazardID] = h
}

func (m *MemoryUnitBackend) findBySourceRefLocked(hazardID string, source domain.Source, sourceRefID string) (domain.HazardEvent, bool) {
	if existing, ok := m.hazards[hazardID]; ok {
		return existing, true
	}
	for _, existing := range m.hazards {
		if existing.Source == source && existing.SourceRefID == sourceRefID {
			return existing, true
		}
	}
	return domain.HazardEvent{}, false
}

func (m *MemoryUnitBackend) GetHazardByID(ctx context.Context, hazardID string) (*domain.HazardEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkHealth(ctx); err != nil {
		return nil, err
	}
	h, ok := m.hazards[hazardID]
	if !ok {
		return nil, nil
	}
	cp := cloneHazardEvent(h)
	return &cp, nil
}

func (m *MemoryUnitBackend) DeleteHazard(ctx context.Context, hazardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkHealth(ctx); err != nil {
		return err
	}
	delete(m.hazards, hazardID)
	return nil
}

func (m *MemoryUnitBackend) GetHazardBySourceRef(ctx context.Context, source domain.Source, sourceRefID string) (*domain.HazardEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkHealth(ctx); err != nil {
		return nil, err
	}
	for _, h := range m.hazards {
		if h.Source == source && h.SourceRefID == sourceRefID {
			cp := cloneHazardEvent(h)
			return &cp, nil
		}
	}
	return nil, nil
}

func (m *MemoryUnitBackend) GetExistingIngestedAt(ctx context.Context, hazardID string, source domain.Source, sourceRefID string) (time.Time, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkHealth(ctx); err != nil {
		return time.Time{}, false, err
	}
	if h, ok := m.findBySourceRefLocked(hazardID, source, sourceRefID); ok && !h.IngestedAt.IsZero() {
		return h.IngestedAt.UTC(), true, nil
	}
	return time.Time{}, false, nil
}

func (m *MemoryUnitBackend) GetLatestOccurredAt(ctx context.Context, source domain.Source) (*time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkHealth(ctx); err != nil {
		return nil, err
	}
	var latest *time.Time
	for _, h := range m.hazards {
		if h.Source != source {
			continue
		}
		if latest == nil || h.OccurredAt.After(*latest) {
			t := h.OccurredAt.UTC()
			latest = &t
		}
	}
	return latest, nil
}

func (m *MemoryUnitBackend) RelayOutboxBatch(
	ctx context.Context,
	limit int,
	relayFn func(records []domain.OutboxEvent) (published []domain.OutboxEvent, quarantined []domain.OutboxEvent, err error),
) error {
	m.mu.RLock()
	if err := m.checkHealth(ctx); err != nil {
		m.mu.RUnlock()
		return err
	}
	if limit <= 0 {
		limit = 50
	}
	var pending []domain.OutboxEvent
	for _, o := range m.outbox {
		if !o.Published {
			pending = append(pending, o)
		}
	}
	m.mu.RUnlock()

	sort.Slice(pending, func(i, j int) bool { return pending[i].ID < pending[j].ID })
	if len(pending) > limit {
		pending = pending[:limit]
	}
	if len(pending) == 0 {
		return nil
	}

	published, quarantined, err := relayFn(pending)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	for _, rec := range published {
		if cur, ok := m.outbox[rec.EventID]; ok && string(cur.Payload) == string(rec.Payload) {
			cur.Published = true
			cur.PublishedAt = &now
			m.outbox[rec.EventID] = cur
		}
	}
	for _, rec := range quarantined {
		if cur, ok := m.outbox[rec.EventID]; ok {
			cur.Published = true
			cur.EventType = "hazard_event_quarantined"
			cur.PublishedAt = &now
			m.outbox[rec.EventID] = cur
		}
	}
	return nil
}

func (m *MemoryUnitBackend) FetchUnprocessedOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkHealth(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	var out []domain.OutboxEvent
	for _, o := range m.outbox {
		if !o.Published {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemoryUnitBackend) MarkOutboxPublished(ctx context.Context, records []domain.OutboxEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkHealth(ctx); err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, rec := range records {
		if cur, ok := m.outbox[rec.EventID]; ok && string(cur.Payload) == string(rec.Payload) {
			cur.Published = true
			cur.PublishedAt = &now
			m.outbox[rec.EventID] = cur
		}
	}
	return nil
}

func (m *MemoryUnitBackend) MarkOutboxQuarantined(ctx context.Context, id int64, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkHealth(ctx); err != nil {
		return err
	}
	now := time.Now().UTC()
	for k, cur := range m.outbox {
		if cur.ID == id {
			cur.Published = true
			cur.EventType = "hazard_event_quarantined"
			cur.PublishedAt = &now
			m.outbox[k] = cur
		}
	}
	return nil
}

func (m *MemoryUnitBackend) QueryUnprocessed(ctx context.Context, source *domain.Source) ([]domain.UnprocessedMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkHealth(ctx); err != nil {
		return nil, err
	}
	out := make([]domain.UnprocessedMessage, 0, len(m.dlq))
	for _, msg := range m.dlq {
		if msg.Status != domain.UnprocessedStatusRetrying && msg.Status != domain.UnprocessedStatusParked {
			continue
		}
		if source != nil && *source != "" && msg.Source != *source {
			continue
		}
		cp := msg
		cp.ReceivedAt = cp.ReceivedAt.UTC()
		if len(msg.RawPayload) > 0 {
			cp.RawPayload = slices.Clone(msg.RawPayload)
		}
		out = append(out, cp)
	}
	return out, nil
}

func (m *MemoryUnitBackend) SaveUnprocessedMessage(ctx context.Context, msg domain.UnprocessedMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkHealth(ctx); err != nil {
		return err
	}
	cp := msg
	if cp.ReceivedAt.IsZero() {
		cp.ReceivedAt = time.Now().UTC()
	} else {
		cp.ReceivedAt = cp.ReceivedAt.UTC()
	}
	if cp.Status == "" {
		cp.Status = domain.UnprocessedStatusParked
	}
	if len(msg.RawPayload) > 0 {
		cp.RawPayload = slices.Clone(msg.RawPayload)
	}
	m.dlq[cp.MessageID] = cp
	return nil
}

func cloneHazardEvent(h domain.HazardEvent) domain.HazardEvent {
	cp := h
	if h.Latitude != nil {
		lat := *h.Latitude
		cp.Latitude = &lat
	}
	if h.Longitude != nil {
		lon := *h.Longitude
		cp.Longitude = &lon
	}
	if h.Attributes != nil {
		cp.Attributes = maps.Clone(h.Attributes)
	} else {
		cp.Attributes = make(map[string]any)
	}
	cp.OccurredAt = cp.OccurredAt.UTC()
	cp.IngestedAt = cp.IngestedAt.UTC()
	return cp
}
