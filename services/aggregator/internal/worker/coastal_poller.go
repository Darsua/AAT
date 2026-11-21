package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/logger"
	"bnpb/aggregator/internal/status"
)

const (
	defaultCoastalPollInterval = 2 * time.Second
	defaultCoastalPageLimit    = 100
	defaultCoastalMaxPages     = 5
	defaultCoastalFailureLimit = 3
	defaultCoastalOpenCooldown = 10 * time.Second
	defaultCoastalFreshnessTTL = 30 * time.Second
	maxQuarantineEntries       = 500
	maxFallbackCacheEntries    = 500
)

// BreakerState represents the circuit breaker state for coastal polling.
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half_open"
)

var errCircuitOpen = errors.New("coastal circuit breaker open")

// CircuitBreaker isolates downstream callers from coastal upstream outages and request storms.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            BreakerState
	failures         int
	failureThreshold int
	openCooldown     time.Duration
	openedAt         time.Time
	halfOpenInFlight bool
	nowFn            func() time.Time
}

// NewCircuitBreaker constructs a CircuitBreaker with safe defaults.
func NewCircuitBreaker(failureThreshold int, openCooldown time.Duration) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = defaultCoastalFailureLimit
	}
	if openCooldown <= 0 {
		openCooldown = defaultCoastalOpenCooldown
	}
	return &CircuitBreaker{
		state:            BreakerClosed,
		failureThreshold: failureThreshold,
		openCooldown:     openCooldown,
		nowFn:            func() time.Time { return time.Now().UTC() },
	}
}

// Allow reports whether a poll attempt is permitted under the current breaker state.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case BreakerClosed:
		return true
	case BreakerOpen:
		if cb.nowFn().Sub(cb.openedAt) >= cb.openCooldown {
			cb.state = BreakerHalfOpen
			cb.halfOpenInFlight = true
			return true
		}
		return false
	case BreakerHalfOpen:
		if !cb.halfOpenInFlight {
			cb.halfOpenInFlight = true
			return true
		}
		return false
	default:
		return true
	}
}

// RecordSuccess resets the breaker to closed.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = BreakerClosed
	cb.failures = 0
	cb.halfOpenInFlight = false
}

// RecordFailure increments failure count and opens the breaker if threshold is reached or probe fails.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.halfOpenInFlight = false
	if cb.state == BreakerHalfOpen {
		cb.state = BreakerOpen
		cb.openedAt = cb.nowFn()
		return
	}
	cb.failures++
	if cb.failures >= cb.failureThreshold {
		cb.state = BreakerOpen
		cb.openedAt = cb.nowFn()
	}
}

// State returns the current breaker state.
func (cb *CircuitBreaker) State() BreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// QuarantinedCoastalRecord is a contract-violating coastal observation preserved separately.
type QuarantinedCoastalRecord struct {
	ObservationID string          `json:"observation_id"`
	StationID     string          `json:"station_id"`
	Reason        string          `json:"reason"`
	RawPayload    json.RawMessage `json:"raw_payload"`
	QuarantinedAt time.Time       `json:"quarantined_at"`
}

// CoastalQuarantineStore preserves invalid coastal observations separately from canonical hazards.
type CoastalQuarantineStore struct {
	mu      sync.RWMutex
	entries []QuarantinedCoastalRecord
	cap     int
}

// NewCoastalQuarantineStore creates a bounded quarantine store.
func NewCoastalQuarantineStore(capacity int) *CoastalQuarantineStore {
	if capacity <= 0 {
		capacity = maxQuarantineEntries
	}
	return &CoastalQuarantineStore{cap: capacity}
}

// Add appends a rejected observation, evicting the oldest entry if at capacity.
func (q *CoastalQuarantineStore) Add(rec QuarantinedCoastalRecord) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) >= q.cap {
		q.entries = q.entries[1:]
	}
	q.entries = append(q.entries, rec)
}

// List returns a snapshot of quarantined records (newest last).
func (q *CoastalQuarantineStore) List() []QuarantinedCoastalRecord {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]QuarantinedCoastalRecord, len(q.entries))
	copy(out, q.entries)
	return out
}

// CoastalFallbackSnapshot captures last-known valid coastal hazards and freshness metadata.
type CoastalFallbackSnapshot struct {
	Events        []domain.HazardEvent `json:"events"`
	LastSuccessAt *time.Time           `json:"last_success_at"`
	Stale         bool                 `json:"stale"`
	BreakerState  BreakerState         `json:"breaker_state"`
}

// CoastalFallbackCache retains last-known valid coastal events for fast degraded responses.
type CoastalFallbackCache struct {
	mu            sync.RWMutex
	events        []domain.HazardEvent
	lastSuccessAt *time.Time
	freshnessTTL  time.Duration
}

// NewCoastalFallbackCache creates a fallback cache with the given freshness window.
func NewCoastalFallbackCache(freshnessTTL time.Duration) *CoastalFallbackCache {
	if freshnessTTL <= 0 {
		freshnessTTL = defaultCoastalFreshnessTTL
	}
	return &CoastalFallbackCache{freshnessTTL: freshnessTTL}
}

// Update stores the latest valid coastal events and updates lastSuccessAt.
func (c *CoastalFallbackCache) Update(events []domain.HazardEvent, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts := now.UTC()
	c.lastSuccessAt = &ts
	if len(events) == 0 {
		return
	}
	c.events = append(c.events, events...)
	if len(c.events) > maxFallbackCacheEntries {
		c.events = c.events[len(c.events)-maxFallbackCacheEntries:]
	}
}

// Snapshot returns cached coastal events and whether they are stale relative to now.
func (c *CoastalFallbackCache) Snapshot(breakerState BreakerState, now time.Time) CoastalFallbackSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	copied := make([]domain.HazardEvent, len(c.events))
	copy(copied, c.events)

	stale := true
	if c.lastSuccessAt != nil && now.Sub(*c.lastSuccessAt) <= c.freshnessTTL && breakerState == BreakerClosed {
		stale = false
	}
	var tsCopy *time.Time
	if c.lastSuccessAt != nil {
		t := *c.lastSuccessAt
		tsCopy = &t
	}
	return CoastalFallbackSnapshot{
		Events:        copied,
		LastSuccessAt: tsCopy,
		Stale:         stale,
		BreakerState:  breakerState,
	}
}

// CoastalClient abstracts the CoastalAdapter for unit testing.
type CoastalClient interface {
	FetchStations(ctx context.Context) (map[string]adapter.CoastalStation, error)
	FetchObservations(ctx context.Context, limit int, cursor string, stations map[string]adapter.CoastalStation, now time.Time) (*adapter.CoastalObservationBatch, error)
}

// CoastalPoller polls the untrusted blackbox Coastal Sensor API in isolation from BMKG/PVMBG.
type CoastalPoller struct {
	client     CoastalClient
	repo       HazardSaver
	status     *status.Tracker
	breaker    *CircuitBreaker
	cache      *CoastalFallbackCache
	quarantine *CoastalQuarantineStore
	interval   time.Duration
	reqTimeout time.Duration
	log        *slog.Logger
	lease      leaseGuard

	stations       map[string]adapter.CoastalStation
	stationsAt     time.Time
	cursor         string
	lastGenerated  time.Time
}

// NewCoastalPoller creates an isolated CoastalPoller with its own breaker, cache, and quarantine store.
func NewCoastalPoller(
	client CoastalClient,
	repo HazardSaver,
	stat *status.Tracker,
	interval time.Duration,
	reqTimeout time.Duration,
	freshnessTTL time.Duration,
	log *slog.Logger,
) *CoastalPoller {
	if interval <= 0 {
		interval = defaultCoastalPollInterval
	}
	if reqTimeout <= 0 {
		reqTimeout = 2 * time.Second
	}
	wLog := log.With(slog.String("worker", "coastal-poller"))
	return &CoastalPoller{
		client:     client,
		repo:       repo,
		status:     stat,
		breaker:    NewCircuitBreaker(defaultCoastalFailureLimit, defaultCoastalOpenCooldown),
		cache:      NewCoastalFallbackCache(freshnessTTL),
		quarantine: NewCoastalQuarantineStore(maxQuarantineEntries),
		interval:   interval,
		reqTimeout: reqTimeout,
		log:        wLog,
		lease:      newLeaseGuard(repo, domain.SourceSensorPantai, wLog),
	}
}

// WithLeaseConfig overrides replica holder identity, lease TTL, and renewal cadence.
func (p *CoastalPoller) WithLeaseConfig(holderID string, leaseTTL, renewInterval time.Duration) *CoastalPoller {
	p.lease.configure(holderID, leaseTTL, renewInterval)
	return p
}

// Quarantine returns the coastal quarantine store for inspection.
func (p *CoastalPoller) Quarantine() *CoastalQuarantineStore { return p.quarantine }

// FallbackSnapshot returns the current cached coastal events and freshness metadata.
func (p *CoastalPoller) FallbackSnapshot() CoastalFallbackSnapshot {
	return p.cache.Snapshot(p.breaker.State(), time.Now().UTC())
}

// Start runs the isolated coastal polling loop until ctx is canceled.
func (p *CoastalPoller) Start(ctx context.Context) {
	p.log.Info("starting coastal sensor polling worker",
		slog.Duration("interval", p.interval),
		slog.Duration("request_timeout", p.reqTimeout),
		slog.String("holder_id", p.lease.holderID),
	)

	p.lease.syncCursorFromStorage(ctx)

	for {
		select {
		case <-ctx.Done():
			p.lease.releaseOnShutdown()
			p.log.Info("stopping coastal sensor polling worker")
			return
		case <-time.After(p.interval):
			if err := p.pollOnce(ctx); err != nil && !errors.Is(err, errCircuitOpen) {
				p.log.Warn("coastal poll cycle failed",
					slog.String("breaker_state", string(p.breaker.State())),
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

func (p *CoastalPoller) pollOnce(ctx context.Context) error {
	if p.client == nil {
		return nil
	}

	fence, cycleCtx, stopRenew, proceed, err := p.lease.acquireCycle(ctx)
	if err != nil {
		return err
	}
	if !proceed {
		if p.status != nil {
			p.status.SetCoastalStatus(true)
		}
		return nil
	}
	defer stopRenew()

	if !p.breaker.Allow() {
		if p.status != nil {
			p.status.SetCoastalStatus(false)
		}
		return errCircuitOpen
	}

	corrID := fmt.Sprintf("coastal-poll-%d", time.Now().UnixNano())
	persistCtx := logger.WithCorrelationID(cycleCtx, corrID)

	now := time.Now().UTC()
	if len(p.stations) == 0 || now.Sub(p.stationsAt) > 5*time.Minute {
		stationCtx, stationCancel := context.WithTimeout(persistCtx, p.reqTimeout)
		stations, err := p.client.FetchStations(stationCtx)
		stationCancel()
		if err != nil {
			p.breaker.RecordFailure()
			if p.status != nil {
				p.status.SetCoastalStatus(false)
			}
			return err
		}
		p.stations = stations
		p.stationsAt = now
	}

	seenCursors := make(map[string]struct{})
	persistedEvents := make([]domain.HazardEvent, 0)

	for page := 0; page < defaultCoastalMaxPages; page++ {
		if p.cursor != "" {
			if _, seen := seenCursors[p.cursor]; seen {
				break
			}
			seenCursors[p.cursor] = struct{}{}
		}

		obsCtx, obsCancel := context.WithTimeout(persistCtx, p.reqTimeout)
		batch, err := p.client.FetchObservations(obsCtx, defaultCoastalPageLimit, p.cursor, p.stations, now)
		obsCancel()
		if err != nil {
			p.breaker.RecordFailure()
			if p.status != nil {
				p.status.SetCoastalStatus(false)
			}
			return err
		}

		for _, rej := range batch.Rejected {
			p.quarantine.Add(QuarantinedCoastalRecord{
				ObservationID: rej.ObservationID,
				StationID:     rej.StationID,
				Reason:        rej.Reason,
				RawPayload:    rej.RawPayload,
				QuarantinedAt: now,
			})
			p.recordDLQ(persistCtx, rej.RawPayload, rej.Reason, now)
			p.log.Warn("quarantined invalid coastal observation",
				slog.String("observation_id", rej.ObservationID),
				slog.String("station_id", rej.StationID),
				slog.String("reason", rej.Reason),
			)
		}

		for _, obs := range batch.Valid {
			event, err := normalizeCoastalObservation(obs, p.stations[obs.StationID], now)
			if err != nil {
				raw, _ := json.Marshal(obs)
				p.quarantine.Add(QuarantinedCoastalRecord{
					ObservationID: obs.ObservationID,
					StationID:     obs.StationID,
					Reason:        err.Error(),
					RawPayload:    raw,
					QuarantinedAt: now,
				})
				p.recordDLQ(persistCtx, raw, err.Error(), now)
				continue
			}
			if p.repo != nil {
				if err := p.lease.saveHazard(persistCtx, event, corrID, fence); err != nil {
					if p.status != nil {
						p.status.SetCoastalStatus(false)
					}
					return fmt.Errorf("persist coastal observation %s: %w", obs.ObservationID, err)
				}
				p.lease.advanceCursor(event.OccurredAt, event.SourceRefID)
			}
			persistedEvents = append(persistedEvents, *event)
		}

		p.lastGenerated = batch.GeneratedAt
		if batch.NextCursor == "" || batch.NextCursor == p.cursor {
			break
		}
		p.cursor = batch.NextCursor
	}

	p.breaker.RecordSuccess()
	if p.status != nil {
		p.status.SetCoastalStatus(true)
	}
	p.cache.Update(persistedEvents, now)
	if fence != nil {
		_ = p.lease.saveCheckpoint(persistCtx, fence)
	}
	return nil
}

type coastalDLQSaver interface {
	SaveUnprocessedMessage(ctx context.Context, msg domain.UnprocessedMessage) error
}

func (p *CoastalPoller) recordDLQ(ctx context.Context, raw []byte, reason string, now time.Time) {
	store, ok := p.repo.(coastalDLQSaver)
	if !ok || len(raw) == 0 {
		return
	}
	sum := sha256.Sum256(raw)
	msgID := "pantai-" + hex.EncodeToString(sum[:8])
	msg := domain.UnprocessedMessage{
		MessageID:  msgID,
		Source:     domain.SourceSensorPantai,
		ReceivedAt: now.UTC(),
		Reason:     reason,
		Retryable:  false,
		Attempts:   1,
		Status:     domain.UnprocessedStatusParked,
		RawPayload: raw,
	}
	if err := store.SaveUnprocessedMessage(ctx, msg); err != nil {
		p.log.Warn("could not persist coastal quarantine record to DLQ",
			slog.String("message_id", msgID),
			slog.String("error", err.Error()),
		)
	}
}

func normalizeCoastalObservation(obs adapter.CoastalObservation, station adapter.CoastalStation, now time.Time) (*domain.HazardEvent, error) {
	occurredAt, err := time.Parse(time.RFC3339Nano, obs.ObservedAt)
	if err != nil {
		return nil, fmt.Errorf("parse observed_at: %w", err)
	}

	severity := classifyCoastalSeverity(obs)
	areaName := station.StationID
	if station.Name != "" && station.Province != "" {
		areaName = station.Name + ", " + station.Province
	} else if station.Name != "" {
		areaName = station.Name
	}

	lat := station.Latitude
	lon := station.Longitude

	return &domain.HazardEvent{
		HazardID:    "haz-pantai-" + obs.ObservationID,
		Source:      domain.SourceSensorPantai,
		SourceRefID: obs.ObservationID,
		HazardType:  domain.HazardTypeCoastal,
		Severity:    severity,
		AreaName:    areaName,
		Latitude:    &lat,
		Longitude:   &lon,
		Attributes: map[string]any{
			"station_id":          obs.StationID,
			"sea_level_anomaly_m": obs.SeaLevelAnomalyM,
			"wave_height_m":       obs.WaveHeightM,
			"quality":             obs.Quality,
			"critical":            obs.Critical,
		},
		OccurredAt: occurredAt.UTC(),
		IngestedAt: now.UTC(),
	}, nil
}

func classifyCoastalSeverity(obs adapter.CoastalObservation) domain.Severity {
	if obs.Critical || obs.WaveHeightM >= 4.0 || obs.SeaLevelAnomalyM >= 1.5 {
		return domain.SeverityAwas
	}
	if obs.WaveHeightM >= 2.5 || obs.SeaLevelAnomalyM >= 0.8 {
		return domain.SeveritySiaga
	}
	if obs.WaveHeightM >= 1.5 || obs.SeaLevelAnomalyM >= 0.4 {
		return domain.SeverityWaspada
	}
	return domain.SeverityNormal
}
