package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/logger"
	"bnpb/aggregator/internal/normalizer"
	"bnpb/aggregator/internal/repository"
	"bnpb/aggregator/internal/status"
)

const overlapWindow = 10 * time.Second

// HazardSaver provides database operations needed by the poller workers.
type HazardSaver interface {
	SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, correlationID string) error
	GetHazardBySourceRef(ctx context.Context, source domain.Source, sourceRefID string) (*domain.HazardEvent, error)
	GetLatestOccurredAt(ctx context.Context, source domain.Source) (*time.Time, error)
}

// FencedCoordinator extends HazardSaver with distributed leader leasing, fenced writes, and durable source checkpoints.
type FencedCoordinator interface {
	AcquireOrRenewSourceLease(ctx context.Context, source domain.Source, holderID string, currentToken int64, ttl time.Duration) (domain.LeaderLease, bool, error)
	ReleaseSourceLease(ctx context.Context, source domain.Source, holderID string, fencingToken int64) error
	SaveHazardWithFence(ctx context.Context, h *domain.HazardEvent, correlationID string, fence domain.WriteFence) error
	GetSourceCheckpoint(ctx context.Context, source domain.Source) (*domain.SourceCheckpoint, error)
	SaveSourceCheckpoint(ctx context.Context, cp domain.SourceCheckpoint) error
}

type leaseGuard struct {
	repo            HazardSaver
	source          domain.Source
	holderID        string
	leaseTTL        time.Duration
	renewInterval   time.Duration
	log             *slog.Logger
	fencingToken    int64
	cursor          *time.Time
	lastSourceRefID string
}

func newLeaseGuard(repo HazardSaver, source domain.Source, log *slog.Logger) leaseGuard {
	holder := os.Getenv("INSTANCE_ID")
	if holder == "" {
		holder = os.Getenv("HOSTNAME")
	}
	if holder == "" {
		holder = fmt.Sprintf("aggregator-%d", os.Getpid())
	}
	return leaseGuard{
		repo:          repo,
		source:        source,
		holderID:      holder,
		leaseTTL:      15 * time.Second,
		renewInterval: 5 * time.Second,
		log:           log,
	}
}

func (g *leaseGuard) configure(holderID string, leaseTTL, renewInterval time.Duration) {
	if holderID != "" {
		g.holderID = holderID
	}
	if leaseTTL > 0 {
		g.leaseTTL = leaseTTL
	}
	if renewInterval > 0 {
		g.renewInterval = renewInterval
	}
}

func (g *leaseGuard) syncCursorFromStorage(ctx context.Context) {
	if fc, ok := g.repo.(FencedCoordinator); ok {
		cp, err := fc.GetSourceCheckpoint(ctx, g.source)
		if err != nil {
			g.log.Warn("could not load durable source checkpoint", "error", err)
		} else if cp != nil && !cp.LastOccurredAt.IsZero() {
			utc := cp.LastOccurredAt.UTC()
			if g.cursor == nil || utc.After(*g.cursor) {
				g.cursor = &utc
				g.lastSourceRefID = cp.LastSourceRefID
			}
		}
	}

	latest, err := g.repo.GetLatestOccurredAt(ctx, g.source)
	if err != nil {
		g.log.Warn("could not bootstrap cursor from stored hazards", "error", err)
		return
	}
	if latest != nil {
		utc := latest.UTC()
		if g.cursor == nil || utc.After(*g.cursor) {
			g.cursor = &utc
		}
	}
}

func (g *leaseGuard) acquireCycle(ctx context.Context) (*domain.WriteFence, context.Context, func(), bool, error) {
	fc, ok := g.repo.(FencedCoordinator)
	if !ok {
		return nil, ctx, func() {}, true, nil
	}

	lease, acquired, err := fc.AcquireOrRenewSourceLease(ctx, g.source, g.holderID, g.fencingToken, g.leaseTTL)
	if err != nil {
		if errors.Is(err, repository.ErrStaleFencingToken) {
			g.log.Warn("stale_fence_rejected", "holder_id", g.holderID, "token", g.fencingToken)
			g.fencingToken = 0
		}
		return nil, ctx, func() {}, false, err
	}

	if !acquired {
		if g.fencingToken > 0 {
			g.log.Info("leadership lost for source",
				slog.String("source", string(g.source)),
				slog.String("holder_id", g.holderID),
				slog.Int64("previous_token", g.fencingToken),
				slog.String("current_leader", lease.HolderID),
			)
			g.fencingToken = 0
		}
		return nil, ctx, func() {}, false, nil
	}

	if g.fencingToken != lease.FencingToken {
		g.log.Info("leadership_acquired",
			slog.String("source", string(g.source)),
			slog.String("holder_id", g.holderID),
			slog.Int64("fencing_token", lease.FencingToken),
		)
		g.fencingToken = lease.FencingToken
		g.syncCursorFromStorage(ctx)
	}

	fence := &domain.WriteFence{
		LeaseKey:     repository.LeaseKeyForSource(g.source),
		Source:       g.source,
		HolderID:     g.holderID,
		FencingToken: g.fencingToken,
	}

	cycleCtx, cancelCycle := context.WithCancel(ctx)
	renewCtx, cancelRenew := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		interval := g.renewInterval
		if interval <= 0 {
			interval = 5 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				renewed, stillHeld, rErr := fc.AcquireOrRenewSourceLease(renewCtx, g.source, g.holderID, fence.FencingToken, g.leaseTTL)
				if rErr != nil || !stillHeld || renewed.FencingToken != fence.FencingToken {
					if rErr == nil && stillHeld && renewed.FencingToken != fence.FencingToken {
						_ = fc.ReleaseSourceLease(renewCtx, g.source, g.holderID, renewed.FencingToken)
					}
					g.log.Warn("lease renewal failed or superseded during poll cycle",
						slog.String("holder_id", g.holderID),
						slog.Int64("token", fence.FencingToken),
						slog.Bool("still_held", stillHeld),
					)
					cancelCycle()
					return
				}
			}
		}
	}()

	stopRenew := func() {
		cancelRenew()
		wg.Wait()
		cancelCycle()
	}

	return fence, cycleCtx, stopRenew, true, nil
}

func (g *leaseGuard) saveHazard(ctx context.Context, h *domain.HazardEvent, correlationID string, fence *domain.WriteFence) error {
	if fence != nil {
		if fc, ok := g.repo.(FencedCoordinator); ok {
			err := fc.SaveHazardWithFence(ctx, h, correlationID, *fence)
			if errors.Is(err, repository.ErrStaleFencingToken) {
				g.log.Warn("stale_fence_rejected", "hazard_id", h.HazardID, "token", fence.FencingToken)
				g.fencingToken = 0
			}
			return err
		}
	}
	return g.repo.SaveHazardWithOutbox(ctx, h, correlationID)
}

func (g *leaseGuard) advanceCursor(occurredAt time.Time, sourceRefID string) {
	utc := occurredAt.UTC()
	if g.cursor == nil || utc.After(*g.cursor) {
		g.cursor = &utc
		if sourceRefID != "" {
			g.lastSourceRefID = sourceRefID
		}
	}
}

func (g *leaseGuard) saveCheckpoint(ctx context.Context, fence *domain.WriteFence) error {
	if fence == nil || g.cursor == nil {
		return nil
	}
	fc, ok := g.repo.(FencedCoordinator)
	if !ok {
		return nil
	}
	err := fc.SaveSourceCheckpoint(ctx, domain.SourceCheckpoint{
		Source:          g.source,
		LastOccurredAt:  g.cursor.UTC(),
		LastSourceRefID: g.lastSourceRefID,
		FencingToken:    fence.FencingToken,
		HolderID:        g.holderID,
	})
	if errors.Is(err, repository.ErrStaleFencingToken) {
		g.log.Warn("stale_fence_rejected", "op", "save_checkpoint", "token", fence.FencingToken)
		g.fencingToken = 0
	}
	return err
}

func (g *leaseGuard) releaseOnShutdown() {
	if g.fencingToken <= 0 {
		return
	}
	fc, ok := g.repo.(FencedCoordinator)
	if !ok {
		return
	}
	relCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := fc.ReleaseSourceLease(relCtx, g.source, g.holderID, g.fencingToken); err != nil {
		g.log.Warn("failed releasing lease cleanly on shutdown", "error", err)
	}
	g.fencingToken = 0
}

// BMKGPoller periodically polls the BMKG upstream API for seismic events and tsunami warnings.
type BMKGPoller struct {
	adapter    *adapter.BMKGAdapter
	normalizer *normalizer.Normalizer
	repo       HazardSaver
	status     *status.Tracker
	interval   time.Duration
	log        *slog.Logger
	lease      leaseGuard
}

// NewBMKGPoller creates a new BMKGPoller.
func NewBMKGPoller(
	adapter *adapter.BMKGAdapter,
	norm *normalizer.Normalizer,
	repo HazardSaver,
	stat *status.Tracker,
	interval time.Duration,
	log *slog.Logger,
) *BMKGPoller {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	wLog := log.With(slog.String("worker", "bmkg-poller"))
	return &BMKGPoller{
		adapter:    adapter,
		normalizer: norm,
		repo:       repo,
		status:     stat,
		interval:   interval,
		log:        wLog,
		lease:      newLeaseGuard(repo, domain.SourceBMKG, wLog),
	}
}

// WithLeaseConfig overrides replica holder identity, lease TTL, and renewal cadence.
func (p *BMKGPoller) WithLeaseConfig(holderID string, leaseTTL, renewInterval time.Duration) *BMKGPoller {
	p.lease.configure(holderID, leaseTTL, renewInterval)
	return p
}

// Start launches the BMKG polling loop until the context is canceled.
func (p *BMKGPoller) Start(ctx context.Context) {
	p.log.Info("starting BMKG polling worker",
		slog.Duration("interval", p.interval),
		slog.String("holder_id", p.lease.holderID),
	)

	p.lease.syncCursorFromStorage(ctx)
	if p.lease.cursor != nil {
		p.log.Info("bootstrapped BMKG cursor", slog.Time("cursor", *p.lease.cursor))
	}

	retryCount := 0
	maxBackoff := 30 * time.Second

	for {
		maxJitter := 500 * time.Millisecond
		if p.interval/4 < maxJitter {
			maxJitter = p.interval / 4
		}
		sleepDuration := p.interval + randomJitter(0, maxJitter)
		if retryCount > 0 {
			sleepDuration = computeBackoff(p.interval, retryCount, maxBackoff)
		}

		select {
		case <-ctx.Done():
			p.lease.releaseOnShutdown()
			p.log.Info("stopping BMKG polling worker")
			return
		case <-time.After(sleepDuration):
			if err := p.poll(ctx); err != nil {
				retryCount++
				p.log.Warn("BMKG poll cycle failed, backing off",
					"retry_count", retryCount,
					"next_delay", sleepDuration,
					"error", err,
				)
			} else {
				retryCount = 0
			}
		}
	}
}

func (p *BMKGPoller) poll(ctx context.Context) error {
	if p.adapter == nil {
		return nil
	}

	fence, cycleCtx, stopRenew, proceed, err := p.lease.acquireCycle(ctx)
	if err != nil {
		return err
	}
	if !proceed {
		return nil
	}
	defer stopRenew()

	var since *time.Time
	if p.lease.cursor != nil {
		t := p.lease.cursor.Add(-overlapWindow)
		since = &t
	}

	pollCtx, cancel := context.WithTimeout(cycleCtx, 10*time.Second)
	defer cancel()
	cycleCorrID := fmt.Sprintf("bmkg-poll-%d", time.Now().UnixNano())
	pollCtx = logger.WithCorrelationID(pollCtx, cycleCorrID)

	// 1. Fetch seismic events
	events, seismicErr := p.adapter.FetchSeismicEvents(pollCtx, since)

	// 2. Fetch tsunami warnings independently (never skipped if seismic is empty)
	warnings, tsunamiErr := p.adapter.FetchTsunamiWarnings(pollCtx, since)

	// 3. Mark healthy only if BOTH endpoints succeed
	if seismicErr != nil || tsunamiErr != nil {
		p.status.SetBMKGStatus(false)
		if seismicErr != nil {
			p.log.Warn("BMKG seismic poll failed", "error", seismicErr)
			return seismicErr
		}
		p.log.Warn("BMKG tsunami warning poll failed", "error", tsunamiErr)
		return tsunamiErr
	}

	p.status.SetBMKGStatus(true)

	// Process new seismic events with correlated warnings
	for _, event := range events {
		canonical := p.normalizer.NormalizeSeismic(event, warnings)
		corrID := fmt.Sprintf("bmkg-%s", event.EventID)
		if err := p.lease.saveHazard(cycleCtx, &canonical, corrID, fence); err != nil {
			p.log.Error("failed to persist normalized seismic hazard", "hazard_id", canonical.HazardID, "error", err)
			return fmt.Errorf("failed to persist seismic hazard %s: %w", canonical.HazardID, err)
		}
		p.lease.advanceCursor(canonical.OccurredAt, canonical.SourceRefID)
	}

	// Update existing stored hazards if any tsunami warnings arrived
	for _, w := range warnings {
		if w.RelatedEventID == "" {
			continue
		}
		existing, err := p.repo.GetHazardBySourceRef(cycleCtx, domain.SourceBMKG, w.RelatedEventID)
		if err != nil {
			p.log.Debug("could not find existing seismic hazard for tsunami warning", "related_event_id", w.RelatedEventID, "error", err)
			continue
		}
		if existing != nil {
			updated := p.normalizer.ApplyTsunamiWarning(existing, w)
			corrID := fmt.Sprintf("bmkg-tsu-%s", w.WarningID)
			if err := p.lease.saveHazard(cycleCtx, updated, corrID, fence); err != nil {
				p.log.Error("failed to update hazard with tsunami warning", "hazard_id", updated.HazardID, "error", err)
				return fmt.Errorf("failed to update hazard with tsunami warning %s: %w", updated.HazardID, err)
			}
		}
	}

	if err := p.lease.saveCheckpoint(cycleCtx, fence); err != nil {
		return err
	}

	p.log.Debug("BMKG poll cycle completed", slog.Int("ingested_events", len(events)), slog.Int("warnings", len(warnings)))
	return nil
}

// PVMBGPoller periodically polls the PVMBG upstream API for volcanic monitoring reports.
type PVMBGPoller struct {
	adapter    *adapter.PVMBGAdapter
	normalizer *normalizer.Normalizer
	repo       HazardSaver
	status     *status.Tracker
	interval   time.Duration
	log        *slog.Logger
	lease      leaseGuard
}

// NewPVMBGPoller creates a new PVMBGPoller.
func NewPVMBGPoller(
	adapter *adapter.PVMBGAdapter,
	norm *normalizer.Normalizer,
	repo HazardSaver,
	stat *status.Tracker,
	interval time.Duration,
	log *slog.Logger,
) *PVMBGPoller {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	wLog := log.With(slog.String("worker", "pvmbg-poller"))
	return &PVMBGPoller{
		adapter:    adapter,
		normalizer: norm,
		repo:       repo,
		status:     stat,
		interval:   interval,
		log:        wLog,
		lease:      newLeaseGuard(repo, domain.SourcePVMBG, wLog),
	}
}

// WithLeaseConfig overrides replica holder identity, lease TTL, and renewal cadence.
func (p *PVMBGPoller) WithLeaseConfig(holderID string, leaseTTL, renewInterval time.Duration) *PVMBGPoller {
	p.lease.configure(holderID, leaseTTL, renewInterval)
	return p
}

// Start launches the PVMBG polling loop until context cancellation.
func (p *PVMBGPoller) Start(ctx context.Context) {
	p.log.Info("starting PVMBG polling worker",
		slog.Duration("interval", p.interval),
		slog.String("holder_id", p.lease.holderID),
	)

	p.lease.syncCursorFromStorage(ctx)
	if p.lease.cursor != nil {
		p.log.Info("bootstrapped PVMBG cursor", slog.Time("cursor", *p.lease.cursor))
	}

	retryCount := 0
	maxBackoff := 30 * time.Second

	for {
		maxJitter := 500 * time.Millisecond
		if p.interval/4 < maxJitter {
			maxJitter = p.interval / 4
		}
		sleepDuration := p.interval + randomJitter(0, maxJitter)
		if retryCount > 0 {
			sleepDuration = computeBackoff(p.interval, retryCount, maxBackoff)
		}

		select {
		case <-ctx.Done():
			p.lease.releaseOnShutdown()
			p.log.Info("stopping PVMBG polling worker")
			return
		case <-time.After(sleepDuration):
			if err := p.poll(ctx); err != nil {
				retryCount++
				p.log.Warn("PVMBG poll cycle failed, backing off",
					"retry_count", retryCount,
					"next_delay", sleepDuration,
					"error", err,
				)
			} else {
				retryCount = 0
			}
		}
	}
}

func (p *PVMBGPoller) poll(ctx context.Context) error {
	if p.adapter == nil {
		return nil
	}

	fence, cycleCtx, stopRenew, proceed, err := p.lease.acquireCycle(ctx)
	if err != nil {
		return err
	}
	if !proceed {
		return nil
	}
	defer stopRenew()

	var since *time.Time
	if p.lease.cursor != nil {
		t := p.lease.cursor.Add(-overlapWindow)
		since = &t
	}

	pollCtx, cancel := context.WithTimeout(cycleCtx, 12*time.Second)
	defer cancel()
	cycleCorrID := fmt.Sprintf("pvmbg-poll-%d", time.Now().UnixNano())
	pollCtx = logger.WithCorrelationID(pollCtx, cycleCorrID)

	reports, err := p.adapter.FetchVolcanicReports(pollCtx, since)
	if err != nil {
		p.status.SetPVMBGStatus(false)
		if errors.Is(err, adapter.ErrUpstreamUnavailable) {
			p.log.Warn("PVMBG upstream reports outage simulation (HTTP 503)")
		} else {
			p.log.Warn("PVMBG volcanic poll failed", "error", err)
		}
		return err
	}

	p.status.SetPVMBGStatus(true)

	if len(reports) == 0 {
		return p.lease.saveCheckpoint(cycleCtx, fence)
	}

	for _, report := range reports {
		canonical, err := p.normalizer.NormalizeVolcanic(cycleCtx, report)
		if err != nil {
			p.log.Error("failed to normalize volcanic report", "report_id", report.ReportID, "error", err)
			continue
		}

		corrID := fmt.Sprintf("pvmbg-%s", report.ReportID)
		if err := p.lease.saveHazard(cycleCtx, &canonical, corrID, fence); err != nil {
			p.log.Error("failed to persist normalized volcanic hazard", "hazard_id", canonical.HazardID, "error", err)
			return fmt.Errorf("failed to persist volcanic hazard %s: %w", canonical.HazardID, err)
		}
		p.lease.advanceCursor(canonical.OccurredAt, canonical.SourceRefID)
	}

	if err := p.lease.saveCheckpoint(cycleCtx, fence); err != nil {
		return err
	}

	p.log.Debug("PVMBG poll cycle completed", slog.Int("ingested_reports", len(reports)))
	return nil
}

func randomJitter(base time.Duration, maxJitter time.Duration) time.Duration {
	if maxJitter <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(int64(maxJitter)))
}

// computeBackoff calculates exponential backoff with bounded exponent shift to prevent overflow during prolonged outages.
func computeBackoff(baseInterval time.Duration, retryCount int, maxBackoff time.Duration) time.Duration {
	shift := retryCount
	if shift > 6 {
		shift = 6
	}
	backoff := baseInterval * (1 << shift)
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	return backoff + randomJitter(0, 1*time.Second)
}
