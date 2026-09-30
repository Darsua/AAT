package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/normalizer"
	"bnpb/aggregator/internal/status"
)

const overlapWindow = 10 * time.Second

// HazardSaver provides database operations needed by the poller workers.
type HazardSaver interface {
	SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent) error
	GetLatestOccurredAt(ctx context.Context, source domain.Source) (*time.Time, error)
}

// BMKGPoller periodically polls the BMKG upstream API for seismic events and tsunami warnings.
type BMKGPoller struct {
	adapter    *adapter.BMKGAdapter
	normalizer *normalizer.Normalizer
	repo       HazardSaver
	status     *status.Tracker
	interval   time.Duration
	log        *slog.Logger
	cursor     *time.Time
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
	return &BMKGPoller{
		adapter:    adapter,
		normalizer: norm,
		repo:       repo,
		status:     stat,
		interval:   interval,
		log:        log.With(slog.String("worker", "bmkg-poller")),
	}
}

// Start launches the BMKG polling loop until the context is canceled.
func (p *BMKGPoller) Start(ctx context.Context) {
	p.log.Info("starting BMKG polling worker", slog.Duration("interval", p.interval))

	// Bootstrap cursor from database
	latest, err := p.repo.GetLatestOccurredAt(ctx, domain.SourceBMKG)
	if err != nil {
		p.log.Warn("could not bootstrap BMKG cursor from database", "error", err)
	} else if latest != nil {
		p.cursor = latest
		p.log.Info("bootstrapped BMKG cursor", slog.Time("cursor", *p.cursor))
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Initial poll if not canceled
	select {
	case <-ctx.Done():
		p.log.Info("stopping BMKG polling worker")
		return
	default:
		p.poll(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			p.log.Info("stopping BMKG polling worker")
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *BMKGPoller) poll(ctx context.Context) {
	if p.adapter == nil {
		return
	}
	var since *time.Time
	if p.cursor != nil {
		t := p.cursor.Add(-overlapWindow)
		since = &t
	}

	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	events, err := p.adapter.FetchSeismicEvents(pollCtx, since)
	if err != nil {
		p.status.SetBMKGStatus(false)
		p.log.Warn("BMKG seismic poll failed", "error", err)
		return
	}

	p.status.SetBMKGStatus(true)

	if len(events) == 0 {
		return
	}

	// Fetch correlated tsunami warnings
	warnings, err := p.adapter.FetchTsunamiWarnings(pollCtx, since)
	if err != nil {
		p.log.Warn("BMKG tsunami warning poll failed, continuing with seismic data alone", "error", err)
		warnings = nil
	}

	for _, event := range events {
		canonical := p.normalizer.NormalizeSeismic(event, warnings)
		if err := p.repo.SaveHazardWithOutbox(ctx, &canonical); err != nil {
			p.log.Error("failed to persist normalized seismic hazard", "hazard_id", canonical.HazardID, "error", err)
			continue
		}

		if p.cursor == nil || canonical.OccurredAt.After(*p.cursor) {
			t := canonical.OccurredAt
			p.cursor = &t
		}
	}

	p.log.Debug("BMKG poll cycle completed", slog.Int("ingested_events", len(events)))
}

// PVMBGPoller periodically polls the PVMBG upstream API for volcanic monitoring reports.
type PVMBGPoller struct {
	adapter    *adapter.PVMBGAdapter
	normalizer *normalizer.Normalizer
	repo       HazardSaver
	status     *status.Tracker
	interval   time.Duration
	log        *slog.Logger
	cursor     *time.Time
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
	return &PVMBGPoller{
		adapter:    adapter,
		normalizer: norm,
		repo:       repo,
		status:     stat,
		interval:   interval,
		log:        log.With(slog.String("worker", "pvmbg-poller")),
	}
}

// Start launches the PVMBG polling loop until context cancellation.
func (p *PVMBGPoller) Start(ctx context.Context) {
	p.log.Info("starting PVMBG polling worker", slog.Duration("interval", p.interval))

	// Bootstrap cursor from database
	latest, err := p.repo.GetLatestOccurredAt(ctx, domain.SourcePVMBG)
	if err != nil {
		p.log.Warn("could not bootstrap PVMBG cursor from database", "error", err)
	} else if latest != nil {
		p.cursor = latest
		p.log.Info("bootstrapped PVMBG cursor", slog.Time("cursor", *p.cursor))
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Initial poll if not canceled
	select {
	case <-ctx.Done():
		p.log.Info("stopping PVMBG polling worker")
		return
	default:
		p.poll(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			p.log.Info("stopping PVMBG polling worker")
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *PVMBGPoller) poll(ctx context.Context) {
	if p.adapter == nil {
		return
	}
	var since *time.Time
	if p.cursor != nil {
		t := p.cursor.Add(-overlapWindow)
		since = &t
	}

	pollCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	reports, err := p.adapter.FetchVolcanicReports(pollCtx, since)
	if err != nil {
		p.status.SetPVMBGStatus(false)
		if errors.Is(err, adapter.ErrUpstreamUnavailable) {
			p.log.Warn("PVMBG upstream reports outage simulation (HTTP 503)")
		} else {
			p.log.Warn("PVMBG volcanic poll failed", "error", err)
		}
		return
	}

	p.status.SetPVMBGStatus(true)

	if len(reports) == 0 {
		return
	}

	for _, report := range reports {
		canonical, err := p.normalizer.NormalizeVolcanic(ctx, report)
		if err != nil {
			p.log.Error("failed to normalize volcanic report", "report_id", report.ReportID, "error", err)
			continue
		}

		if err := p.repo.SaveHazardWithOutbox(ctx, &canonical); err != nil {
			p.log.Error("failed to persist normalized volcanic hazard", "hazard_id", canonical.HazardID, "error", err)
			continue
		}

		if p.cursor == nil || canonical.OccurredAt.After(*p.cursor) {
			t := canonical.OccurredAt
			p.cursor = &t
		}
	}

	p.log.Debug("PVMBG poll cycle completed", slog.Int("ingested_reports", len(reports)))
}

func randomJitter(base time.Duration, maxJitter time.Duration) time.Duration {
	return base + time.Duration(rand.Int63n(int64(maxJitter)))
}
