package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/logger"
	"bnpb/aggregator/internal/normalizer"
	"bnpb/aggregator/internal/status"
)

const overlapWindow = 10 * time.Second

// HazardSaver provides database operations needed by the poller workers.
type HazardSaver interface {
	SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, correlationID string) error
	GetHazardBySourceRef(ctx context.Context, source domain.Source, sourceRefID string) (*domain.HazardEvent, error)
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

	retryCount := 0
	maxBackoff := 30 * time.Second

	for {
		sleepDuration := p.interval + randomJitter(0, 500*time.Millisecond)
		if retryCount > 0 {
			backoff := p.interval * (1 << retryCount)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			sleepDuration = backoff + randomJitter(0, 1*time.Second)
		}

		select {
		case <-ctx.Done():
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
	var since *time.Time
	if p.cursor != nil {
		t := p.cursor.Add(-overlapWindow)
		since = &t
	}

	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
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
		if err := p.repo.SaveHazardWithOutbox(ctx, &canonical, corrID); err != nil {
			p.log.Error("failed to persist normalized seismic hazard", "hazard_id", canonical.HazardID, "error", err)
			return fmt.Errorf("failed to persist seismic hazard %s: %w", canonical.HazardID, err)
		}

		if p.cursor == nil || canonical.OccurredAt.After(*p.cursor) {
			t := canonical.OccurredAt
			p.cursor = &t
		}
	}

	// Update existing stored hazards if any tsunami warnings arrived
	for _, w := range warnings {
		if w.RelatedEventID == "" {
			continue
		}
		existing, err := p.repo.GetHazardBySourceRef(ctx, domain.SourceBMKG, w.RelatedEventID)
		if err != nil {
			p.log.Debug("could not find existing seismic hazard for tsunami warning", "related_event_id", w.RelatedEventID, "error", err)
			continue
		}
		if existing != nil {
			updated := p.normalizer.ApplyTsunamiWarning(existing, w)
			corrID := fmt.Sprintf("bmkg-tsu-%s", w.WarningID)
			if err := p.repo.SaveHazardWithOutbox(ctx, updated, corrID); err != nil {
				p.log.Error("failed to update hazard with tsunami warning", "hazard_id", updated.HazardID, "error", err)
				return fmt.Errorf("failed to update hazard with tsunami warning %s: %w", updated.HazardID, err)
			}
		}
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

	retryCount := 0
	maxBackoff := 30 * time.Second

	for {
		sleepDuration := p.interval + randomJitter(0, 500*time.Millisecond)
		if retryCount > 0 {
			backoff := p.interval * (1 << retryCount)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			sleepDuration = backoff + randomJitter(0, 1*time.Second)
		}

		select {
		case <-ctx.Done():
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
	var since *time.Time
	if p.cursor != nil {
		t := p.cursor.Add(-overlapWindow)
		since = &t
	}

	pollCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
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
		return nil
	}

	for _, report := range reports {
		canonical, err := p.normalizer.NormalizeVolcanic(ctx, report)
		if err != nil {
			p.log.Error("failed to normalize volcanic report", "report_id", report.ReportID, "error", err)
			continue
		}

		corrID := fmt.Sprintf("pvmbg-%s", report.ReportID)
		if err := p.repo.SaveHazardWithOutbox(ctx, &canonical, corrID); err != nil {
			p.log.Error("failed to persist normalized volcanic hazard", "hazard_id", canonical.HazardID, "error", err)
			return fmt.Errorf("failed to persist volcanic hazard %s: %w", canonical.HazardID, err)
		}

		if p.cursor == nil || canonical.OccurredAt.After(*p.cursor) {
			t := canonical.OccurredAt
			p.cursor = &t
		}
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
