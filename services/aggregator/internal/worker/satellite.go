package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/repository"
	"bnpb/aggregator/internal/satellite"
)

const (
	satelliteStream             = "satellite"
	defaultSatelliteCheckpoint  = 5 * time.Second
	defaultSatelliteReportEvery = 10 * time.Second
	satelliteRetryWait          = 5 * time.Second
	satelliteFatalWait          = 60 * time.Second
	satelliteLatencyWindow      = 1000
)

// SatelliteStore persists wildfire hazards and the stream checkpoint.
type SatelliteStore interface {
	SaveHazardWithOutbox(ctx context.Context, h *domain.HazardEvent, correlationID string) error
	LoadSatelliteCheckpoint(ctx context.Context, stream string) (*repository.SatelliteCheckpoint, error)
	SaveSatelliteCheckpoint(ctx context.Context, stream string, lastOffset int64, state []byte) error
}

// SatelliteFeed is the subset of satellite.Client the worker uses.
type SatelliteFeed interface {
	GetSession(ctx context.Context) (satellite.Session, error)
	Run(ctx context.Context, lastApplied int64, handle func(satellite.Event) error) error
}

// SatelliteStatus is the worker's progress for logs and future metrics (#35).
type SatelliteStatus struct {
	LastAppliedOffset int64           `json:"last_applied_offset"`
	LatestOffset      int64           `json:"latest_offset"`
	LagEvents         int64           `json:"lag_events"`
	P95PersistMillis  float64         `json:"p95_persist_ms"`
	RejectedEvents    int64           `json:"rejected_events"`
	ActiveFires       int             `json:"active_fires"`
	Engine            satellite.Stats `json:"engine"`
}

// SatelliteWorker applies the satellite SSE feed to the wildfire engine and persists the
// resulting hazards. Hazards are written before the checkpoint, so a crash replays from
// the last checkpoint and re-applies idempotent upserts without skipping any event.
type SatelliteWorker struct {
	feed               SatelliteFeed
	store              SatelliteStore
	engineCfg          satellite.Config
	locate             satellite.AreaLocator
	checkpointInterval time.Duration
	reportInterval     time.Duration
	log                *slog.Logger
	now                func() time.Time

	mu        sync.Mutex
	status    SatelliteStatus
	latencies []time.Duration
}

// NewSatelliteWorker creates a worker for one satellite stream.
func NewSatelliteWorker(feed SatelliteFeed, store SatelliteStore, cfg satellite.Config, locate satellite.AreaLocator, log *slog.Logger) *SatelliteWorker {
	return &SatelliteWorker{
		feed:               feed,
		store:              store,
		engineCfg:          cfg,
		locate:             locate,
		checkpointInterval: defaultSatelliteCheckpoint,
		reportInterval:     defaultSatelliteReportEvery,
		log:                log.With(slog.String("worker", "satellite")),
		now:                time.Now,
	}
}

// Status returns a copy of the current progress.
func (w *SatelliteWorker) Status() SatelliteStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
}

// Start runs until ctx is canceled, restarting from the durable checkpoint after failures.
func (w *SatelliteWorker) Start(ctx context.Context) {
	w.log.Info("starting satellite stream worker")
	go w.report(ctx)
	for {
		err := w.runOnce(ctx)
		if ctx.Err() != nil {
			w.log.Info("stopping satellite stream worker")
			return
		}
		wait := satelliteRetryWait
		if errors.Is(err, satellite.ErrUnauthorized) || errors.Is(err, satellite.ErrBadRequest) {
			wait = satelliteFatalWait
		}
		w.log.Error("satellite stream stopped; restarting from checkpoint", "error", err, "retry_in", wait)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (w *SatelliteWorker) restore(ctx context.Context) (*satellite.Engine, int64, error) {
	cp, err := w.store.LoadSatelliteCheckpoint(ctx, satelliteStream)
	if err != nil {
		return nil, 0, err
	}
	if cp == nil {
		return satellite.NewEngine(w.engineCfg), 0, nil
	}
	var st satellite.State
	if err := json.Unmarshal(cp.State, &st); err != nil {
		return nil, 0, fmt.Errorf("decode satellite checkpoint: %w", err)
	}
	engine, err := satellite.Restore(w.engineCfg, st)
	if err != nil {
		return nil, 0, fmt.Errorf("restore satellite checkpoint: %w", err)
	}
	return engine, cp.LastOffset, nil
}

func (w *SatelliteWorker) runOnce(ctx context.Context) error {
	engine, offset, err := w.restore(ctx)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.status.LastAppliedOffset = offset
	w.mu.Unlock()
	w.log.Info("satellite stream resuming", slog.Int64("from_offset", offset), slog.Int("active_fires", len(engine.Fires())))

	lastCheckpoint := w.now()
	dirty := false
	// consistent is false while an event is half applied; a checkpoint is never taken then.
	consistent := true

	runErr := w.feed.Run(ctx, offset, func(ev satellite.Event) error {
		consistent = false
		start := w.now()
		disp, changes, applyErr := satellite.ApplyEvent(engine, ev)
		if applyErr != nil {
			// A poison event cannot block the stream: it is logged with its offset and counted.
			w.mu.Lock()
			w.status.RejectedEvents++
			w.mu.Unlock()
			w.log.Warn("satellite_event_rejected", slog.Int64("offset", ev.Offset), slog.String("type", ev.Type),
				slog.Int("bytes", len(ev.Data)), slog.String("reason", applyErr.Error()))
		}
		for _, c := range changes {
			h := satelliteHazard(c, w.locate)
			if err := w.store.SaveHazardWithOutbox(ctx, &h, fmt.Sprintf("satelit-%d", ev.Offset)); err != nil {
				return fmt.Errorf("persist %s at offset %d: %w", h.HazardID, ev.Offset, err)
			}
		}
		offset, dirty, consistent = ev.Offset, true, true
		w.observe(engine, offset, w.now().Sub(start))
		if disp == satellite.DispositionLate {
			w.log.Info("satellite_detection_late", slog.Int64("offset", ev.Offset), slog.Time("watermark", engine.Watermark()))
		}
		if w.now().Sub(lastCheckpoint) >= w.checkpointInterval {
			if err := w.checkpoint(ctx, engine, offset); err != nil {
				return err
			}
			lastCheckpoint, dirty = w.now(), false
		}
		return nil
	})

	if dirty && consistent {
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := w.checkpoint(saveCtx, engine, offset); err != nil {
			w.log.Error("final satellite checkpoint failed", "error", err)
		}
		cancel()
	}
	return runErr
}

func (w *SatelliteWorker) checkpoint(ctx context.Context, engine *satellite.Engine, offset int64) error {
	state, err := json.Marshal(engine.Snapshot())
	if err != nil {
		return fmt.Errorf("encode satellite checkpoint: %w", err)
	}
	return w.store.SaveSatelliteCheckpoint(ctx, satelliteStream, offset, state)
}

func (w *SatelliteWorker) observe(engine *satellite.Engine, offset int64, latency time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.latencies = append(w.latencies, latency)
	if len(w.latencies) > satelliteLatencyWindow {
		w.latencies = w.latencies[len(w.latencies)-satelliteLatencyWindow:]
	}
	sorted := append([]time.Duration(nil), w.latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	w.status.P95PersistMillis = float64(sorted[(len(sorted)*95-1)/100]) / float64(time.Millisecond)
	w.status.LastAppliedOffset = offset
	if w.status.LatestOffset < offset {
		w.status.LatestOffset = offset
	}
	w.status.LagEvents = w.status.LatestOffset - offset
	w.status.Engine = engine.Stats()
	w.status.ActiveFires = len(engine.Fires())
}

// report refreshes the feed's latest offset and logs progress so processing lag is visible.
func (w *SatelliteWorker) report(ctx context.Context) {
	ticker := time.NewTicker(w.reportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		sessionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		session, err := w.feed.GetSession(sessionCtx)
		cancel()
		w.mu.Lock()
		if err == nil && session.LatestOffset > w.status.LatestOffset {
			w.status.LatestOffset = session.LatestOffset
		}
		w.status.LagEvents = w.status.LatestOffset - w.status.LastAppliedOffset
		st := w.status
		w.mu.Unlock()
		if err != nil {
			w.log.Warn("satellite session check failed", "error", err)
		}
		w.log.Info("satellite_progress",
			slog.Int64("last_applied_offset", st.LastAppliedOffset),
			slog.Int64("latest_offset", st.LatestOffset),
			slog.Int64("lag_events", st.LagEvents),
			slog.Float64("p95_persist_ms", st.P95PersistMillis),
			slog.Int("active_fires", st.ActiveFires),
			slog.Int("singletons", st.Engine.Singletons),
			slog.Int("late_beyond_watermark", st.Engine.Late),
			slog.Int64("rejected_events", st.RejectedEvents),
		)
	}
}

// satelliteHazard maps an engine change to the canonical hazard. The frozen HazardEvent
// has no lifecycle field, so retirement is carried in attributes (additional properties
// are allowed) and the hazard row is kept rather than deleted.
func satelliteHazard(c satellite.Change, locate satellite.AreaLocator) domain.HazardEvent {
	h := c.Fire.ToHazard(locate)
	h.Attributes["lifecycle_status"] = "active"
	if c.Kind == satellite.ChangeRetire {
		h.Attributes["lifecycle_status"] = "retired"
		h.Attributes["retired_reason"] = c.Reason
		if c.MergedTo != "" {
			h.Attributes["merged_into"] = satellite.HazardIDFor(c.MergedTo)
		}
	}
	return h
}
