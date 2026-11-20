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
	"bnpb/aggregator/internal/telemetry"
)

const (
	satelliteStream             = "satellite"
	defaultSatelliteCheckpoint  = 5 * time.Second
	defaultSatelliteReportEvery = 10 * time.Second
	satelliteRetryWait          = 5 * time.Second
	satelliteFatalWait          = 60 * time.Second
	satelliteLatencyWindow      = 1000
	defaultSatelliteRetention   = 48 * time.Hour
	satelliteMetricsEvery       = 2 * time.Second
)

// SatelliteStore persists wildfire hazards and the stream checkpoint. When it also
// implements FencedCoordinator, the worker streams only while it holds the SATELIT
// lease and every write is fenced, so one aggregator replica streams at a time.
type SatelliteStore interface {
	HazardSaver
	LoadSatelliteCheckpoint(ctx context.Context, stream string) (*repository.SatelliteCheckpoint, error)
	SaveSatelliteCheckpoint(ctx context.Context, cp repository.SatelliteCheckpoint, fence *domain.WriteFence) error
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
	Streaming         bool            `json:"streaming"`
	CheckpointBytes   int             `json:"checkpoint_bytes"`
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
	lease              leaseGuard
	checkpointInterval time.Duration
	reportInterval     time.Duration
	retention          time.Duration
	reg                *telemetry.Registry
	log                *slog.Logger
	now                func() time.Time

	mu        sync.Mutex
	status    SatelliteStatus
	latencies []time.Duration
	// active holds the IDs of current fires, maintained from engine changes so
	// progress tracking stays O(1) per event instead of rebuilding every fire.
	active map[string]struct{}
}

// NewSatelliteWorker creates a worker for one satellite stream.
func NewSatelliteWorker(feed SatelliteFeed, store SatelliteStore, cfg satellite.Config, locate satellite.AreaLocator, log *slog.Logger) *SatelliteWorker {
	return &SatelliteWorker{
		feed:               feed,
		store:              store,
		engineCfg:          cfg,
		locate:             locate,
		lease:              newLeaseGuard(store, domain.SourceSatelit, log.With(slog.String("worker", "satellite"))),
		checkpointInterval: defaultSatelliteCheckpoint,
		reportInterval:     defaultSatelliteReportEvery,
		retention:          defaultSatelliteRetention,
		log:                log.With(slog.String("worker", "satellite")),
		now:                time.Now,
	}
}

// WithRetention sets how far behind the newest acquisition time a fire must fall before
// it is closed and its detections are dropped from memory and checkpoints.
func (w *SatelliteWorker) WithRetention(d time.Duration) *SatelliteWorker {
	if d > 0 {
		w.retention = d
	}
	return w
}

// WithTelemetry publishes stream progress and per-event latency as Prometheus metrics.
func (w *SatelliteWorker) WithTelemetry(reg *telemetry.Registry) *SatelliteWorker {
	w.reg = reg
	return w
}

// WithLeaseConfig overrides replica holder identity, lease TTL, and renewal cadence.
func (w *SatelliteWorker) WithLeaseConfig(holderID string, leaseTTL, renewInterval time.Duration) *SatelliteWorker {
	w.lease.configure(holderID, leaseTTL, renewInterval)
	return w
}

// Status returns a copy of the current progress.
func (w *SatelliteWorker) Status() SatelliteStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status.P95PersistMillis = w.p95Locked()
	return w.status
}

// Start runs until ctx is canceled. A replica streams only while it holds the SATELIT
// lease; standbys retry the lease every renew interval. Each leadership term and each
// failure restarts from the durable checkpoint.
func (w *SatelliteWorker) Start(ctx context.Context) {
	w.log.Info("starting satellite stream worker", slog.String("holder_id", w.lease.holderID))
	go w.report(ctx)
	if w.reg != nil {
		go w.publishMetrics(ctx)
	}
	for {
		fence, streamCtx, stopRenew, leader, err := w.lease.acquireCycle(ctx)
		wait := w.lease.renewInterval
		if err == nil && leader {
			w.setStreaming(true)
			err = w.runOnce(streamCtx, fence)
			w.setStreaming(false)
			lostLease := streamCtx.Err() != nil && ctx.Err() == nil
			stopRenew()
			switch {
			case ctx.Err() != nil:
			case lostLease || errors.Is(err, repository.ErrStaleFencingToken):
				w.log.Warn("satellite lease lost; standing by", "error", err)
			default:
				wait = satelliteRetryWait
				if errors.Is(err, satellite.ErrUnauthorized) || errors.Is(err, satellite.ErrBadRequest) {
					wait = satelliteFatalWait
				}
				w.log.Error("satellite stream stopped; restarting from checkpoint", "error", err, "retry_in", wait)
			}
		} else if err != nil {
			w.log.Warn("satellite lease check failed", "error", err)
		}
		if ctx.Err() != nil {
			w.lease.releaseOnShutdown()
			w.log.Info("stopping satellite stream worker")
			return
		}
		select {
		case <-ctx.Done():
			w.lease.releaseOnShutdown()
			w.log.Info("stopping satellite stream worker")
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
		return satellite.NewEngine(w.engineCfg), -1, nil // feed offsets start at 0
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

func (w *SatelliteWorker) runOnce(ctx context.Context, fence *domain.WriteFence) error {
	engine, offset, err := w.restore(ctx)
	if err != nil {
		return err
	}
	fires := engine.Fires()
	w.mu.Lock()
	w.status.LastAppliedOffset = offset
	w.active = make(map[string]struct{}, len(fires))
	for _, f := range fires {
		w.active[f.ID] = struct{}{}
	}
	w.status.ActiveFires = len(w.active)
	w.mu.Unlock()
	w.log.Info("satellite stream resuming", slog.Int64("from_offset", offset), slog.Int("active_fires", len(fires)))

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
			if err := w.lease.saveHazard(ctx, &h, fmt.Sprintf("satelit-%d", ev.Offset), fence); err != nil {
				return fmt.Errorf("persist %s at offset %d: %w", h.HazardID, ev.Offset, err)
			}
		}
		offset, dirty, consistent = ev.Offset, true, true
		w.observe(engine, changes, offset, w.now().Sub(start))
		if disp == satellite.DispositionLate {
			w.log.Info("satellite_detection_late", slog.Int64("offset", ev.Offset), slog.Time("watermark", engine.Watermark()))
		}
		if w.now().Sub(lastCheckpoint) >= w.checkpointInterval {
			if err := w.checkpoint(ctx, engine, offset, fence); err != nil {
				return err
			}
			lastCheckpoint, dirty = w.now(), false
		}
		return nil
	})

	if dirty && consistent {
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := w.checkpoint(saveCtx, engine, offset, fence); err != nil {
			w.log.Error("final satellite checkpoint failed", "error", err)
		}
		cancel()
	}
	return runErr
}

func (w *SatelliteWorker) checkpoint(ctx context.Context, engine *satellite.Engine, offset int64, fence *domain.WriteFence) error {
	// Close fires that can no longer grow before snapshotting, so memory and checkpoint
	// size track the active window instead of the whole stream.
	if closed, dropped := engine.Evict(w.retention); dropped > 0 {
		w.mu.Lock()
		for _, id := range closed {
			delete(w.active, id)
		}
		w.status.ActiveFires = len(w.active)
		w.status.Engine = engine.Stats()
		w.mu.Unlock()
		w.log.Info("satellite_state_evicted", slog.Int("closed_fires", len(closed)), slog.Int("dropped_detections", dropped),
			slog.Duration("retention", w.retention))
	}
	state, err := json.Marshal(engine.Snapshot())
	if err != nil {
		return fmt.Errorf("encode satellite checkpoint: %w", err)
	}
	w.mu.Lock()
	w.status.CheckpointBytes = len(state)
	w.mu.Unlock()
	err = w.store.SaveSatelliteCheckpoint(ctx, repository.SatelliteCheckpoint{
		Stream:     satelliteStream,
		LastOffset: offset,
		State:      state,
	}, fence)
	if errors.Is(err, repository.ErrStaleFencingToken) {
		w.lease.fencingToken = 0
	}
	return err
}

func (w *SatelliteWorker) observe(engine *satellite.Engine, changes []satellite.Change, offset int64, latency time.Duration) {
	if w.reg != nil {
		w.reg.ObserveHistogram("satellite_event_persist_duration_seconds", map[string]string{"service": "aggregator"}, latency.Seconds())
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.latencies = append(w.latencies, latency)
	if len(w.latencies) > 2*satelliteLatencyWindow {
		w.latencies = append(w.latencies[:0], w.latencies[len(w.latencies)-satelliteLatencyWindow:]...)
	}
	for _, c := range changes {
		if c.Kind == satellite.ChangeRetire {
			delete(w.active, c.Fire.ID)
		} else {
			w.active[c.Fire.ID] = struct{}{}
		}
	}
	w.status.LastAppliedOffset = offset
	if w.status.LatestOffset < offset {
		w.status.LatestOffset = offset
	}
	w.status.LagEvents = w.status.LatestOffset - offset
	w.status.Engine = engine.Stats()
	w.status.ActiveFires = len(w.active)
}

// p95Locked returns the p95 of the last satelliteLatencyWindow per-event apply-and-persist times.
func (w *SatelliteWorker) p95Locked() float64 {
	window := w.latencies
	if len(window) > satelliteLatencyWindow {
		window = window[len(window)-satelliteLatencyWindow:]
	}
	if len(window) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), window...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return float64(sorted[(len(sorted)*95-1)/100]) / float64(time.Millisecond)
}

func (w *SatelliteWorker) setStreaming(on bool) {
	w.mu.Lock()
	w.status.Streaming = on
	w.mu.Unlock()
}

// publishMetrics exports stream progress as gauges. Only the lease holder streams, so
// standbys report streaming=0; dashboards take the max across instances.
func (w *SatelliteWorker) publishMetrics(ctx context.Context) {
	ticker := time.NewTicker(satelliteMetricsEvery)
	defer ticker.Stop()
	labels := map[string]string{"service": "aggregator"}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		st := w.Status()
		streaming := 0.0
		if st.Streaming {
			streaming = 1
		}
		for name, v := range map[string]float64{
			"satellite_streaming":           streaming,
			"satellite_last_applied_offset": float64(st.LastAppliedOffset),
			"satellite_latest_offset":       float64(st.LatestOffset),
			"satellite_lag_events":          float64(st.LagEvents),
			"satellite_active_fires":        float64(st.ActiveFires),
			"satellite_closed_fires":        float64(st.Engine.ClosedFires),
			"satellite_singletons":          float64(st.Engine.Singletons),
			"satellite_late_detections":     float64(st.Engine.Late),
			"satellite_unknown_references":  float64(st.Engine.Unknown),
			"satellite_rejected_events":     float64(st.RejectedEvents),
			"satellite_checkpoint_bytes":    float64(st.CheckpointBytes),
			"satellite_p95_persist_seconds": st.P95PersistMillis / 1000,
		} {
			w.reg.SetGauge(name, labels, v)
		}
	}
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
		w.status.P95PersistMillis = w.p95Locked()
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
// are allowed) and the hazard row is kept rather than deleted. A retired fire drops to
// NORMAL so consumers see a de-escalation instead of a repeated alert.
func satelliteHazard(c satellite.Change, locate satellite.AreaLocator) domain.HazardEvent {
	h := c.Fire.ToHazard(locate)
	h.Attributes["lifecycle_status"] = "active"
	if c.Kind == satellite.ChangeRetire {
		h.Severity = domain.SeverityNormal
		h.Attributes["severity_before_retire"] = string(c.Fire.Severity)
		h.Attributes["lifecycle_status"] = "retired"
		h.Attributes["retired_reason"] = c.Reason
		if c.MergedTo != "" {
			h.Attributes["merged_into"] = satellite.HazardIDFor(c.MergedTo)
		}
	}
	return h
}
