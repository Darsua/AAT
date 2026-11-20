package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const advisoryDisclaimer = "AI-generated situational summary for informational awareness only; never gates, mutates, or delays canonical BNPB early warnings."

// HazardEvent mirrors the canonical event in contracts/hazard_event.json.
type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    *float64       `json:"latitude,omitempty"`
	Longitude   *float64       `json:"longitude,omitempty"`
	Attributes  map[string]any `json:"attributes"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
}

// CorrelatedCluster groups recent events sharing a normalized geographic corridor.
type CorrelatedCluster struct {
	Region          string   `json:"region"`
	Sources         []string `json:"sources"`
	HazardTypes     []string `json:"hazard_types"`
	HighestSeverity string   `json:"highest_severity"`
	EventCount      int      `json:"event_count"`
	HazardIDs       []string `json:"hazard_ids"`
	MultiSource     bool     `json:"multi_source"`
	MultiHazard     bool     `json:"multi_hazard"`
}

// SituationSummary records one bounded AI or extractive fallback briefing.
type SituationSummary struct {
	ID             string   `json:"id"`
	Generated      bool     `json:"generated"`
	GeneratorMode  string   `json:"generator_mode"`
	Model          string   `json:"model"`
	Degraded       bool     `json:"degraded"`
	DegradedReason string   `json:"degraded_reason,omitempty"`
	Text           string   `json:"text"`
	EventCount     int      `json:"event_count"`
	Sources        []string `json:"sources"`
	HazardIDs      []string `json:"hazard_ids"`
	CorrelationID  string   `json:"correlation_id,omitempty"`
	LatencyMS      int64    `json:"latency_ms"`
	GeneratedAt    string   `json:"generated_at"`
}

// SummaryMetrics captures bounded lock-free and atomic operational telemetry.
type SummaryMetrics struct {
	EventsConsumedTotal     int64 `json:"events_consumed_total"`
	DuplicatesSkippedTotal  int64 `json:"duplicates_skipped_total"`
	SummariesGeneratedTotal int64 `json:"summaries_generated_total"`
	LLMSuccessTotal         int64 `json:"llm_success_total"`
	FallbackTotal           int64 `json:"fallback_total"`
	CoalescedTotal          int64 `json:"coalesced_total"`
	QuotaExhaustedTotal     int64 `json:"quota_exhausted_total"`
	ValidationFailuresTotal int64 `json:"validation_failures_total"`
	LastLatencyMS           int64 `json:"last_latency_ms"`
	WindowEvents            int   `json:"window_events"`
}

// SummariesResponse is the truthful inspection envelope served on /summaries and /summaries/latest.
type SummariesResponse struct {
	Generated          bool                `json:"generated"`
	AdvisoryDisclaimer string              `json:"advisory_disclaimer"`
	Model              string              `json:"model"`
	CircuitState       string              `json:"circuit_state"`
	Latest             *SituationSummary   `json:"latest,omitempty"`
	Summaries          []SituationSummary  `json:"summaries,omitempty"`
	ActiveCorrelations []CorrelatedCluster `json:"active_correlations"`
	BySource           map[string]int      `json:"by_source"`
	ByType             map[string]int      `json:"by_type"`
	BySeverity         map[string]int      `json:"by_severity"`
	Metrics            SummaryMetrics      `json:"metrics"`
}

type triggerJob struct {
	correlationID string
}

// SummarizerEngine maintains bounded recent events, triggers async workers without
// blocking Kafka consumption, and stores generated or fallback briefings.
type SummarizerEngine struct {
	cfg Config
	llm LLMGenerator

	mu            sync.RWMutex
	windowOrder   []string
	windowByID    map[string]HazardEvent
	bySource      map[string]int
	byType        map[string]int
	bySeverity    map[string]int
	summaries     []SituationSummary
	summarySeq    int64
	lastGenAt     time.Time

	triggerCh chan triggerJob

	eventsConsumedTotal     atomic.Int64
	duplicatesSkippedTotal  atomic.Int64
	summariesGeneratedTotal atomic.Int64
	llmSuccessTotal         atomic.Int64
	fallbackTotal           atomic.Int64
	coalescedTotal          atomic.Int64
	quotaExhaustedTotal     atomic.Int64
	validationFailuresTotal atomic.Int64
	lastLatencyMS           atomic.Int64

	onSummary func(SituationSummary) // optional test hook
}

// NewSummarizerEngine constructs a bounded, non-blocking summarizer engine.
func NewSummarizerEngine(cfg Config, llm LLMGenerator) *SummarizerEngine {
	queueSize := cfg.SummaryQueueSize
	if queueSize <= 0 {
		queueSize = 1
	}
	return &SummarizerEngine{
		cfg:        cfg,
		llm:        llm,
		windowByID: make(map[string]HazardEvent),
		bySource:   make(map[string]int),
		byType:     make(map[string]int),
		bySeverity: make(map[string]int),
		triggerCh:  make(chan triggerJob, queueSize),
	}
}

func severityRank(severity string) int {
	switch strings.ToUpper(strings.TrimSpace(severity)) {
	case "AWAS":
		return 4
	case "SIAGA":
		return 3
	case "WASPADA":
		return 2
	case "NORMAL":
		return 1
	default:
		return 0
	}
}

// Ingest records a canonical event in the bounded rolling window and schedules
// asynchronous summarization without blocking the caller.
func (e *SummarizerEngine) Ingest(ev HazardEvent, correlationID string) bool {
	e.mu.Lock()
	existing, exists := e.windowByID[ev.HazardID]
	if exists {
		escalated := severityRank(ev.Severity) > severityRank(existing.Severity) ||
			ev.OccurredAt.After(existing.OccurredAt)
		if !escalated {
			e.mu.Unlock()
			e.duplicatesSkippedTotal.Add(1)
			return false
		}
		// Adjust aggregates when replacing an escalated/updated event.
		e.byType[existing.HazardType] = maxInt(0, e.byType[existing.HazardType]-1)
		e.bySeverity[existing.Severity] = maxInt(0, e.bySeverity[existing.Severity]-1)
		e.byType[ev.HazardType]++
		e.bySeverity[ev.Severity]++
		e.windowByID[ev.HazardID] = ev
	} else {
		e.windowOrder = append(e.windowOrder, ev.HazardID)
		e.windowByID[ev.HazardID] = ev
		e.bySource[ev.Source]++
		e.byType[ev.HazardType]++
		e.bySeverity[ev.Severity]++
		for len(e.windowOrder) > e.cfg.MaxRecentEvents {
			evictID := e.windowOrder[0]
			e.windowOrder = e.windowOrder[1:]
			delete(e.windowByID, evictID)
		}
	}
	e.mu.Unlock()

	e.eventsConsumedTotal.Add(1)
	e.enqueueTrigger(correlationID)
	return true
}

func (e *SummarizerEngine) enqueueTrigger(correlationID string) {
	select {
	case e.triggerCh <- triggerJob{correlationID: correlationID}:
	default:
		e.coalescedTotal.Add(1)
	}
}

// RunWorker consumes queued trigger signals and generates summaries asynchronously
// until ctx is cancelled.
func (e *SummarizerEngine) RunWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-e.triggerCh:
			// Drain additional burst signals accumulated before generating one fresh summary.
			drained := 0
			for {
				select {
				case extra := <-e.triggerCh:
					if extra.correlationID != "" {
						job.correlationID = extra.correlationID
					}
					drained++
				default:
					goto coalesceInterval
				}
			}
		coalesceInterval:
			if drained > 0 {
				e.coalescedTotal.Add(int64(drained))
			}

			if e.cfg.SummaryMinInterval > 0 {
				e.mu.RLock()
				sinceLast := time.Since(e.lastGenAt)
				lastSet := !e.lastGenAt.IsZero()
				e.mu.RUnlock()
				if lastSet && sinceLast < e.cfg.SummaryMinInterval {
					wait := e.cfg.SummaryMinInterval - sinceLast
					timer := time.NewTimer(wait)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					// Drain signals that arrived during the coalescing wait.
					for {
						select {
						case extra := <-e.triggerCh:
							if extra.correlationID != "" {
								job.correlationID = extra.correlationID
							}
							e.coalescedTotal.Add(1)
						default:
							goto generate
						}
					}
				}
			}
		generate:
			e.GenerateOnce(ctx, job.correlationID)
		}
	}
}

// GenerateOnce snapshots up to the latest events and runs the LLM or extractive fallback.
func (e *SummarizerEngine) GenerateOnce(ctx context.Context, correlationID string) SituationSummary {
	events := e.snapshotRecentEvents(5)
	if len(events) == 0 {
		return SituationSummary{}
	}
	clusters := computeCorrelations(events)

	start := time.Now()
	var text string
	mode := "llm"
	degraded := false
	degradedReason := ""

	if !e.cfg.AIEnabled || strings.TrimSpace(e.cfg.AIAPIURL) == "" || e.llm == nil {
		mode = "extractive-fallback"
		degraded = true
		degradedReason = "disabled"
		text = buildDeterministicFallback(events, clusters)
		e.fallbackTotal.Add(1)
	} else {
		systemPrompt, userPrompt := buildSummaryPrompts(events, clusters)
		raw, err := e.llm.Generate(ctx, systemPrompt, userPrompt)
		if err != nil {
			mode = "extractive-fallback"
			degraded = true
			degradedReason = classifyAIError(err)
			if degradedReason == "quota_exhausted" {
				e.quotaExhaustedTotal.Add(1)
			}
			text = buildDeterministicFallback(events, clusters)
			e.fallbackTotal.Add(1)
		} else if !validateSummaryOutput(raw, events) {
			mode = "extractive-fallback"
			degraded = true
			degradedReason = "validation_failed"
			e.validationFailuresTotal.Add(1)
			e.fallbackTotal.Add(1)
			text = buildDeterministicFallback(events, clusters)
		} else {
			text = strings.TrimSpace(raw)
			e.llmSuccessTotal.Add(1)
		}
	}

	latencyMS := time.Since(start).Milliseconds()
	e.lastLatencyMS.Store(latencyMS)
	total := e.summariesGeneratedTotal.Add(1)

	sources := uniqueSortedStrings(events, func(ev HazardEvent) string { return ev.Source })
	ids := make([]string, len(events))
	for i, ev := range events {
		ids[i] = ev.HazardID
	}

	e.mu.Lock()
	e.summarySeq++
	summary := SituationSummary{
		ID:             fmt.Sprintf("sum-%06d", e.summarySeq),
		Generated:      true,
		GeneratorMode:  mode,
		Model:          e.cfg.AIModel,
		Degraded:       degraded,
		DegradedReason: degradedReason,
		Text:           text,
		EventCount:     len(events),
		Sources:        sources,
		HazardIDs:      ids,
		CorrelationID:  correlationID,
		LatencyMS:      latencyMS,
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	e.summaries = append(e.summaries, summary)
	if len(e.summaries) > e.cfg.MaxSummaries {
		e.summaries = e.summaries[len(e.summaries)-e.cfg.MaxSummaries:]
	}
	e.lastGenAt = time.Now()
	hook := e.onSummary
	e.mu.Unlock()

	if total > 0 && hook != nil {
		hook(summary)
	}
	return summary
}

func (e *SummarizerEngine) snapshotRecentEvents(limit int) []HazardEvent {
	e.mu.RLock()
	defer e.mu.RUnlock()

	total := len(e.windowOrder)
	if total == 0 {
		return nil
	}
	start := 0
	if limit > 0 && total > limit {
		start = total - limit
	}
	// Return newest-first slice so prompts prioritize freshest hazards.
	out := make([]HazardEvent, 0, total-start)
	for i := total - 1; i >= start; i-- {
		if ev, ok := e.windowByID[e.windowOrder[i]]; ok {
			out = append(out, ev)
		}
	}
	return out
}

// Snapshot builds a consistent JSON inspection payload.
func (e *SummarizerEngine) Snapshot(includeHistory bool) SummariesResponse {
	e.mu.RLock()
	bySource := copyIntMap(e.bySource)
	byType := copyIntMap(e.byType)
	bySeverity := copyIntMap(e.bySeverity)
	windowLen := len(e.windowOrder)

	var latest *SituationSummary
	var history []SituationSummary
	if n := len(e.summaries); n > 0 {
		cp := e.summaries[n-1]
		latest = &cp
		if includeHistory {
			history = make([]SituationSummary, n)
			for i := 0; i < n; i++ {
				history[i] = e.summaries[n-1-i]
			}
		}
	}
	e.mu.RUnlock()

	recent := e.snapshotRecentEvents(50)
	clusters := computeCorrelations(recent)

	circuitState := "disabled"
	if e.cfg.AIEnabled && strings.TrimSpace(e.cfg.AIAPIURL) != "" && e.llm != nil {
		circuitState = e.llm.CircuitState()
	}

	return SummariesResponse{
		Generated:          true,
		AdvisoryDisclaimer: advisoryDisclaimer,
		Model:              e.cfg.AIModel,
		CircuitState:       circuitState,
		Latest:             latest,
		Summaries:          history,
		ActiveCorrelations: clusters,
		BySource:           bySource,
		ByType:             byType,
		BySeverity:         bySeverity,
		Metrics: SummaryMetrics{
			EventsConsumedTotal:     e.eventsConsumedTotal.Load(),
			DuplicatesSkippedTotal:  e.duplicatesSkippedTotal.Load(),
			SummariesGeneratedTotal: e.summariesGeneratedTotal.Load(),
			LLMSuccessTotal:         e.llmSuccessTotal.Load(),
			FallbackTotal:           e.fallbackTotal.Load(),
			CoalescedTotal:          e.coalescedTotal.Load(),
			QuotaExhaustedTotal:     e.quotaExhaustedTotal.Load(),
			ValidationFailuresTotal: e.validationFailuresTotal.Load(),
			LastLatencyMS:           e.lastLatencyMS.Load(),
			WindowEvents:            windowLen,
		},
	}
}

func formatEventFact(ev HazardEvent) string {
	attrs := ev.Attributes
	details := make([]string, 0, 5)

	switch ev.Source {
	case "BMKG":
		if mag, ok := numAttr(attrs, "magnitude"); ok {
			details = append(details, fmt.Sprintf("M%.1f", mag))
		}
		if depth, ok := numAttr(attrs, "depth_km"); ok {
			details = append(details, fmt.Sprintf("depth=%.0fkm", depth))
		}
		if ts, ok := boolAttr(attrs, "potential_tsunami"); ok && ts {
			details = append(details, "potential_tsunami=true")
		}
		if tw, ok := attrs["tsunami_warning"].(map[string]any); ok {
			if tl := strAttr(tw, "threat_level"); tl != "" {
				details = append(details, "tsunami_threat="+tl)
			}
			if arr := strAttr(tw, "estimated_arrival"); arr != "" {
				details = append(details, "eta="+arr)
			}
		}
	case "PVMBG":
		if vid := strAttr(attrs, "volcano_id"); vid != "" {
			details = append(details, "volcano="+vid)
		}
		if ec, ok := numAttr(attrs, "eruption_count_24h"); ok {
			details = append(details, fmt.Sprintf("eruptions_24h=%.0f", ec))
		}
		if ash, ok := numAttr(attrs, "ash_column_height_m"); ok {
			details = append(details, fmt.Sprintf("ash=%.0fm", ash))
		}
		if conf, ok := numAttr(attrs, "confidence_level"); ok {
			details = append(details, fmt.Sprintf("confidence=%.2f", conf))
		}
	case "SATELIT":
		if dc, ok := numAttr(attrs, "detection_count"); ok {
			details = append(details, fmt.Sprintf("hotspots=%.0f", dc))
		}
		if mc := strAttr(attrs, "max_confidence"); mc != "" {
			details = append(details, "max_conf="+mc)
		}
		if frp, ok := numAttr(attrs, "total_frp_mw"); ok {
			details = append(details, fmt.Sprintf("frp=%.1fMW", frp))
		}
		if sats := strSliceAttr(attrs, "satellites"); len(sats) > 0 {
			details = append(details, "sats="+strings.Join(sats, "/"))
		}
	case "SENSOR_PANTAI":
		if sid := strAttr(attrs, "station_id"); sid != "" {
			details = append(details, "station="+sid)
		}
		if sla, ok := numAttr(attrs, "sea_level_anomaly_m"); ok {
			details = append(details, fmt.Sprintf("sea_anomaly=%.2fm", sla))
		}
		if wh, ok := numAttr(attrs, "wave_height_m"); ok {
			details = append(details, fmt.Sprintf("wave=%.2fm", wh))
		}
		if q := strAttr(attrs, "quality"); q != "" {
			details = append(details, "quality="+q)
		}
		if crit, ok := boolAttr(attrs, "critical"); ok && crit {
			details = append(details, "critical=true")
		}
	default:
		if desc := strAttr(attrs, "description"); desc != "" {
			details = append(details, desc)
		}
	}

	ts := ev.OccurredAt.UTC().Format(time.RFC3339)
	if ev.OccurredAt.IsZero() {
		ts = "unknown-time"
	}
	suffix := ""
	if len(details) > 0 {
		suffix = " (" + strings.Join(details, ", ") + ")"
	}
	return fmt.Sprintf("[%s | %s | %s | %s] %s%s at %s",
		ev.HazardID, ev.Source, ev.HazardType, ev.Severity, ev.AreaName, suffix, ts)
}

func normalizeRegionKey(areaName string) string {
	clean := strings.TrimSpace(areaName)
	if clean == "" {
		return "Unknown"
	}
	if idx := strings.Index(clean, ","); idx >= 0 {
		right := strings.TrimSpace(clean[idx+1:])
		if right != "" {
			if _, err := strconv.ParseFloat(right, 64); err != nil {
				return right
			}
		}
	}
	if idx := strings.Index(clean, " - "); idx >= 0 {
		left := strings.TrimSpace(clean[:idx])
		if left != "" {
			return left
		}
	}
	clean = strings.TrimPrefix(clean, "Gunung ")
	clean = strings.TrimPrefix(clean, "Pesisir ")
	return clean
}

func computeCorrelations(events []HazardEvent) []CorrelatedCluster {
	type acc struct {
		region  string
		sources map[string]struct{}
		types   map[string]struct{}
		ids     []string
		highest string
	}
	groups := make(map[string]*acc)
	for _, ev := range events {
		region := normalizeRegionKey(ev.AreaName)
		key := strings.ToLower(region)
		g, ok := groups[key]
		if !ok {
			g = &acc{
				region:  region,
				sources: make(map[string]struct{}),
				types:   make(map[string]struct{}),
				highest: ev.Severity,
			}
			groups[key] = g
		}
		g.sources[ev.Source] = struct{}{}
		g.types[ev.HazardType] = struct{}{}
		g.ids = append(g.ids, ev.HazardID)
		if severityRank(ev.Severity) > severityRank(g.highest) {
			g.highest = ev.Severity
		}
	}

	clusters := make([]CorrelatedCluster, 0, len(groups))
	for _, g := range groups {
		sources := mapKeysSorted(g.sources)
		types := mapKeysSorted(g.types)
		clusters = append(clusters, CorrelatedCluster{
			Region:          g.region,
			Sources:         sources,
			HazardTypes:     types,
			HighestSeverity: g.highest,
			EventCount:      len(g.ids),
			HazardIDs:       g.ids,
			MultiSource:     len(sources) > 1,
			MultiHazard:     len(types) > 1,
		})
	}

	sort.Slice(clusters, func(i, j int) bool {
		if clusters[i].MultiHazard != clusters[j].MultiHazard {
			return clusters[i].MultiHazard
		}
		ri, rj := severityRank(clusters[i].HighestSeverity), severityRank(clusters[j].HighestSeverity)
		if ri != rj {
			return ri > rj
		}
		if clusters[i].EventCount != clusters[j].EventCount {
			return clusters[i].EventCount > clusters[j].EventCount
		}
		return clusters[i].Region < clusters[j].Region
	})
	return clusters
}

func buildSummaryPrompts(events []HazardEvent, clusters []CorrelatedCluster) (string, string) {
	systemPrompt := "You are the BNPB Indonesia situational awareness summarizer. " +
		"Write one concise operational brief (2-4 sentences, max 900 chars) grounded strictly in the provided canonical facts. " +
		"Always cite exact area names, severities, and sources from the facts, and highlight any compound multi-source/multi-hazard clusters. " +
		"Do not invent measurements, IDs, or recommendations outside the facts."

	var b strings.Builder
	b.WriteString("Active canonical hazard facts (newest first):\n")
	for _, ev := range events {
		b.WriteString("- ")
		b.WriteString(formatEventFact(ev))
		b.WriteByte('\n')
	}
	if len(clusters) > 0 {
		b.WriteString("\nRegional correlation summary:\n")
		limit := len(clusters)
		if limit > 3 {
			limit = 3
		}
		for _, c := range clusters[:limit] {
			tag := "single-hazard"
			if c.MultiHazard || c.MultiSource {
				tag = "COMPOUND"
			}
			fmt.Fprintf(&b, "- %s [%s]: severity=%s, events=%d, sources=%s, types=%s\n",
				c.Region, tag, c.HighestSeverity, c.EventCount,
				strings.Join(c.Sources, "+"), strings.Join(c.HazardTypes, "+"))
		}
	}
	return systemPrompt, b.String()
}

func buildDeterministicFallback(events []HazardEvent, clusters []CorrelatedCluster) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[Extractive Fallback Summary] Monitoring %d recent canonical hazard event(s)", len(events))
	if len(clusters) > 0 {
		top := clusters[0]
		fmt.Fprintf(&b, " across %d region(s); top hotspot %s (%s, %s, %d events)",
			len(clusters), top.Region, top.HighestSeverity, strings.Join(top.Sources, "+"), top.EventCount)
		if top.MultiHazard {
			b.WriteString(" [compound multi-hazard]")
		}
	}
	b.WriteString(". Latest: ")
	limit := len(events)
	if limit > 3 {
		limit = 3
	}
	for i := 0; i < limit; i++ {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(formatEventFact(events[i]))
	}
	b.WriteByte('.')
	return b.String()
}

func numAttr(attrs map[string]any, key string) (float64, bool) {
	if attrs == nil {
		return 0, false
	}
	v, ok := attrs[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

func boolAttr(attrs map[string]any, key string) (bool, bool) {
	if attrs == nil {
		return false, false
	}
	v, ok := attrs[key]
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

func strAttr(attrs map[string]any, key string) string {
	if attrs == nil {
		return ""
	}
	s, _ := attrs[key].(string)
	return strings.TrimSpace(s)
}

func strSliceAttr(attrs map[string]any, key string) []string {
	if attrs == nil {
		return nil
	}
	raw, ok := attrs[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func uniqueSortedStrings(events []HazardEvent, pick func(HazardEvent) string) []string {
	seen := make(map[string]struct{})
	for _, ev := range events {
		if v := strings.TrimSpace(pick(ev)); v != "" {
			seen[v] = struct{}{}
		}
	}
	return mapKeysSorted(seen)
}

func mapKeysSorted(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func copyIntMap(src map[string]int) map[string]int {
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
