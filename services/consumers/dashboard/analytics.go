package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	maxAnalyticsResponseEvents  = 500
	defaultAuditChecksumInterval = 30 * time.Second
)

var errAuditUnavailable = errors.New("audit api unavailable")

//go:embed dashboard.html
var dashboardHTML string

type DashboardStatus struct {
	PartialData         bool       `json:"partial_data"`
	Degraded            bool       `json:"degraded"`
	UnavailableUnits    []string   `json:"unavailable_units,omitempty"`
	PartialReason       string     `json:"partial_reason,omitempty"`
	StaleSince          *time.Time `json:"stale_since,omitempty"`
	LastSyncAt          *time.Time `json:"last_sync_at,omitempty"`
	LastKafkaEventAt    *time.Time `json:"last_kafka_event_at,omitempty"`
	FreshnessLagSeconds *int64     `json:"freshness_lag_seconds,omitempty"`
	ConsistencyModel    string     `json:"consistency_model"`
	SyncIntervalSeconds int        `json:"sync_interval_seconds"`
}

type AreaAggregate struct {
	AreaName string `json:"area_name"`
	Count    int    `json:"count"`
	Awas     int    `json:"awas"`
	Siaga    int    `json:"siaga"`
	Waspada  int    `json:"waspada"`
	Normal   int    `json:"normal"`
}

type TimeBucket struct {
	Bucket  string         `json:"bucket"`
	Total   int            `json:"total"`
	ByType  map[string]int `json:"by_type"`
	Tsunami int            `json:"tsunami"`
}

type AnalyticsFilters struct {
	Type     string `json:"type,omitempty"`
	Severity string `json:"severity,omitempty"`
	Source   string `json:"source,omitempty"`
}

type SnapshotOptions struct {
	Window         string
	SinceOverride  *time.Time
	TypeFilter     string
	SeverityFilter string
	SourceFilter   string
	IncludeEvents  bool
}

type AnalyticsSnapshot struct {
	Window                 string           `json:"window"`
	Since                  *time.Time       `json:"since,omitempty"`
	GeneratedAt            time.Time        `json:"generated_at"`
	TotalHazards           int              `json:"total_hazards"`
	ByType                 map[string]int   `json:"by_type"`
	CorrelatedTsunamiCount int              `json:"correlated_tsunami_count"`
	BySeverity             map[string]int   `json:"by_severity"`
	ByArea                 []AreaAggregate  `json:"by_area"`
	Timeseries             []TimeBucket     `json:"timeseries"`
	Events                 []HazardEvent    `json:"events,omitempty"`
	SourceRefChecksum      string           `json:"source_ref_checksum"`
	Filters                AnalyticsFilters `json:"filters"`
	Status                 DashboardStatus  `json:"status"`
}

type wireAttributes struct {
	PotentialTsunami bool            `json:"potential_tsunami"`
	TsunamiWarning   json.RawMessage `json:"tsunami_warning"`
}

type wireHazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Attributes  wireAttributes `json:"attributes"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
}

func (w wireHazardEvent) toCompact() compactHazard {
	ht := internCanonical(w.HazardType)
	tsunami := false
	if ht == "SEISMIC" {
		if w.Attributes.PotentialTsunami {
			tsunami = true
		} else {
			raw := strings.TrimSpace(string(w.Attributes.TsunamiWarning))
			if raw != "" && raw != "null" {
				tsunami = true
			}
		}
	}
	return compactHazard{
		HazardID:          w.HazardID,
		Source:            internCanonical(w.Source),
		SourceRefID:       w.SourceRefID,
		HazardType:        ht,
		Severity:          internCanonical(w.Severity),
		AreaName:          w.AreaName,
		OccurredAt:        w.OccurredAt.UTC(),
		IngestedAt:        w.IngestedAt.UTC(),
		CorrelatedTsunami: tsunami,
	}
}

type aggregatorListResponse struct {
	Data []wireHazardEvent `json:"data"`
	Meta struct {
		Total      int        `json:"total"`
		Degraded   bool       `json:"degraded"`
		StaleSince *time.Time `json:"stale_since"`
	} `json:"meta"`
}

type auditEventsResponse struct {
	Items            []wireHazardEvent `json:"items"`
	NextCursor       *string           `json:"next_cursor"`
	Partial          bool              `json:"partial"`
	UnavailableUnits []string          `json:"unavailable_units"`
}

type auditChecksumResponse struct {
	Source           string   `json:"source"`
	Count            int      `json:"count"`
	Checksum         string   `json:"checksum"`
	Partial          bool     `json:"partial"`
	UnavailableUnits []string `json:"unavailable_units"`
}

func (d *Dashboard) applySyncStatusLocked(
	degraded bool,
	partial bool,
	unavailableUnits []string,
	staleSince *time.Time,
	reason string,
	now time.Time,
) {
	d.lastSyncAt = &now
	d.degraded = degraded
	d.partialData = partial || degraded
	if len(unavailableUnits) > 0 {
		d.unavailableUnits = append([]string(nil), unavailableUnits...)
	} else {
		d.unavailableUnits = nil
	}
	d.partialReason = strings.TrimSpace(reason)
	if d.partialData && d.partialReason == "" {
		if len(d.unavailableUnits) > 0 {
			d.partialReason = "Partial storage outage: unavailable units " + strings.Join(d.unavailableUnits, ", ")
		} else {
			d.partialReason = "Upstream or storage degradation reported by Aggregator"
		}
	}
	if !d.partialData {
		d.partialReason = ""
		d.staleSince = nil
	} else if staleSince != nil {
		t := staleSince.UTC()
		d.staleSince = &t
	} else if d.staleSince == nil {
		d.staleSince = &now
	}
}

// LocalParity returns the projected event count, SourceRefID checksum, and sync state.
func (d *Dashboard) LocalParity() (count int, checksum string, initialSynced bool, wasDegraded bool, maxIngested time.Time, lastFullSync time.Time) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	refs := make([]string, 0, len(d.events))
	for _, ev := range d.events {
		if ev.SourceRefID != "" {
			refs = append(refs, ev.SourceRefID)
		}
	}
	return len(d.events), computeSourceRefChecksum(refs), d.initialSynced, d.partialData || d.degraded, d.maxIngestedAt, d.lastFullSyncAt
}

// NoteChecksumDrift increments consecutive parity drift detections and returns the count.
func (d *Dashboard) NoteChecksumDrift() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.consecutiveDrift++
	return d.consecutiveDrift
}

// ClearChecksumDrift resets consecutive parity drift detections to zero.
func (d *Dashboard) ClearChecksumDrift() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.consecutiveDrift = 0
}

// ShouldCheckAuditChecksum reports whether enough time has elapsed since the last checksum probe.
func (d *Dashboard) ShouldCheckAuditChecksum(interval time.Duration) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastChecksumAt.IsZero() || time.Since(d.lastChecksumAt) >= interval
}

// RecordAuditChecksumCheck records the timestamp of a completed /internal/audit/checksum call.
func (d *Dashboard) RecordAuditChecksumCheck(at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastChecksumAt = at.UTC()
}

// UpdateSyncStatus updates sync timestamps and degradation flags without altering events.
func (d *Dashboard) UpdateSyncStatus(
	degraded bool,
	partial bool,
	unavailableUnits []string,
	staleSince *time.Time,
	reason string,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.applySyncStatusLocked(degraded, partial, unavailableUnits, staleSince, reason, time.Now().UTC())
}

// MergeCompactDelta upserts incremental records without pruning older entries.
func (d *Dashboard) MergeCompactDelta(
	records []compactHazard,
	degraded bool,
	partial bool,
	unavailableUnits []string,
	staleSince *time.Time,
	reason string,
) {
	d.mu.Lock()
	defer d.mu.Unlock()

	changed := false
	for _, ev := range records {
		if ev.HazardID == "" || ev.HazardType == "" || ev.Severity == "" {
			continue
		}
		if existing, ok := d.events[ev.HazardID]; !ok || !shouldSkipUpdate(existing, ev) {
			d.events[ev.HazardID] = ev
			changed = true
		}
	}
	if changed {
		d.rebuildCountersLocked()
	}
	d.applySyncStatusLocked(degraded, partial, unavailableUnits, staleSince, reason, time.Now().UTC())
}

// ReconcileCompactSnapshot merges a compact full snapshot and removes retracted records when complete.
func (d *Dashboard) ReconcileCompactSnapshot(
	records []compactHazard,
	syncStartedAt time.Time,
	degraded bool,
	partial bool,
	unavailableUnits []string,
	staleSince *time.Time,
	reason string,
) {
	d.mu.Lock()
	defer d.mu.Unlock()

	seen := make(map[string]struct{}, len(records))
	for _, ev := range records {
		if ev.HazardID == "" || ev.HazardType == "" || ev.Severity == "" {
			continue
		}
		seen[ev.HazardID] = struct{}{}
		if existing, ok := d.events[ev.HazardID]; !ok || !shouldSkipUpdate(existing, ev) {
			d.events[ev.HazardID] = ev
		}
	}

	now := time.Now().UTC()
	if !degraded && !partial {
		for id, existing := range d.events {
			if _, ok := seen[id]; ok {
				continue
			}
			// Preserve newly consumed Kafka events that arrived while the snapshot was being fetched.
			if existing.IngestedAt.IsZero() || existing.IngestedAt.Before(syncStartedAt) {
				delete(d.events, id)
			}
		}
		d.initialSynced = true
		d.lastFullSyncAt = now
		d.consecutiveDrift = 0
	}

	d.rebuildCountersLocked()
	d.applySyncStatusLocked(degraded, partial, unavailableUnits, staleSince, reason, now)
}

// ReconcileSnapshot merges an Aggregator snapshot into Dashboard state and removes retracted records when complete.
func (d *Dashboard) ReconcileSnapshot(
	events []HazardEvent,
	syncStartedAt time.Time,
	degraded bool,
	partial bool,
	unavailableUnits []string,
	staleSince *time.Time,
	reason string,
) {
	compact := make([]compactHazard, 0, len(events))
	for _, ev := range events {
		compact = append(compact, toCompactHazard(ev))
	}
	d.ReconcileCompactSnapshot(compact, syncStartedAt, degraded, partial, unavailableUnits, staleSince, reason)
}

// RecordSyncFailure preserves cached aggregates while marking the dashboard partial/degraded.
func (d *Dashboard) RecordSyncFailure(err error, now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	utcNow := now.UTC()
	d.degraded = true
	d.partialData = true
	if err != nil {
		d.partialReason = fmt.Sprintf("Aggregator sync unavailable: %v", err)
	} else {
		d.partialReason = "Aggregator sync unavailable"
	}
	if d.staleSince == nil {
		d.staleSince = &utcNow
	}
}

func resolveWindow(window string, sinceOverride *time.Time, now time.Time) (string, *time.Time, bool) {
	if sinceOverride != nil {
		utc := sinceOverride.UTC()
		hourly := now.Sub(utc) <= 48*time.Hour
		return "custom", &utc, hourly
	}
	switch strings.ToLower(strings.TrimSpace(window)) {
	case "1h":
		t := now.Add(-1 * time.Hour)
		return "1h", &t, true
	case "6h":
		t := now.Add(-6 * time.Hour)
		return "6h", &t, true
	case "24h":
		t := now.Add(-24 * time.Hour)
		return "24h", &t, true
	case "7d":
		t := now.Add(-7 * 24 * time.Hour)
		return "7d", &t, false
	case "30d":
		t := now.Add(-30 * 24 * time.Hour)
		return "30d", &t, false
	default:
		return "all", nil, false
	}
}

func pickTimestamp(occurredAt, ingestedAt time.Time) time.Time {
	if !occurredAt.IsZero() {
		return occurredAt.UTC()
	}
	return ingestedAt.UTC()
}

func eventTimestamp(ev HazardEvent) time.Time {
	return pickTimestamp(ev.OccurredAt, ev.IngestedAt)
}

func hasCorrelatedTsunami(ev HazardEvent) bool {
	if ev.HazardType != "SEISMIC" || ev.Attributes == nil {
		return false
	}
	if pot, ok := ev.Attributes["potential_tsunami"].(bool); ok && pot {
		return true
	}
	if tw, ok := ev.Attributes["tsunami_warning"]; ok && tw != nil {
		return true
	}
	return false
}

func computeSourceRefChecksum(refs []string) string {
	if len(refs) == 0 {
		return ""
	}
	sorted := make([]string, len(refs))
	copy(sorted, refs)
	sort.Strings(sorted)
	joined := strings.Join(sorted, "\n")
	sum := sha256.Sum256([]byte(joined))
	return hex.EncodeToString(sum[:])
}

// Snapshot computes an analytics view over the current projection for the requested time window.
func (d *Dashboard) Snapshot(window string, sinceOverride *time.Time, now time.Time) AnalyticsSnapshot {
	return d.SnapshotWithOptions(SnapshotOptions{
		Window:        window,
		SinceOverride: sinceOverride,
	}, now)
}

// SnapshotWithOptions computes a filtered analytics view over the projection.
func (d *Dashboard) SnapshotWithOptions(opts SnapshotOptions, now time.Time) AnalyticsSnapshot {
	utcNow := now.UTC()
	resolvedWindow, since, hourly := resolveWindow(opts.Window, opts.SinceOverride, utcNow)

	d.mu.RLock()
	defer d.mu.RUnlock()

	byType := map[string]int{
		"SEISMIC":  0,
		"VOLCANIC": 0,
		"WILDFIRE": 0,
		"COASTAL":  0,
	}
	bySeverity := map[string]int{
		"AWAS":    0,
		"SIAGA":   0,
		"WASPADA": 0,
		"NORMAL":  0,
	}
	areas := make(map[string]*AreaAggregate)
	buckets := make(map[string]*TimeBucket)

	total := 0
	tsunamiCount := 0
	var sourceRefs []string
	var matchingEvents []HazardEvent

	for _, ev := range d.events {
		if opts.TypeFilter != "" && !strings.EqualFold(ev.HazardType, opts.TypeFilter) {
			continue
		}
		if opts.SeverityFilter != "" && !strings.EqualFold(ev.Severity, opts.SeverityFilter) {
			continue
		}
		if opts.SourceFilter != "" && !strings.EqualFold(ev.Source, opts.SourceFilter) {
			continue
		}

		ts := pickTimestamp(ev.OccurredAt, ev.IngestedAt)
		if since != nil && !ts.IsZero() && ts.Before(*since) {
			continue
		}

		total++
		byType[ev.HazardType]++
		bySeverity[ev.Severity]++
		if ev.CorrelatedTsunami {
			tsunamiCount++
		}
		if ev.SourceRefID != "" {
			sourceRefs = append(sourceRefs, ev.SourceRefID)
		}
		if opts.IncludeEvents {
			matchingEvents = append(matchingEvents, ev.toHazardEvent())
		}

		areaName := strings.TrimSpace(ev.AreaName)
		if areaName == "" {
			areaName = "Unknown Area"
		}
		area := areas[areaName]
		if area == nil {
			area = &AreaAggregate{AreaName: areaName}
			areas[areaName] = area
		}
		area.Count++
		switch ev.Severity {
		case "AWAS":
			area.Awas++
		case "SIAGA":
			area.Siaga++
		case "WASPADA":
			area.Waspada++
		case "NORMAL":
			area.Normal++
		}

		if !ts.IsZero() {
			bucketKey := ts.Format("2006-01-02")
			if hourly {
				bucketKey = ts.Format("2006-01-02T15:00Z")
			}
			b := buckets[bucketKey]
			if b == nil {
				b = &TimeBucket{
					Bucket: bucketKey,
					ByType: map[string]int{
						"SEISMIC":  0,
						"VOLCANIC": 0,
						"WILDFIRE": 0,
						"COASTAL":  0,
					},
				}
				buckets[bucketKey] = b
			}
			b.Total++
			b.ByType[ev.HazardType]++
			if ev.CorrelatedTsunami {
				b.Tsunami++
			}
		}
	}

	byArea := make([]AreaAggregate, 0, len(areas))
	for _, agg := range areas {
		byArea = append(byArea, *agg)
	}
	sort.Slice(byArea, func(i, j int) bool {
		if byArea[i].Count != byArea[j].Count {
			return byArea[i].Count > byArea[j].Count
		}
		return byArea[i].AreaName < byArea[j].AreaName
	})

	timeseries := make([]TimeBucket, 0, len(buckets))
	for _, b := range buckets {
		timeseries = append(timeseries, *b)
	}
	sort.Slice(timeseries, func(i, j int) bool {
		return timeseries[i].Bucket < timeseries[j].Bucket
	})

	if len(matchingEvents) > 0 {
		sort.Slice(matchingEvents, func(i, j int) bool {
			ti := eventTimestamp(matchingEvents[i])
			tj := eventTimestamp(matchingEvents[j])
			if !ti.Equal(tj) {
				return ti.After(tj)
			}
			return matchingEvents[i].HazardID < matchingEvents[j].HazardID
		})
		if len(matchingEvents) > maxAnalyticsResponseEvents {
			matchingEvents = matchingEvents[:maxAnalyticsResponseEvents]
		}
	}

	syncInterval := d.syncIntervalSeconds
	if syncInterval <= 0 {
		syncInterval = int(defaultSyncInterval.Seconds())
	}

	status := DashboardStatus{
		PartialData:         d.partialData,
		Degraded:            d.degraded,
		UnavailableUnits:    append([]string(nil), d.unavailableUnits...),
		PartialReason:       d.partialReason,
		ConsistencyModel:    "eventual_consistency",
		SyncIntervalSeconds: syncInterval,
	}
	if d.staleSince != nil {
		t := *d.staleSince
		status.StaleSince = &t
	}
	if d.lastSyncAt != nil {
		t := *d.lastSyncAt
		status.LastSyncAt = &t
		lag := int64(utcNow.Sub(t).Seconds())
		if lag < 0 {
			lag = 0
		}
		status.FreshnessLagSeconds = &lag
	}
	if d.lastKafkaEventAt != nil {
		t := *d.lastKafkaEventAt
		status.LastKafkaEventAt = &t
	}

	return AnalyticsSnapshot{
		Window:                 resolvedWindow,
		Since:                  since,
		GeneratedAt:            utcNow,
		TotalHazards:           total,
		ByType:                 byType,
		CorrelatedTsunamiCount: tsunamiCount,
		BySeverity:             bySeverity,
		ByArea:                 byArea,
		Timeseries:             timeseries,
		Events:                 matchingEvents,
		SourceRefChecksum:      computeSourceRefChecksum(sourceRefs),
		Filters: AnalyticsFilters{
			Type:     opts.TypeFilter,
			Severity: opts.SeverityFilter,
			Source:   opts.SourceFilter,
		},
		Status: status,
	}
}

type syncProbe struct {
	total      int
	degraded   bool
	partial    bool
	staleSince *time.Time
	units      map[string]struct{}
}

func addUnavailableUnits(dst map[string]struct{}, units []string) {
	for _, u := range units {
		if trimmed := strings.TrimSpace(u); trimmed != "" {
			dst[trimmed] = struct{}{}
		}
	}
}

func sortedUnavailableUnits(unitSet map[string]struct{}) []string {
	if len(unitSet) == 0 {
		return nil
	}
	out := make([]string, 0, len(unitSet))
	for u := range unitSet {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

func buildSyncStatusReason(degraded, partial bool, unavailableUnits []string) string {
	if partial && len(unavailableUnits) > 0 {
		return "Partial storage outage: unavailable units " + strings.Join(unavailableUnits, ", ")
	}
	if partial {
		return "Partial storage outage reported by Aggregator"
	}
	if degraded {
		return "Upstream or storage degradation reported by Aggregator"
	}
	return ""
}

func doGetBody(ctx context.Context, client *http.Client, endpoint, bearerToken string) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	_ = resp.Body.Close()
	if readErr != nil {
		return resp.StatusCode, resp.Header, nil, readErr
	}
	return resp.StatusCode, resp.Header, body, nil
}

func fetchHazardProbe(ctx context.Context, baseURL string, client *http.Client) (syncProbe, error) {
	endpoint := fmt.Sprintf("%s/internal/hazards?limit=1&offset=0", strings.TrimRight(baseURL, "/"))
	status, hdr, body, err := doGetBody(ctx, client, endpoint, "")
	if err != nil {
		return syncProbe{}, err
	}
	if status != http.StatusOK {
		return syncProbe{}, fmt.Errorf("aggregator returned HTTP %d", status)
	}
	var page aggregatorListResponse
	if err := json.Unmarshal(body, &page); err != nil {
		return syncProbe{}, fmt.Errorf("decode aggregator response: %w", err)
	}
	p := syncProbe{
		total:    page.Meta.Total,
		degraded: page.Meta.Degraded,
		units:    make(map[string]struct{}),
	}
	if page.Meta.StaleSince != nil {
		t := page.Meta.StaleSince.UTC()
		p.staleSince = &t
	}
	if strings.EqualFold(hdr.Get("X-Storage-Partial"), "true") {
		p.partial = true
	}
	if rawUnits := strings.TrimSpace(hdr.Get("X-Unavailable-Units")); rawUnits != "" {
		p.partial = true
		addUnavailableUnits(p.units, strings.Split(rawUnits, ","))
	}
	return p, nil
}

func fetchAuditPages(ctx context.Context, baseURL, auditToken string, client *http.Client, since *time.Time) ([]compactHazard, bool, []string, error) {
	base := strings.TrimRight(baseURL, "/") + "/internal/audit/events"
	var out []compactHazard
	partialAny := false
	unitSet := make(map[string]struct{})
	cursor := ""

	for {
		params := url.Values{}
		params.Set("limit", "1000")
		if since != nil && !since.IsZero() {
			params.Set("since", since.UTC().Format(time.RFC3339Nano))
		}
		if cursor != "" {
			params.Set("cursor", cursor)
		}

		status, _, body, err := doGetBody(ctx, client, base+"?"+params.Encode(), auditToken)
		if err != nil {
			return nil, false, nil, err
		}
		if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound {
			return nil, false, nil, errAuditUnavailable
		}
		if status != http.StatusOK {
			return nil, false, nil, fmt.Errorf("audit events returned HTTP %d", status)
		}

		var page auditEventsResponse
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, false, nil, fmt.Errorf("decode audit events response: %w", err)
		}
		if page.Partial {
			partialAny = true
		}
		addUnavailableUnits(unitSet, page.UnavailableUnits)

		for _, item := range page.Items {
			out = append(out, item.toCompact())
		}
		if len(page.Items) == 0 || page.NextCursor == nil || *page.NextCursor == "" || *page.NextCursor == cursor {
			break
		}
		cursor = *page.NextCursor
	}
	return out, partialAny, sortedUnavailableUnits(unitSet), nil
}

func fetchAuditChecksum(ctx context.Context, baseURL, auditToken string, client *http.Client) (auditChecksumResponse, error) {
	endpoint := strings.TrimRight(baseURL, "/") + "/internal/audit/checksum"
	status, _, body, err := doGetBody(ctx, client, endpoint, auditToken)
	if err != nil {
		return auditChecksumResponse{}, err
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound {
		return auditChecksumResponse{}, errAuditUnavailable
	}
	if status != http.StatusOK {
		return auditChecksumResponse{}, fmt.Errorf("audit checksum returned HTTP %d", status)
	}
	var cs auditChecksumResponse
	if err := json.Unmarshal(body, &cs); err != nil {
		return auditChecksumResponse{}, fmt.Errorf("decode audit checksum response: %w", err)
	}
	return cs, nil
}

func runLegacyFullSync(ctx context.Context, baseURL string, client *http.Client, dashboard *Dashboard, syncStartedAt time.Time) error {
	const pageSize = 500
	var allEvents []compactHazard
	degraded := false
	partial := false
	var staleSince *time.Time
	unitSet := make(map[string]struct{})

	for offset := 0; ; {
		endpoint := fmt.Sprintf("%s/internal/hazards?limit=%d&offset=%d", strings.TrimRight(baseURL, "/"), pageSize, offset)
		status, hdr, body, err := doGetBody(ctx, client, endpoint, "")
		if err != nil {
			dashboard.RecordSyncFailure(err, time.Now().UTC())
			return err
		}
		if status != http.StatusOK {
			err := fmt.Errorf("aggregator returned HTTP %d", status)
			dashboard.RecordSyncFailure(err, time.Now().UTC())
			return err
		}

		var page aggregatorListResponse
		if err := json.Unmarshal(body, &page); err != nil {
			err = fmt.Errorf("decode aggregator response: %w", err)
			dashboard.RecordSyncFailure(err, time.Now().UTC())
			return err
		}

		if page.Meta.Degraded {
			degraded = true
		}
		if staleSince == nil && page.Meta.StaleSince != nil {
			t := page.Meta.StaleSince.UTC()
			staleSince = &t
		}
		if strings.EqualFold(hdr.Get("X-Storage-Partial"), "true") {
			partial = true
		}
		if units := strings.TrimSpace(hdr.Get("X-Unavailable-Units")); units != "" {
			partial = true
			addUnavailableUnits(unitSet, strings.Split(units, ","))
		}

		for _, ev := range page.Data {
			allEvents = append(allEvents, ev.toCompact())
		}
		if len(page.Data) == 0 || offset+len(page.Data) >= page.Meta.Total {
			break
		}
		offset += len(page.Data)
	}

	unavailableUnits := sortedUnavailableUnits(unitSet)
	reason := buildSyncStatusReason(degraded, partial, unavailableUnits)
	dashboard.ReconcileCompactSnapshot(allEvents, syncStartedAt, degraded, partial, unavailableUnits, staleSince, reason)
	return nil
}

func syncFromAggregator(ctx context.Context, baseURL string, client *http.Client, dashboard *Dashboard) error {
	return syncFromAggregatorWithAudit(ctx, baseURL, "", defaultFullSyncInterval, client, dashboard)
}

func syncFromAggregatorWithAudit(
	ctx context.Context,
	baseURL string,
	auditToken string,
	fullSyncInterval time.Duration,
	client *http.Client,
	dashboard *Dashboard,
) error {
	if strings.TrimSpace(baseURL) == "" || dashboard == nil {
		return nil
	}
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	if fullSyncInterval <= 0 {
		fullSyncInterval = defaultFullSyncInterval
	}

	recordErr := func(err error) error {
		dashboard.RecordSyncFailure(err, time.Now().UTC())
		return err
	}

	syncStartedAt := time.Now().UTC()
	probe, err := fetchHazardProbe(ctx, baseURL, client)
	if err != nil {
		return recordErr(err)
	}

	unavailableUnits := sortedUnavailableUnits(probe.units)
	reason := buildSyncStatusReason(probe.degraded, probe.partial, unavailableUnits)

	localCount, localChecksum, initialSynced, wasDegraded, maxIngested, lastFullSync := dashboard.LocalParity()

	legacyOrStatus := func() error {
		if (probe.partial || probe.degraded) && initialSynced {
			dashboard.UpdateSyncStatus(probe.degraded, probe.partial, unavailableUnits, probe.staleSince, reason)
			return nil
		}
		if !probe.partial && !probe.degraded && initialSynced && !wasDegraded && localCount == probe.total && time.Since(lastFullSync) < fullSyncInterval {
			dashboard.UpdateSyncStatus(false, false, nil, nil, "")
			return nil
		}
		return runLegacyFullSync(ctx, baseURL, client, dashboard, syncStartedAt)
	}

	if auditToken == "" {
		return legacyOrStatus()
	}

	if initialSynced && !wasDegraded && !probe.partial && !probe.degraded && time.Since(lastFullSync) < fullSyncInterval {
		if !maxIngested.IsZero() {
			since := maxIngested.Add(-15 * time.Second).UTC()
			deltaEvents, dPartial, dUnits, err := fetchAuditPages(ctx, baseURL, auditToken, client, &since)
			if errors.Is(err, errAuditUnavailable) {
				return legacyOrStatus()
			}
			if err != nil {
				return recordErr(err)
			}
			if dPartial {
				probe.partial = true
			}
			addUnavailableUnits(probe.units, dUnits)
			unavailableUnits = sortedUnavailableUnits(probe.units)
			reason = buildSyncStatusReason(probe.degraded, probe.partial, unavailableUnits)
			if len(deltaEvents) > 0 {
				dashboard.MergeCompactDelta(deltaEvents, probe.degraded, probe.partial, unavailableUnits, probe.staleSince, reason)
				localCount, localChecksum, _, _, _, lastFullSync = dashboard.LocalParity()
			}
		}

		if localCount != probe.total || dashboard.ShouldCheckAuditChecksum(defaultAuditChecksumInterval) {
			cs, err := fetchAuditChecksum(ctx, baseURL, auditToken, client)
			if errors.Is(err, errAuditUnavailable) {
				return legacyOrStatus()
			}
			if err != nil {
				return recordErr(err)
			}
			dashboard.RecordAuditChecksumCheck(time.Now().UTC())
			if cs.Partial {
				probe.partial = true
			}
			addUnavailableUnits(probe.units, cs.UnavailableUnits)
			unavailableUnits = sortedUnavailableUnits(probe.units)
			reason = buildSyncStatusReason(probe.degraded, probe.partial, unavailableUnits)

			if probe.partial || probe.degraded {
				dashboard.UpdateSyncStatus(probe.degraded, probe.partial, unavailableUnits, probe.staleSince, reason)
				return nil
			}

			checksumMatch := (localCount == 0 && cs.Count == 0) || localChecksum == cs.Checksum
			if localCount == cs.Count && checksumMatch {
				dashboard.ClearChecksumDrift()
				dashboard.UpdateSyncStatus(false, false, nil, nil, "")
				return nil
			}

			drift := dashboard.NoteChecksumDrift()
			if !((localCount > cs.Count || drift >= 3) && time.Since(lastFullSync) >= 30*time.Second) && localCount <= cs.Count {
				dashboard.UpdateSyncStatus(probe.degraded, probe.partial, unavailableUnits, probe.staleSince, reason)
				return nil
			}
		} else {
			dashboard.ClearChecksumDrift()
			dashboard.UpdateSyncStatus(false, false, nil, nil, "")
			return nil
		}
	}

	if (probe.partial || probe.degraded) && initialSynced {
		dashboard.UpdateSyncStatus(probe.degraded, probe.partial, unavailableUnits, probe.staleSince, reason)
		return nil
	}

	allEvents, fPartial, fUnits, err := fetchAuditPages(ctx, baseURL, auditToken, client, nil)
	if errors.Is(err, errAuditUnavailable) {
		return legacyOrStatus()
	}
	if err != nil {
		return recordErr(err)
	}
	if fPartial {
		probe.partial = true
	}
	addUnavailableUnits(probe.units, fUnits)
	unavailableUnits = sortedUnavailableUnits(probe.units)
	reason = buildSyncStatusReason(probe.degraded, probe.partial, unavailableUnits)
	dashboard.ReconcileCompactSnapshot(allEvents, syncStartedAt, probe.degraded, probe.partial, unavailableUnits, probe.staleSince, reason)
	return nil
}

func startAggregatorSyncLoop(ctx context.Context, baseURL string, interval time.Duration, client *http.Client, dashboard *Dashboard) {
	startAggregatorSyncLoopWithConfig(ctx, Config{
		AggregatorURL:    baseURL,
		SyncInterval:     interval,
		FullSyncInterval: defaultFullSyncInterval,
	}, client, dashboard)
}

func startAggregatorSyncLoopWithConfig(ctx context.Context, cfg Config, client *http.Client, dashboard *Dashboard) {
	if strings.TrimSpace(cfg.AggregatorURL) == "" || dashboard == nil {
		return
	}
	interval := cfg.SyncInterval
	if interval <= 0 {
		interval = defaultSyncInterval
	}
	fullSyncInterval := cfg.FullSyncInterval
	if fullSyncInterval <= 0 {
		fullSyncInterval = defaultFullSyncInterval
	}
	go func() {
		if err := syncFromAggregatorWithAudit(ctx, cfg.AggregatorURL, cfg.AuditToken, fullSyncInterval, client, dashboard); err != nil && ctx.Err() == nil {
			log.Printf("[dashboard] initial aggregator sync failed: %v", err)
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := syncFromAggregatorWithAudit(ctx, cfg.AggregatorURL, cfg.AuditToken, fullSyncInterval, client, dashboard); err != nil && ctx.Err() == nil {
					log.Printf("[dashboard] aggregator sync failed: %v", err)
				}
			}
		}
	}()
}

func analyticsHandler(dashboard *Dashboard) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		opts := SnapshotOptions{
			Window:         r.URL.Query().Get("window"),
			TypeFilter:     strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("type"))),
			SeverityFilter: strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("severity"))),
			SourceFilter:   strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("source"))),
			IncludeEvents:  strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("include_events")), "true"),
		}
		if rawSince := strings.TrimSpace(r.URL.Query().Get("since")); rawSince != "" {
			parsed, err := time.Parse(time.RFC3339, rawSince)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":   "BAD_REQUEST",
					"message": "Invalid query parameter 'since': expected RFC3339 timestamp",
				})
				return
			}
			opts.SinceOverride = &parsed
		}

		snap := dashboard.SnapshotWithOptions(opts, time.Now().UTC())
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(snap)
	}
}

func dashboardPageHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = io.WriteString(w, dashboardHTML)
}
