package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

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

type aggregatorListResponse struct {
	Data []HazardEvent `json:"data"`
	Meta struct {
		Total      int        `json:"total"`
		Degraded   bool       `json:"degraded"`
		StaleSince *time.Time `json:"stale_since"`
	} `json:"meta"`
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
	d.mu.Lock()
	defer d.mu.Unlock()

	seen := make(map[string]struct{}, len(events))
	for _, ev := range events {
		if ev.HazardID == "" || ev.HazardType == "" || ev.Severity == "" {
			continue
		}
		seen[ev.HazardID] = struct{}{}
		if existing, ok := d.events[ev.HazardID]; !ok || !shouldSkipUpdate(existing, ev) {
			d.events[ev.HazardID] = cloneHazardEvent(ev)
		}
	}

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
	}

	d.rebuildCountersLocked()

	now := time.Now().UTC()
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

func eventTimestamp(ev HazardEvent) time.Time {
	if !ev.OccurredAt.IsZero() {
		return ev.OccurredAt.UTC()
	}
	return ev.IngestedAt.UTC()
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
	var latestEventTime time.Time

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

		ts := eventTimestamp(ev)
		if ts.After(latestEventTime) {
			latestEventTime = ts
		}
		if since != nil && !ts.IsZero() && ts.Before(*since) {
			continue
		}

		total++
		byType[ev.HazardType]++
		bySeverity[ev.Severity]++
		if hasCorrelatedTsunami(ev) {
			tsunamiCount++
		}
		if ev.SourceRefID != "" {
			sourceRefs = append(sourceRefs, ev.SourceRefID)
		}
		matchingEvents = append(matchingEvents, cloneHazardEvent(ev))

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
			if hasCorrelatedTsunami(ev) {
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

	sort.Slice(matchingEvents, func(i, j int) bool {
		ti := eventTimestamp(matchingEvents[i])
		tj := eventTimestamp(matchingEvents[j])
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		return matchingEvents[i].HazardID < matchingEvents[j].HazardID
	})

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

func syncFromAggregator(ctx context.Context, baseURL string, client *http.Client, dashboard *Dashboard) error {
	if strings.TrimSpace(baseURL) == "" || dashboard == nil {
		return nil
	}
	if client == nil {
		client = &http.Client{Timeout: 4 * time.Second}
	}

	recordErr := func(err error) error {
		dashboard.RecordSyncFailure(err, time.Now().UTC())
		return err
	}

	syncStartedAt := time.Now().UTC()
	const pageSize = 500
	var allEvents []HazardEvent
	degraded := false
	partial := false
	var staleSince *time.Time
	unitSet := make(map[string]struct{})

	for offset := 0; ; {
		endpoint := fmt.Sprintf("%s/internal/hazards?limit=%d&offset=%d", strings.TrimRight(baseURL, "/"), pageSize, offset)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return recordErr(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return recordErr(err)
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return recordErr(readErr)
		}
		if resp.StatusCode != http.StatusOK {
			return recordErr(fmt.Errorf("aggregator returned HTTP %d", resp.StatusCode))
		}

		var page aggregatorListResponse
		if err := json.Unmarshal(body, &page); err != nil {
			return recordErr(fmt.Errorf("decode aggregator response: %w", err))
		}

		if page.Meta.Degraded {
			degraded = true
		}
		if staleSince == nil && page.Meta.StaleSince != nil {
			t := page.Meta.StaleSince.UTC()
			staleSince = &t
		}
		if strings.EqualFold(resp.Header.Get("X-Storage-Partial"), "true") {
			partial = true
		}
		if units := strings.TrimSpace(resp.Header.Get("X-Unavailable-Units")); units != "" {
			partial = true
			for _, u := range strings.Split(units, ",") {
				if trimmed := strings.TrimSpace(u); trimmed != "" {
					unitSet[trimmed] = struct{}{}
				}
			}
		}

		allEvents = append(allEvents, page.Data...)
		if len(page.Data) == 0 || offset+len(page.Data) >= page.Meta.Total {
			break
		}
		offset += len(page.Data)
	}

	var unavailableUnits []string
	for u := range unitSet {
		unavailableUnits = append(unavailableUnits, u)
	}
	sort.Strings(unavailableUnits)

	reason := ""
	if partial && len(unavailableUnits) > 0 {
		reason = "Partial storage outage: unavailable units " + strings.Join(unavailableUnits, ", ")
	} else if partial {
		reason = "Partial storage outage reported by Aggregator"
	} else if degraded {
		reason = "Upstream or storage degradation reported by Aggregator"
	}

	dashboard.ReconcileSnapshot(allEvents, syncStartedAt, degraded, partial, unavailableUnits, staleSince, reason)
	return nil
}

func startAggregatorSyncLoop(ctx context.Context, baseURL string, interval time.Duration, client *http.Client, dashboard *Dashboard) {
	if strings.TrimSpace(baseURL) == "" || dashboard == nil {
		return
	}
	if interval <= 0 {
		interval = defaultSyncInterval
	}
	go func() {
		if err := syncFromAggregator(ctx, baseURL, client, dashboard); err != nil && ctx.Err() == nil {
			log.Printf("[dashboard] initial aggregator sync failed: %v", err)
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := syncFromAggregator(ctx, baseURL, client, dashboard); err != nil && ctx.Err() == nil {
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
