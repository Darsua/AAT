package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"bnpb/aggregator/internal/config"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/repository"
)

const (
	defaultSpikeArea = "Jawa Barat - Cekungan Bandung"
)

type tectonicZone struct {
	name string
	lat  float64
	lon  float64
}

var indonesiaSeismicZones = []tectonicZone{
	{name: "Aceh - Subduksi Sumatra", lat: 4.6951, lon: 96.7494},
	{name: "Sumatera Barat - Mentawai", lat: -1.0242, lon: 99.6641},
	{name: "Selat Sunda - Banten", lat: -6.1214, lon: 105.4233},
	{name: "Jawa Barat - Garut Selatan", lat: -7.5231, lon: 107.7634},
	{name: "Jawa Tengah - Yogyakarta", lat: -7.9125, lon: 110.3695},
	{name: "Jawa Timur - Malang Selatan", lat: -8.3422, lon: 112.6326},
	{name: "Bali - Selat Lombok", lat: -8.5069, lon: 115.2625},
	{name: "Nusa Tenggara Barat - Lombok", lat: -8.3931, lon: 116.4572},
	{name: "Maluku - Laut Banda", lat: -4.2500, lon: 129.8833},
	{name: "Sulawesi Tengah - Palu Koro", lat: -0.9003, lon: 119.8779},
	{name: "Halmahera - Maluku Utara", lat: 1.1570, lon: 127.8087},
	{name: "Papua - Biak Utara", lat: -0.9167, lon: 136.0500},
}

// CatalogRecord wraps a HazardEvent with its loading phase ("baseline", "spike", or "companion").
type CatalogRecord struct {
	Phase string
	Event domain.HazardEvent
}

// NodeDistributionReport captures per-node record and write-load counts.
type NodeDistributionReport struct {
	Node                string  `json:"node"`
	PrimaryCount        int     `json:"primary_count"`
	ReplicaCount        int     `json:"replica_count"`
	PhysicalRowCount    int     `json:"physical_row_count"`
	UniqueEventCount    int     `json:"unique_event_count"`
	BaselineWrites      int     `json:"baseline_writes"`
	SpikeWrites         int     `json:"spike_writes"`
	PrimaryShare        float64 `json:"primary_share"`
	UniqueEventCoverage float64 `json:"unique_event_coverage"`
	PhysicalRowShare    float64 `json:"physical_row_share"`
}

// SourceChecksumSummary summarizes verified canonical counts and SHA-256 digests per source.
type SourceChecksumSummary struct {
	Source   string `json:"source"`
	Count    int    `json:"count"`
	Checksum string `json:"checksum"`
}

// BenchmarkReport summarizes historical catalog distribution, regional spike skew, and audit checksums.
type BenchmarkReport struct {
	Mode                     string                           `json:"mode"`
	ShardCount               int                              `json:"shard_count"`
	BaselineEvents           int                              `json:"baseline_events"`
	SpikeEvents              int                              `json:"spike_events"`
	CompanionEvents          int                              `json:"companion_events"`
	TotalEvents              int                              `json:"total_events"`
	MaxPrimaryNodeShare      float64                          `json:"max_primary_node_share"`
	MaxNodeUniqueCoverage    float64                          `json:"max_node_unique_coverage"`
	NoNodeHoldsAllRecords    bool                             `json:"no_node_holds_all_records"`
	NodeDistribution         []NodeDistributionReport         `json:"node_distribution"`
	PrimaryCountsByShard     map[string]int                   `json:"primary_counts_by_shard"`
	ReplicaCountsByShard     map[string]int                   `json:"replica_counts_by_shard"`
	FullPlacementComparison  repository.PlacementComparison   `json:"full_placement_comparison"`
	SpikePlacementComparison *repository.PlacementComparison  `json:"spike_placement_comparison,omitempty"`
	Checksums                map[string]SourceChecksumSummary `json:"checksums"`
}

// GenerateHistoricalCatalog builds a deterministic Indonesian historical earthquake dataset plus a regional seismic spike
// and companion multi-source hazards so all canonical sources are represented in Audit API checksums.
func GenerateHistoricalCatalog(baselineCount, spikeCount int, spikeArea string) []CatalogRecord {
	if strings.TrimSpace(spikeArea) == "" {
		spikeArea = defaultSpikeArea
	}
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	records := make([]CatalogRecord, 0, baselineCount+spikeCount+9)
	seq := 0

	for i := range baselineCount {
		seq++
		zone := indonesiaSeismicZones[i%len(indonesiaSeismicZones)]
		lat := zone.lat + float64((i%7)-3)*0.08
		lon := zone.lon + float64((i%5)-2)*0.09
		mag := 3.4 + float64(i%28)*0.15
		depthKm := 10.0 + float64((i*7)%140)
		sev := severityFromMagnitude(mag)
		ts := baseTime.Add(time.Duration(seq) * time.Minute)

		records = append(records, CatalogRecord{
			Phase: "baseline",
			Event: domain.HazardEvent{
				HazardID:    fmt.Sprintf("00000000-0000-4000-8000-%012d", seq),
				Source:      domain.SourceBMKG,
				SourceRefID: fmt.Sprintf("BMKG-HIST-2025-%05d", i+1),
				HazardType:  domain.HazardTypeSeismic,
				Severity:    sev,
				AreaName:    zone.name,
				Latitude:    &lat,
				Longitude:   &lon,
				OccurredAt:  ts,
				IngestedAt:  ts.Add(2 * time.Second),
				Attributes: map[string]any{
					"magnitude": mag,
					"depth_km":  depthKm,
					"phase":     "baseline",
				},
			},
		})
	}

	for i := range spikeCount {
		seq++
		lat := -7.02 + float64((i%11)-5)*0.015
		lon := 107.61 + float64((i%9)-4)*0.015
		mag := 3.8 + float64(i%24)*0.12
		depthKm := 6.0 + float64((i*3)%25)
		sev := severityFromMagnitude(mag)
		ts := baseTime.Add(30*24*time.Hour + time.Duration(i+1)*15*time.Second)

		records = append(records, CatalogRecord{
			Phase: "spike",
			Event: domain.HazardEvent{
				HazardID:    fmt.Sprintf("00000000-0000-4000-8000-%012d", seq),
				Source:      domain.SourceBMKG,
				SourceRefID: fmt.Sprintf("BMKG-SPIKE-JABAR-%05d", i+1),
				HazardType:  domain.HazardTypeSeismic,
				Severity:    sev,
				AreaName:    spikeArea,
				Latitude:    &lat,
				Longitude:   &lon,
				OccurredAt:  ts,
				IngestedAt:  ts.Add(2 * time.Second),
				Attributes: map[string]any{
					"magnitude": mag,
					"depth_km":  depthKm,
					"phase":     "spike",
				},
			},
		})
	}

	type companionSpec struct {
		source     domain.Source
		hazardType domain.HazardType
		severity   domain.Severity
		area       string
		prefix     string
		lat        float64
		lon        float64
	}
	companions := []companionSpec{
		{source: domain.SourcePVMBG, hazardType: domain.HazardTypeVolcanic, severity: domain.SeveritySiaga, area: "Gunung Merapi", prefix: "PVMBG-HIST", lat: -7.5407, lon: 110.4456},
		{source: domain.SourcePVMBG, hazardType: domain.HazardTypeVolcanic, severity: domain.SeverityWaspada, area: "Gunung Semeru", prefix: "PVMBG-HIST", lat: -8.1077, lon: 112.9224},
		{source: domain.SourcePVMBG, hazardType: domain.HazardTypeVolcanic, severity: domain.SeverityAwas, area: "Gunung Ruang", prefix: "PVMBG-HIST", lat: 2.3014, lon: 125.3697},
		{source: domain.SourceSatelit, hazardType: domain.HazardTypeWildfire, severity: domain.SeveritySiaga, area: "Kalimantan Tengah", prefix: "SATELIT-HIST", lat: -2.5123, lon: 113.8044},
		{source: domain.SourceSatelit, hazardType: domain.HazardTypeWildfire, severity: domain.SeverityWaspada, area: "Riau - Pelalawan", prefix: "SATELIT-HIST", lat: 0.2145, lon: 101.8562},
		{source: domain.SourceSatelit, hazardType: domain.HazardTypeWildfire, severity: domain.SeveritySiaga, area: "Sumatera Selatan", prefix: "SATELIT-HIST", lat: -3.2841, lon: 104.7621},
		{source: domain.SourceSensorPantai, hazardType: domain.HazardTypeCoastal, severity: domain.SeverityWaspada, area: "Pantai Pangandaran", prefix: "PANTAI-HIST", lat: -7.7012, lon: 108.6531},
		{source: domain.SourceSensorPantai, hazardType: domain.HazardTypeCoastal, severity: domain.SeveritySiaga, area: "Teluk Palu", prefix: "PANTAI-HIST", lat: -0.8211, lon: 119.8654},
		{source: domain.SourceSensorPantai, hazardType: domain.HazardTypeCoastal, severity: domain.SeverityNormal, area: "Selat Sunda - Carita", prefix: "PANTAI-HIST", lat: -6.2981, lon: 105.8412},
	}
	for i, c := range companions {
		seq++
		lat := c.lat
		lon := c.lon
		ts := baseTime.Add(60*24*time.Hour + time.Duration(i+1)*time.Minute)
		records = append(records, CatalogRecord{
			Phase: "companion",
			Event: domain.HazardEvent{
				HazardID:    fmt.Sprintf("00000000-0000-4000-8000-%012d", seq),
				Source:      c.source,
				SourceRefID: fmt.Sprintf("%s-2025-%05d", c.prefix, i+1),
				HazardType:  c.hazardType,
				Severity:    c.severity,
				AreaName:    c.area,
				Latitude:    &lat,
				Longitude:   &lon,
				OccurredAt:  ts,
				IngestedAt:  ts.Add(2 * time.Second),
				Attributes: map[string]any{
					"phase": "companion",
				},
			},
		})
	}

	return records
}

func severityFromMagnitude(mag float64) domain.Severity {
	switch {
	case mag >= 6.5:
		return domain.SeverityAwas
	case mag >= 5.5:
		return domain.SeveritySiaga
	case mag >= 4.5:
		return domain.SeverityWaspada
	default:
		return domain.SeverityNormal
	}
}

// BuildInMemoryShardedRepo constructs a deterministic 3-node (aqua, ruby, kana), 3-shard repository with RF=2.
func BuildInMemoryShardedRepo() (*repository.HazardRepository, error) {
	groups := []repository.ShardGroup{
		{
			ShardID: "shard-1",
			Units: []repository.StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: repository.NewMemoryUnitBackend()},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: repository.NewMemoryUnitBackend()},
			},
		},
		{
			ShardID: "shard-2",
			Units: []repository.StorageUnit{
				{Unit: "shard-2", Node: "ruby", Role: domain.StorageRolePrimary, Backend: repository.NewMemoryUnitBackend()},
				{Unit: "shard-2-r1", Node: "kana", Role: domain.StorageRoleReplica, Backend: repository.NewMemoryUnitBackend()},
			},
		},
		{
			ShardID: "shard-3",
			Units: []repository.StorageUnit{
				{Unit: "shard-3", Node: "kana", Role: domain.StorageRolePrimary, Backend: repository.NewMemoryUnitBackend()},
				{Unit: "shard-3-r1", Node: "aqua", Role: domain.StorageRoleReplica, Backend: repository.NewMemoryUnitBackend()},
			},
		},
	}
	return repository.NewShardedHazardRepository(groups)
}

// LoadCatalogFromFile reads canonical or array-formatted catalog JSON files.
func LoadCatalogFromFile(path string) ([]CatalogRecord, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read catalog %q: %w", path, err)
	}

	var events []domain.HazardEvent
	if err := json.Unmarshal(raw, &events); err != nil {
		var wrapper struct {
			Events []domain.HazardEvent `json:"events"`
			Items  []domain.HazardEvent `json:"items"`
		}
		if wErr := json.Unmarshal(raw, &wrapper); wErr != nil {
			return nil, fmt.Errorf("decode catalog %q as array or {events|items} object: %w", path, err)
		}
		if len(wrapper.Events) > 0 {
			events = wrapper.Events
		} else {
			events = wrapper.Items
		}
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("catalog %q contains zero events", path)
	}

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	records := make([]CatalogRecord, 0, len(events))
	for i, ev := range events {
		if strings.TrimSpace(ev.HazardID) == "" {
			ev.HazardID = fmt.Sprintf("catalog-%06d-%s", i+1, strings.TrimSpace(ev.SourceRefID))
		}
		if strings.TrimSpace(ev.SourceRefID) == "" {
			ev.SourceRefID = ev.HazardID
		}
		if ev.Source == "" {
			ev.Source = domain.SourceBMKG
		}
		if ev.HazardType == "" {
			ev.HazardType = domain.HazardTypeSeismic
		}
		if ev.Severity == "" {
			ev.Severity = domain.SeverityNormal
		}
		if ev.OccurredAt.IsZero() {
			ev.OccurredAt = baseTime.Add(time.Duration(i+1) * time.Second)
		}
		if ev.IngestedAt.IsZero() {
			ev.IngestedAt = ev.OccurredAt.Add(time.Second)
		}
		if ev.Attributes == nil {
			ev.Attributes = map[string]any{}
		}
		phase := "baseline"
		if rawPhase, ok := ev.Attributes["phase"].(string); ok && strings.TrimSpace(rawPhase) != "" {
			phase = rawPhase
		}
		records = append(records, CatalogRecord{Phase: phase, Event: ev})
	}
	return records, nil
}

// RunCatalogBenchmark persists all records via HazardRepository, measures per-node & per-shard placement,
// compares consistent hashing against naive region/source placement, and computes Audit API checksums.
func RunCatalogBenchmark(
	ctx context.Context,
	repo *repository.HazardRepository,
	records []CatalogRecord,
	mode string,
	shardCount int,
) (BenchmarkReport, []domain.HazardEvent, error) {
	if len(records) == 0 {
		return BenchmarkReport{}, nil, errors.New("at least one catalog record is required")
	}
	if shardCount <= 0 {
		shardCount = 3
	}

	persisted := make([]domain.HazardEvent, 0, len(records))
	spikeEvents := make([]domain.HazardEvent, 0)
	primaryByNode := make(map[string]int)
	replicaByNode := make(map[string]int)
	uniqueByNode := make(map[string]map[string]struct{})
	baselineWritesByNode := make(map[string]int)
	spikeWritesByNode := make(map[string]int)
	primaryByShard := make(map[string]int)
	replicaByShard := make(map[string]int)

	baselineCount := 0
	spikeCount := 0
	companionCount := 0

	for i := range records {
		rec := records[i]
		ev := rec.Event
		if ev.Attributes != nil {
			ev.Attributes = maps.Clone(ev.Attributes)
		}
		if err := repo.SaveHazard(ctx, &ev); err != nil {
			return BenchmarkReport{}, nil, fmt.Errorf("save hazard %q: %w", ev.HazardID, err)
		}
		persisted = append(persisted, ev)

		switch rec.Phase {
		case "spike":
			spikeCount++
			spikeEvents = append(spikeEvents, ev)
		case "companion":
			companionCount++
		default:
			baselineCount++
		}

		loc, err := repo.LocateHazard(ctx, ev.HazardID)
		if err != nil {
			return BenchmarkReport{}, nil, fmt.Errorf("locate hazard %q: %w", ev.HazardID, err)
		}
		for _, unitLoc := range loc.Locations {
			if _, ok := uniqueByNode[unitLoc.Node]; !ok {
				uniqueByNode[unitLoc.Node] = make(map[string]struct{})
			}
			uniqueByNode[unitLoc.Node][ev.HazardID] = struct{}{}

			switch unitLoc.Role {
			case domain.StorageRolePrimary:
				primaryByNode[unitLoc.Node]++
				primaryByShard[unitLoc.Unit]++
				if rec.Phase == "spike" {
					spikeWritesByNode[unitLoc.Node]++
				} else {
					baselineWritesByNode[unitLoc.Node]++
				}
			case domain.StorageRoleReplica:
				replicaByNode[unitLoc.Node]++
				replicaByShard[unitLoc.Unit]++
				if rec.Phase == "spike" {
					spikeWritesByNode[unitLoc.Node]++
				} else {
					baselineWritesByNode[unitLoc.Node]++
				}
			}
		}
	}

	nodes := make([]string, 0, len(uniqueByNode))
	for n := range uniqueByNode {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	totalEvents := len(persisted)
	totalPhysicalRows := 0
	for _, n := range nodes {
		totalPhysicalRows += primaryByNode[n] + replicaByNode[n]
	}

	nodeReports := make([]NodeDistributionReport, 0, len(nodes))
	maxPrimaryCount := 0
	maxUniqueCount := 0
	maxPrimaryShare := 0.0
	maxUniqueCoverage := 0.0

	for _, n := range nodes {
		pCount := primaryByNode[n]
		rCount := replicaByNode[n]
		phys := pCount + rCount
		uniq := len(uniqueByNode[n])
		if pCount > maxPrimaryCount {
			maxPrimaryCount = pCount
		}
		if uniq > maxUniqueCount {
			maxUniqueCount = uniq
		}
		pShare := float64(pCount) / float64(totalEvents)
		uCov := float64(uniq) / float64(totalEvents)
		physShare := 0.0
		if totalPhysicalRows > 0 {
			physShare = float64(phys) / float64(totalPhysicalRows)
		}
		if pShare > maxPrimaryShare {
			maxPrimaryShare = pShare
		}
		if uCov > maxUniqueCoverage {
			maxUniqueCoverage = uCov
		}
		nodeReports = append(nodeReports, NodeDistributionReport{
			Node:                n,
			PrimaryCount:        pCount,
			ReplicaCount:        rCount,
			PhysicalRowCount:    phys,
			UniqueEventCount:    uniq,
			BaselineWrites:      baselineWritesByNode[n],
			SpikeWrites:         spikeWritesByNode[n],
			PrimaryShare:        pShare,
			UniqueEventCoverage: uCov,
			PhysicalRowShare:    physShare,
		})
	}

	noNodeHoldsAll := len(nodes) >= 2 && maxPrimaryCount < totalEvents && maxUniqueCount < totalEvents

	fullComp := repository.ComparePlacementStrategies(persisted, shardCount)
	var spikeComp *repository.PlacementComparison
	if len(spikeEvents) > 0 {
		sc := repository.ComparePlacementStrategies(spikeEvents, shardCount)
		spikeComp = &sc
	}

	checksums := make(map[string]SourceChecksumSummary)
	allRes, err := repo.ComputeAuditChecksum(ctx, nil, "ALL")
	if err != nil {
		return BenchmarkReport{}, nil, fmt.Errorf("compute ALL checksum: %w", err)
	}
	checksums["ALL"] = SourceChecksumSummary{
		Source:   allRes.Source,
		Count:    allRes.Count,
		Checksum: allRes.Checksum,
	}
	for _, src := range []domain.Source{
		domain.SourceBMKG,
		domain.SourcePVMBG,
		domain.SourceSatelit,
		domain.SourceSensorPantai,
	} {
		s := src
		res, err := repo.ComputeAuditChecksum(ctx, &s, "ALL")
		if err != nil {
			return BenchmarkReport{}, nil, fmt.Errorf("compute %s checksum: %w", src, err)
		}
		checksums[string(src)] = SourceChecksumSummary{
			Source:   res.Source,
			Count:    res.Count,
			Checksum: res.Checksum,
		}
	}

	sort.Slice(persisted, func(i, j int) bool {
		if !persisted[i].IngestedAt.Equal(persisted[j].IngestedAt) {
			return persisted[i].IngestedAt.Before(persisted[j].IngestedAt)
		}
		return persisted[i].HazardID < persisted[j].HazardID
	})

	return BenchmarkReport{
		Mode:                     mode,
		ShardCount:               shardCount,
		BaselineEvents:           baselineCount,
		SpikeEvents:              spikeCount,
		CompanionEvents:          companionCount,
		TotalEvents:              totalEvents,
		MaxPrimaryNodeShare:      maxPrimaryShare,
		MaxNodeUniqueCoverage:    maxUniqueCoverage,
		NoNodeHoldsAllRecords:    noNodeHoldsAll,
		NodeDistribution:         nodeReports,
		PrimaryCountsByShard:     primaryByShard,
		ReplicaCountsByShard:     replicaByShard,
		FullPlacementComparison:  fullComp,
		SpikePlacementComparison: spikeComp,
		Checksums:                checksums,
	}, persisted, nil
}

func writeExportCatalog(path string, events []domain.HazardEvent) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload := struct {
		Events []domain.HazardEvent `json:"events"`
	}{Events: events}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func renderTextReport(w io.Writer, r BenchmarkReport) {
	fmt.Fprintf(w, "Mode: %s | Shards: %d | Events: %d (baseline=%d, spike=%d, companion=%d)\n",
		r.Mode, r.ShardCount, r.TotalEvents, r.BaselineEvents, r.SpikeEvents, r.CompanionEvents)
	fmt.Fprintf(w, "No Node Holds All Records: %v (max_primary_share=%.4f, max_unique_coverage=%.4f)\n\n",
		r.NoNodeHoldsAllRecords, r.MaxPrimaryNodeShare, r.MaxNodeUniqueCoverage)

	fmt.Fprintln(w, "Per-Node Distribution:")
	for _, n := range r.NodeDistribution {
		fmt.Fprintf(w, "  - %-6s primary=%-4d replica=%-4d unique=%-4d baseline_writes=%-4d spike_writes=%-4d primary_share=%.3f coverage=%.3f\n",
			n.Node, n.PrimaryCount, n.ReplicaCount, n.UniqueEventCount, n.BaselineWrites, n.SpikeWrites, n.PrimaryShare, n.UniqueEventCoverage)
	}

	fmt.Fprintln(w, "\nPlacement Comparison (Full Catalog):")
	fmt.Fprintf(w, "  - %-28s min=%-4d max=%-4d skew=%.3f\n",
		r.FullPlacementComparison.ConsistentHashByHazardID.Name,
		r.FullPlacementComparison.ConsistentHashByHazardID.MinCount,
		r.FullPlacementComparison.ConsistentHashByHazardID.MaxCount,
		r.FullPlacementComparison.ConsistentHashByHazardID.SkewRatio)
	fmt.Fprintf(w, "  - %-28s min=%-4d max=%-4d skew=%.3f\n",
		r.FullPlacementComparison.RegionHashByAreaName.Name,
		r.FullPlacementComparison.RegionHashByAreaName.MinCount,
		r.FullPlacementComparison.RegionHashByAreaName.MaxCount,
		r.FullPlacementComparison.RegionHashByAreaName.SkewRatio)
	fmt.Fprintf(w, "  - %-28s min=%-4d max=%-4d skew=%.3f\n",
		r.FullPlacementComparison.AgencyPartitionBySource.Name,
		r.FullPlacementComparison.AgencyPartitionBySource.MinCount,
		r.FullPlacementComparison.AgencyPartitionBySource.MaxCount,
		r.FullPlacementComparison.AgencyPartitionBySource.SkewRatio)

	if r.SpikePlacementComparison != nil {
		fmt.Fprintln(w, "\nPlacement Comparison (Regional Seismic Spike):")
		fmt.Fprintf(w, "  - %-28s min=%-4d max=%-4d skew=%.3f\n",
			r.SpikePlacementComparison.ConsistentHashByHazardID.Name,
			r.SpikePlacementComparison.ConsistentHashByHazardID.MinCount,
			r.SpikePlacementComparison.ConsistentHashByHazardID.MaxCount,
			r.SpikePlacementComparison.ConsistentHashByHazardID.SkewRatio)
		fmt.Fprintf(w, "  - %-28s min=%-4d max=%-4d skew=%.3f\n",
			r.SpikePlacementComparison.RegionHashByAreaName.Name,
			r.SpikePlacementComparison.RegionHashByAreaName.MinCount,
			r.SpikePlacementComparison.RegionHashByAreaName.MaxCount,
			r.SpikePlacementComparison.RegionHashByAreaName.SkewRatio)
	}

	fmt.Fprintln(w, "\nAudit Checksums:")
	keys := make([]string, 0, len(r.Checksums))
	for k := range r.Checksums {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		c := r.Checksums[k]
		fmt.Fprintf(w, "  - %-14s count=%-4d sha256=%s\n", c.Source, c.Count, c.Checksum)
	}
}

func main() {
	catalogPath := flag.String("catalog", "", "Optional path to canonical hazard catalog JSON file")
	baselineCount := flag.Int("baseline-count", 500, "Historical multi-region seismic events when --catalog is omitted")
	spikeCount := flag.Int("spike-count", 200, "Regional spike seismic events when --catalog is omitted")
	spikeArea := flag.String("spike-area", defaultSpikeArea, "Area name for simulated regional seismic spike")
	inMemory := flag.Bool("in-memory", false, "Run against an in-memory 3-node (aqua/ruby/kana) sharded repository")
	shardConfigOverride := flag.String("shard-config", "", "Override SHARD_CONFIG topology string")
	exportCatalogPath := flag.String("export-catalog", "", "Optional path to write canonical catalog JSON")
	jsonOutput := flag.Bool("json", false, "Emit machine-readable JSON report to stdout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var (
		records []CatalogRecord
		err     error
	)
	if strings.TrimSpace(*catalogPath) != "" {
		records, err = LoadCatalogFromFile(*catalogPath)
	} else {
		records = GenerateHistoricalCatalog(*baselineCount, *spikeCount, *spikeArea)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "catalog error: %v\n", err)
		os.Exit(1)
	}

	var (
		repo       *repository.HazardRepository
		mode       string
		shardCount int
	)

	if *inMemory {
		repo, err = BuildInMemoryShardedRepo()
		mode = "in-memory"
		shardCount = 3
	} else {
		cfg := config.Load()
		shardConfig := cfg.ShardConfig
		if strings.TrimSpace(*shardConfigOverride) != "" {
			shardConfig = *shardConfigOverride
		}
		specs, pErr := repository.ParseShardTopology(
			cfg.ShardTopologyJSON,
			shardConfig,
			cfg.DatabaseURL,
			cfg.NodeName,
			cfg.StorageUnitID,
		)
		if pErr != nil {
			fmt.Fprintf(os.Stderr, "topology error: %v\n", pErr)
			os.Exit(1)
		}
		log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
		var dialErr error
		repo, _, dialErr = repository.DialShardedRepository(ctx, specs, cfg.DatabaseMaxConns, cfg.DatabaseMinConns, log)
		if dialErr != nil {
			fmt.Fprintf(os.Stderr, "postgres dial error: %v\n", dialErr)
			os.Exit(1)
		}
		defer repo.Close()
		mode = "postgres"
		shardCount = len(specs)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "repository init error: %v\n", err)
		os.Exit(1)
	}

	report, persisted, err := RunCatalogBenchmark(ctx, repo, records, mode, shardCount)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}

	if strings.TrimSpace(*exportCatalogPath) != "" {
		if err := writeExportCatalog(*exportCatalogPath, persisted); err != nil {
			fmt.Fprintf(os.Stderr, "export catalog failed: %v\n", err)
			os.Exit(1)
		}
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fmt.Fprintf(os.Stderr, "json encode error: %v\n", err)
			os.Exit(1)
		}
		return
	}
	renderTextReport(os.Stdout, report)
}
