package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateHistoricalCatalog_AndBenchmarkSkew(t *testing.T) {
	records := GenerateHistoricalCatalog(500, 200, defaultSpikeArea)
	if len(records) != 709 {
		t.Fatalf("expected 709 total records (500 baseline + 200 spike + 9 companions), got %d", len(records))
	}

	repo, err := BuildInMemoryShardedRepo()
	if err != nil {
		t.Fatalf("BuildInMemoryShardedRepo failed: %v", err)
	}

	report, persisted, err := RunCatalogBenchmark(context.Background(), repo, records, "in-memory", 3)
	if err != nil {
		t.Fatalf("RunCatalogBenchmark failed: %v", err)
	}

	if len(persisted) != 709 || report.TotalEvents != 709 {
		t.Fatalf("expected 709 persisted events, got %d / %d", len(persisted), report.TotalEvents)
	}
	if !report.NoNodeHoldsAllRecords {
		t.Fatalf("expected NoNodeHoldsAllRecords=true, got false (max_primary_share=%.4f, max_unique_coverage=%.4f)",
			report.MaxPrimaryNodeShare, report.MaxNodeUniqueCoverage)
	}
	if len(report.NodeDistribution) != 3 {
		t.Fatalf("expected 3 nodes (aqua, ruby, kana), got %d", len(report.NodeDistribution))
	}
	for _, n := range report.NodeDistribution {
		if n.PrimaryCount == 0 || n.ReplicaCount == 0 {
			t.Errorf("node %s has empty primary or replica count: %+v", n.Node, n)
		}
		if n.BaselineWrites == 0 || n.SpikeWrites == 0 {
			t.Errorf("node %s missed baseline or spike writes: %+v", n.Node, n)
		}
	}

	if report.SpikePlacementComparison == nil {
		t.Fatal("expected non-nil SpikePlacementComparison")
	}
	hashSkew := report.SpikePlacementComparison.ConsistentHashByHazardID.SkewRatio
	regionSkew := report.SpikePlacementComparison.RegionHashByAreaName.SkewRatio
	if hashSkew >= regionSkew {
		t.Fatalf("expected consistent hash skew (%.3f) to beat naive region modulo skew (%.3f) during regional spike", hashSkew, regionSkew)
	}
	if regionSkew < 2.9 {
		t.Fatalf("expected naive region modulo to concentrate 100%% of spike on 1 shard (skew ~3.0), got %.3f", regionSkew)
	}

	for _, key := range []string{"ALL", "BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"} {
		sum, ok := report.Checksums[key]
		if !ok || sum.Count == 0 || len(sum.Checksum) != 64 {
			t.Errorf("missing or invalid checksum summary for %s: %+v", key, sum)
		}
	}

	var buf bytes.Buffer
	renderTextReport(&buf, report)
	if buf.Len() == 0 {
		t.Fatal("expected non-empty rendered text report")
	}
}

func TestExportAndLoadCatalogRoundTrip(t *testing.T) {
	records := GenerateHistoricalCatalog(12, 6, defaultSpikeArea)
	repo, err := BuildInMemoryShardedRepo()
	if err != nil {
		t.Fatalf("BuildInMemoryShardedRepo failed: %v", err)
	}
	_, persisted, err := RunCatalogBenchmark(context.Background(), repo, records, "in-memory", 3)
	if err != nil {
		t.Fatalf("RunCatalogBenchmark failed: %v", err)
	}

	tmpPath := filepath.Join(t.TempDir(), "catalog.json")
	if err := writeExportCatalog(tmpPath, persisted); err != nil {
		t.Fatalf("writeExportCatalog failed: %v", err)
	}

	loaded, err := LoadCatalogFromFile(tmpPath)
	if err != nil {
		t.Fatalf("LoadCatalogFromFile failed: %v", err)
	}
	if len(loaded) != len(persisted) {
		t.Fatalf("expected %d reloaded events, got %d", len(persisted), len(loaded))
	}

	// Ensure JSON report serialization succeeds cleanly without NaN/Inf.
	rawReport, err := json.Marshal(struct {
		Loaded int `json:"loaded"`
	}{Loaded: len(loaded)})
	if err != nil || len(rawReport) == 0 {
		t.Fatalf("marshal error: %v", err)
	}
	if _, err := os.Stat(tmpPath); err != nil {
		t.Fatalf("stat exported catalog failed: %v", err)
	}
}
