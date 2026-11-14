package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
)

func buildThreeNodeShardedRepo(t *testing.T) (*HazardRepository, map[string]*MemoryUnitBackend, map[string][]string) {
	t.Helper()

	backends := map[string]*MemoryUnitBackend{
		"shard-1":    NewMemoryUnitBackend(),
		"shard-1-r1": NewMemoryUnitBackend(),
		"shard-2":    NewMemoryUnitBackend(),
		"shard-2-r1": NewMemoryUnitBackend(),
		"shard-3":    NewMemoryUnitBackend(),
		"shard-3-r1": NewMemoryUnitBackend(),
	}

	nodeUnits := map[string][]string{
		"aqua": {"shard-1", "shard-3-r1"},
		"ruby": {"shard-2", "shard-1-r1"},
		"kana": {"shard-3", "shard-2-r1"},
	}

	groups := []ShardGroup{
		{
			ShardID: "shard-1",
			Units: []StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: backends["shard-1"]},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: backends["shard-1-r1"]},
			},
		},
		{
			ShardID: "shard-2",
			Units: []StorageUnit{
				{Unit: "shard-2", Node: "ruby", Role: domain.StorageRolePrimary, Backend: backends["shard-2"]},
				{Unit: "shard-2-r1", Node: "kana", Role: domain.StorageRoleReplica, Backend: backends["shard-2-r1"]},
			},
		},
		{
			ShardID: "shard-3",
			Units: []StorageUnit{
				{Unit: "shard-3", Node: "kana", Role: domain.StorageRolePrimary, Backend: backends["shard-3"]},
				{Unit: "shard-3-r1", Node: "aqua", Role: domain.StorageRoleReplica, Backend: backends["shard-3-r1"]},
			},
		},
	}

	repo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("failed to build sharded repo: %v", err)
	}
	return repo, backends, nodeUnits
}

func makeSampleHazard(id, sourceRef, area string, occurred, ingested time.Time) domain.HazardEvent {
	lat := -7.5
	lon := 110.4
	return domain.HazardEvent{
		HazardID:    id,
		Source:      domain.SourceBMKG,
		SourceRefID: sourceRef,
		HazardType:  domain.HazardTypeSeismic,
		Severity:    domain.SeverityWaspada,
		AreaName:    area,
		Latitude:    &lat,
		Longitude:   &lon,
		Attributes: map[string]any{
			"magnitude": 5.4,
			"depth_km":  12.0,
		},
		OccurredAt: occurred.UTC(),
		IngestedAt: ingested.UTC(),
	}
}

func TestParseShardTopology_AllModes(t *testing.T) {
	// 1. Single DSN fallback
	fallbackSpecs, err := ParseShardTopology("", "", "postgres://user:pass@db:5432/bnpb", "aqua", "shard-1")
	if err != nil {
		t.Fatalf("fallback topology failed: %v", err)
	}
	if len(fallbackSpecs) != 1 || fallbackSpecs[0].Units[0].Node != "aqua" {
		t.Fatalf("unexpected fallback specs: %+v", fallbackSpecs)
	}

	// 2. SHARD_CONFIG DSL
	dsl := "shard-1=shard-1|aqua|primary|postgres://p1,shard-1-r1|ruby|replica|postgres://r1; shard-2=shard-2|ruby|primary|postgres://p2,shard-2-r1|kana|replica|postgres://r2"
	dslSpecs, err := ParseShardTopology("", dsl, "", "", "")
	if err != nil {
		t.Fatalf("SHARD_CONFIG parse failed: %v", err)
	}
	if len(dslSpecs) != 2 || len(dslSpecs[0].Units) != 2 || dslSpecs[1].Units[1].Role != domain.StorageRoleReplica {
		t.Fatalf("unexpected DSL specs: %+v", dslSpecs)
	}

	// 3. SHARD_TOPOLOGY_JSON
	jsonCfg := `[{"shard_id":"s1","units":[{"unit":"u1","node":"aqua","role":"primary","dsn":"postgres://a"}]}]`
	jsonSpecs, err := ParseShardTopology(jsonCfg, "", "", "", "")
	if err != nil {
		t.Fatalf("JSON topology parse failed: %v", err)
	}
	if len(jsonSpecs) != 1 || jsonSpecs[0].ShardID != "s1" {
		t.Fatalf("unexpected JSON specs: %+v", jsonSpecs)
	}

	// 4. Empty config returns error
	if _, err := ParseShardTopology("", "", "", "", ""); err == nil {
		t.Fatal("expected error for completely empty topology")
	}
}

func TestShardedRepository_DistributionAndNoNodeHoldsAll(t *testing.T) {
	repo, backends, nodeUnits := buildThreeNodeShardedRepo(t)
	ctx := context.Background()

	const totalEvents = 300
	baseTime := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

	for i := range totalEvents {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		h := makeSampleHazard(
			fmt.Sprintf("haz-bmkg-%04d", i),
			fmt.Sprintf("bmkg-ref-%04d", i),
			"Pesisir Selatan Jawa",
			ts,
			ts.Add(2*time.Second),
		)
		if err := repo.SaveHazardWithOutbox(ctx, &h, "corr-dist"); err != nil {
			t.Fatalf("save hazard %d failed: %v", i, err)
		}
	}

	// Verify no single node holds 100% of canonical dataset (3 shards, R=2 -> ~66.7% each)
	for node, units := range nodeUnits {
		nodeTotal := 0
		for _, u := range units {
			nodeTotal += backends[u].Count()
		}
		if nodeTotal == 0 {
			t.Errorf("node %s stored 0 records; expected distributed slice", node)
		}
		if nodeTotal >= totalEvents {
			t.Errorf("node %s holds %d/%d records (>=100%% violation)", node, nodeTotal, totalEvents)
		}
	}

	// Verify global query deduplicates replicas
	page, total, err := repo.QueryHazards(ctx, HazardFilter{Limit: 50, Offset: 0})
	if err != nil {
		t.Fatalf("QueryHazards failed: %v", err)
	}
	if total != totalEvents || len(page) != 50 {
		t.Fatalf("expected total=%d len(page)=50, got total=%d len=%d", totalEvents, total, len(page))
	}
}

func TestComparePlacementStrategies_RegionalSpikeSkew(t *testing.T) {
	const total = 500
	events := make([]domain.HazardEvent, total)
	ts := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

	for i := range total {
		events[i] = makeSampleHazard(
			fmt.Sprintf("haz-spike-%04d", i),
			fmt.Sprintf("spike-ref-%04d", i),
			"Selat Sunda", // Identical hotspot region & agency (BMKG)
			ts,
			ts,
		)
	}

	cmp := ComparePlacementStrategies(events, 3)
	if cmp.ConsistentHashByHazardID.SkewRatio >= 1.35 {
		t.Errorf("expected consistent hash skew < 1.35, got %.2f (%v)", cmp.ConsistentHashByHazardID.SkewRatio, cmp.ConsistentHashByHazardID.CountsByShard)
	}
	if cmp.RegionHashByAreaName.MaxCount != total || cmp.RegionHashByAreaName.SkewRatio != 3.0 {
		t.Errorf("expected region partitioning to hotspot all %d events (skew=3.0), got max=%d skew=%.2f",
			total, cmp.RegionHashByAreaName.MaxCount, cmp.RegionHashByAreaName.SkewRatio)
	}
	if cmp.AgencyPartitionBySource.MaxCount != total {
		t.Errorf("expected agency partitioning to hotspot all %d BMKG events, got max=%d", total, cmp.AgencyPartitionBySource.MaxCount)
	}
}

func TestShardedRepository_ScatterGatherTieOrdering(t *testing.T) {
	repo, _, _ := buildThreeNodeShardedRepo(t)
	ctx := context.Background()

	sameOccurred := time.Date(2026, 11, 14, 15, 0, 0, 0, time.UTC)
	ids := []string{"haz-tie-c", "haz-tie-a", "haz-tie-d", "haz-tie-b"}
	for idx, id := range ids {
		h := makeSampleHazard(id, "ref-"+id, "Maluku", sameOccurred, sameOccurred.Add(time.Duration(idx)*time.Millisecond))
		if err := repo.SaveHazardWithOutbox(ctx, &h, "corr-tie"); err != nil {
			t.Fatalf("save tie hazard %s failed: %v", id, err)
		}
	}

	got, total, err := repo.QueryHazards(ctx, HazardFilter{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("QueryHazards tie test failed: %v", err)
	}
	if total != 4 {
		t.Fatalf("expected 4 unique hazards, got %d", total)
	}

	expectedOrder := []string{"haz-tie-a", "haz-tie-b", "haz-tie-c", "haz-tie-d"}
	for i, exp := range expectedOrder {
		if got[i].HazardID != exp {
			t.Errorf("index %d: expected %s, got %s", i, exp, got[i].HazardID)
		}
	}
}

func TestShardedRepository_WorkerWipeSurvivalRecoveryRebalanceAndSecondLoss(t *testing.T) {
	repo, backends, _ := buildThreeNodeShardedRepo(t)
	ctx := context.Background()

	const count = 120
	baseTime := time.Date(2026, 11, 14, 8, 0, 0, 0, time.UTC)
	for i := range count {
		ts := baseTime.Add(time.Duration(i) * time.Second)
		h := makeSampleHazard(
			fmt.Sprintf("haz-surv-%03d", i),
			fmt.Sprintf("surv-ref-%03d", i),
			"Sumatera Barat",
			ts,
			ts.Add(time.Second),
		)
		if err := repo.SaveHazardWithOutbox(ctx, &h, "corr-surv"); err != nil {
			t.Fatalf("save hazard %d failed: %v", i, err)
		}
	}

	baselineChecksum, err := repo.ComputeAuditChecksum(ctx, nil, "ALL")
	if err != nil {
		t.Fatalf("baseline checksum failed: %v", err)
	}
	if baselineChecksum.Partial || baselineChecksum.Count != count {
		t.Fatalf("unexpected baseline checksum result: %+v", baselineChecksum)
	}

	// Seed a DLQ message routed to shard-2 (ruby primary + kana replica) so both wipe stages exercise DLQ survival.
	var shard2MsgID string
	for idx := 0; idx < 1000; idx++ {
		cand := fmt.Sprintf("dlq-surv-%04d", idx)
		if repo.LocateShard(cand) == "shard-2" {
			shard2MsgID = cand
			break
		}
	}
	if shard2MsgID == "" {
		t.Fatal("failed to find DLQ message ID routed to shard-2")
	}
	if err := repo.SaveUnprocessedMessage(ctx, domain.UnprocessedMessage{
		MessageID:  shard2MsgID,
		Source:     domain.SourcePVMBG,
		ReceivedAt: baseTime,
		Reason:     "schema_incompatible: level changed",
		Retryable:  false,
		Attempts:   3,
		Status:     domain.UnprocessedStatusParked,
		RawPayload: []byte(`{"report_id":"r-dlq"}`),
	}); err != nil {
		t.Fatalf("seed dlq message failed: %v", err)
	}

	// 1. Simulate catastrophic worker node (ruby) termination + local disk wipe!
	// ruby hosts shard-2 (primary) and shard-1-r1 (replica).
	backends["shard-2"].SetHealthy(false)
	backends["shard-2"].Wipe()
	backends["shard-1-r1"].SetHealthy(false)
	backends["shard-1-r1"].Wipe()

	// Checksum must still match 100% with 0 lost records, reporting partial=true & unavailable_units.
	degradedChecksum, err := repo.ComputeAuditChecksum(ctx, nil, "ALL")
	if err != nil {
		t.Fatalf("degraded checksum failed: %v", err)
	}
	if !degradedChecksum.Partial {
		t.Error("expected partial=true while ruby units are down")
	}
	if degradedChecksum.Count != count || degradedChecksum.Checksum != baselineChecksum.Checksum {
		t.Fatalf("zero data loss violated: baseline=(%d,%s) vs degraded=(%d,%s)",
			baselineChecksum.Count, baselineChecksum.Checksum, degradedChecksum.Count, degradedChecksum.Checksum)
	}
	if len(degradedChecksum.UnavailableUnits) != 2 {
		t.Errorf("expected 2 unavailable units, got %v", degradedChecksum.UnavailableUnits)
	}

	// Write a hazard routed to shard-2 while its primary on ruby is down -> must promote replica on kana!
	var shard2HazardID string
	for idx := 0; idx < 1000; idx++ {
		cand := fmt.Sprintf("haz-failover-%04d", idx)
		if repo.LocateShard(cand) == "shard-2" {
			shard2HazardID = cand
			break
		}
	}
	if shard2HazardID == "" {
		t.Fatal("failed to find key routed to shard-2")
	}
	failoverEvent := makeSampleHazard(shard2HazardID, "failover-ref-1", "Sulawesi", baseTime.Add(5*time.Minute), baseTime.Add(6*time.Minute))
	if err := repo.SaveHazardWithOutbox(ctx, &failoverEvent, "corr-failover"); err != nil {
		t.Fatalf("write during primary failure failed to promote replica: %v", err)
	}
	if pending, _ := backends["shard-2-r1"].FetchUnprocessedOutbox(ctx, 10); len(pending) != 1 {
		t.Errorf("expected promoted replica shard-2-r1 to persist 1 outbox event, got %d", len(pending))
	}

	// 2. Replacement ruby disk/node comes back online EMPTY and joins the cluster.
	backends["shard-2"].SetHealthy(true)
	backends["shard-1-r1"].SetHealthy(true)

	// Before SyncShardReplicas heals ruby, shard-2 is still marked stale. If shard-2-r1 becomes unreachable,
	// LocateHazard must return ErrAllStorageUnavailable (503) rather than falsely claiming 404 not found.
	backends["shard-2-r1"].SetHealthy(false)
	if _, locErr := repo.LocateHazard(ctx, shard2HazardID); !errors.Is(locErr, ErrAllStorageUnavailable) {
		t.Fatalf("expected ErrAllStorageUnavailable while only stale empty primary responds, got %v", locErr)
	}
	backends["shard-2-r1"].SetHealthy(true)

	// SyncShardReplicas restores hazard + DLQ redundancy across recovered units.
	if err := repo.SyncShardReplicas(ctx); err != nil {
		t.Fatalf("SyncShardReplicas failed: %v", err)
	}

	// Verify restored units re-acquired data, including the failover event written during outage.
	if backends["shard-2"].Count() == 0 || backends["shard-1-r1"].Count() == 0 {
		t.Fatalf("expected recovered units to hold synced data, got shard-2=%d shard-1-r1=%d",
			backends["shard-2"].Count(), backends["shard-1-r1"].Count())
	}

	// 3. Rebalance cluster adding a 4th shard (shard-4 on node-d + ruby replica) without downtime.
	newPrimary := NewMemoryUnitBackend()
	newReplica := NewMemoryUnitBackend()
	rebalancedGroups := []ShardGroup{
		{ShardID: "shard-1", Units: []StorageUnit{{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: backends["shard-1"]}, {Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: backends["shard-1-r1"]}}},
		{ShardID: "shard-2", Units: []StorageUnit{{Unit: "shard-2", Node: "ruby", Role: domain.StorageRolePrimary, Backend: backends["shard-2"]}, {Unit: "shard-2-r1", Node: "kana", Role: domain.StorageRoleReplica, Backend: backends["shard-2-r1"]}}},
		{ShardID: "shard-3", Units: []StorageUnit{{Unit: "shard-3", Node: "kana", Role: domain.StorageRolePrimary, Backend: backends["shard-3"]}, {Unit: "shard-3-r1", Node: "aqua", Role: domain.StorageRoleReplica, Backend: backends["shard-3-r1"]}}},
		{ShardID: "shard-4", Units: []StorageUnit{{Unit: "shard-4", Node: "node-d", Role: domain.StorageRolePrimary, Backend: newPrimary}, {Unit: "shard-4-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: newReplica}}},
	}
	if err := repo.RebalanceCluster(ctx, rebalancedGroups); err != nil {
		t.Fatalf("RebalanceCluster failed: %v", err)
	}
	if newPrimary.Count() == 0 || newReplica.Count() != newPrimary.Count() {
		t.Fatalf("expected new shard-4 primary/replica to acquire migrated records, got p=%d r=%d",
			newPrimary.Count(), newReplica.Count())
	}

	// 4. Subsequent SECOND node loss (kana wiped & down) must STILL lose zero of all count+1 events!
	backends["shard-3"].SetHealthy(false)
	backends["shard-3"].Wipe()
	backends["shard-2-r1"].SetHealthy(false)
	backends["shard-2-r1"].Wipe()

	postSecondLoss, err := repo.ComputeAuditChecksum(ctx, nil, "ALL")
	if err != nil {
		t.Fatalf("checksum after second node loss failed: %v", err)
	}
	if postSecondLoss.Count != count+1 {
		t.Fatalf("expected zero loss after second node loss (%d events), got %d", count+1, postSecondLoss.Count)
	}

	dlqAfterSecondLoss, err := repo.QueryAuditUnprocessed(ctx, AuditUnprocessedFilter{Limit: 10})
	if err != nil {
		t.Fatalf("unprocessed query after second node loss failed: %v", err)
	}
	if len(dlqAfterSecondLoss.Items) != 1 || dlqAfterSecondLoss.Items[0].MessageID != shard2MsgID {
		t.Fatalf("expected DLQ message %q to survive both wipes via SyncShardReplicas, got %+v", shard2MsgID, dlqAfterSecondLoss.Items)
	}
}
