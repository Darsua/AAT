package repository

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
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

	// 4. Environment variable expansion inside SHARD_CONFIG
	t.Setenv("POSTGRES_USER", "u")
	t.Setenv("POSTGRES_PASSWORD", "p")
	t.Setenv("STORE_NODE_3", "kana")
	expandedDSL := "shard-3=shard-3|${STORE_NODE_3}|primary|postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@store-kana:5432/bnpb_shard_3?sslmode=disable"
	expandedSpecs, err := ParseShardTopology("", expandedDSL, "", "", "")
	if err != nil {
		t.Fatalf("expanded SHARD_CONFIG parse failed: %v", err)
	}
	if expandedSpecs[0].Units[0].Node != "kana" ||
		expandedSpecs[0].Units[0].DSN != "postgres://u:p@store-kana:5432/bnpb_shard_3?sslmode=disable" {
		t.Fatalf("unexpected expanded SHARD_CONFIG specs: %+v", expandedSpecs)
	}

	// 5. Empty config returns error
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

func TestSyncShardReplicas_NewerReplicaRowVersionWinsOnFreshRepo(t *testing.T) {
	ctx := context.Background()
	primary := NewMemoryUnitBackend()
	replica := NewMemoryUnitBackend()
	groups := []ShardGroup{
		{
			ShardID: "shard-1",
			Units: []StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: primary},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: replica},
			},
		},
	}

	writerRepo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("NewShardedHazardRepository failed: %v", err)
	}

	occurred := time.Date(2026, 11, 17, 1, 0, 0, 0, time.UTC)
	ev := makeSampleHazard("haz-version-1", "bmkg-v1", "Selat Sunda", occurred, occurred)
	ev.Severity = domain.SeverityWaspada
	if err := writerRepo.SaveHazard(ctx, &ev); err != nil {
		t.Fatalf("initial SaveHazard failed: %v", err)
	}

	// Primary goes down, an update with identical OccurredAt lands on the replica (RowVersion=2).
	primary.SetHealthy(false)
	updated := makeSampleHazard("haz-version-1", "bmkg-v1", "Selat Sunda", occurred, occurred)
	updated.Severity = domain.SeverityAwas
	updated.Attributes = map[string]any{"magnitude": 7.1, "potential_tsunami": true}
	if err := writerRepo.SaveHazard(ctx, &updated); err != nil {
		t.Fatalf("replica update during primary outage failed: %v", err)
	}

	// Primary comes back and a DIFFERENT repository instance (empty staleUnits) runs SyncShardReplicas.
	primary.SetHealthy(true)
	freshRepo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("fresh NewShardedHazardRepository failed: %v", err)
	}
	if err := freshRepo.SyncShardReplicas(ctx); err != nil {
		t.Fatalf("SyncShardReplicas failed: %v", err)
	}

	gotPrimary, err := primary.GetHazardByID(ctx, "haz-version-1")
	if err != nil || gotPrimary == nil {
		t.Fatalf("GetHazardByID primary failed: %v", err)
	}
	if gotPrimary.Severity != domain.SeverityAwas || gotPrimary.RowVersion < 2 {
		t.Fatalf("expected primary to converge to replica AWAS RowVersion>=2, got severity=%s row_version=%d",
			gotPrimary.Severity, gotPrimary.RowVersion)
	}
}

func TestQueryHazards_PrimaryFirstAndPaginationCount(t *testing.T) {
	ctx := context.Background()
	repo, _, _ := buildThreeNodeShardedRepo(t)

	base := time.Date(2026, 11, 17, 2, 0, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		ev := makeSampleHazard(fmt.Sprintf("haz-page-%02d", i), fmt.Sprintf("ref-%02d", i), "Jawa Barat", ts, ts)
		if err := repo.SaveHazard(ctx, &ev); err != nil {
			t.Fatalf("SaveHazard %d failed: %v", i, err)
		}
	}

	page1, total, err := repo.QueryHazards(ctx, HazardFilter{Limit: 5, Offset: 0})
	if err != nil {
		t.Fatalf("QueryHazards page1 failed: %v", err)
	}
	if total != 12 || len(page1) != 5 {
		t.Fatalf("expected total=12 len=5, got total=%d len=%d", total, len(page1))
	}
	if page1[0].HazardID != "haz-page-11" {
		t.Fatalf("expected newest hazard first (haz-page-11), got %s", page1[0].HazardID)
	}

	page2, total2, err := repo.QueryHazards(ctx, HazardFilter{Limit: 5, Offset: 5})
	if err != nil {
		t.Fatalf("QueryHazards page2 failed: %v", err)
	}
	if total2 != 12 || len(page2) != 5 || page2[0].HazardID != "haz-page-06" {
		t.Fatalf("unexpected page2: total=%d len=%d first=%+v", total2, len(page2), page2)
	}
}

func TestRecoveredDLQMessageIsNotReopenedByStaleReplica(t *testing.T) {
	ctx := context.Background()
	primary := NewMemoryUnitBackend()
	replica := NewMemoryUnitBackend()
	groups := []ShardGroup{
		{
			ShardID: "shard-1",
			Units: []StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: primary},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: replica},
			},
		},
	}
	repo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("NewShardedHazardRepository failed: %v", err)
	}

	msg := domain.UnprocessedMessage{
		MessageID:  "pvmbg-recover-1",
		Source:     domain.SourcePVMBG,
		RawPayload: []byte(`{"report_id":"r-1"}`),
		ReceivedAt: time.Date(2026, 11, 17, 3, 0, 0, 0, time.UTC),
		Reason:     "schema_incompatible: missing volcano_id",
		Attempts:   5,
		Status:     domain.UnprocessedStatusParked,
	}
	if err := repo.SaveUnprocessedMessage(ctx, msg); err != nil {
		t.Fatalf("park failed: %v", err)
	}

	// The replica misses the replay's recovered write.
	replica.SetHealthy(false)
	msg.Status = domain.UnprocessedStatusRecovered
	if err := repo.SaveUnprocessedMessage(ctx, msg); err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	replica.SetHealthy(true)

	got, err := repo.QueryAuditUnprocessed(ctx, AuditUnprocessedFilter{Limit: 10})
	if err != nil {
		t.Fatalf("QueryAuditUnprocessed failed: %v", err)
	}
	if len(got.Items) != 0 {
		t.Fatalf("expected the recovered message to stay hidden behind the stale replica, got %+v", got.Items)
	}

	// Sync from a repository that does not know the replica is stale must not reopen it.
	freshRepo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("fresh NewShardedHazardRepository failed: %v", err)
	}
	_ = freshRepo.SyncShardReplicas(ctx)
	active, err := primary.QueryUnprocessed(ctx, nil)
	if err != nil {
		t.Fatalf("primary QueryUnprocessed failed: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected the primary to keep the message recovered after sync, got %+v", active)
	}
}

func TestQueryAuditEventsPushdownAcrossShardsAndPartialOutage(t *testing.T) {
	ctx := context.Background()
	repo, backends, _ := buildThreeNodeShardedRepo(t)

	base := time.Date(2026, 11, 18, 4, 0, 0, 0, time.UTC)
	events := []domain.HazardEvent{
		{HazardID: "haz-push-b", Source: domain.SourceBMKG, SourceRefID: "ref-b", HazardType: domain.HazardTypeSeismic, Severity: domain.SeverityNormal, AreaName: "Aceh", OccurredAt: base.Add(-4 * time.Minute), IngestedAt: base},
		{HazardID: "haz-push-a", Source: domain.SourceSatelit, SourceRefID: "ref-a", HazardType: domain.HazardTypeWildfire, Severity: domain.SeveritySiaga, AreaName: "Riau", OccurredAt: base.Add(-5 * time.Minute), IngestedAt: base},
		{HazardID: "haz-push-c", Source: domain.SourcePVMBG, SourceRefID: "ref-c", HazardType: domain.HazardTypeVolcanic, Severity: domain.SeverityWaspada, AreaName: "Merapi", OccurredAt: base.Add(-3 * time.Minute), IngestedAt: base.Add(1 * time.Second)},
		{HazardID: "haz-push-d", Source: domain.SourceBMKG, SourceRefID: "ref-d", HazardType: domain.HazardTypeSeismic, Severity: domain.SeverityNormal, AreaName: "Palu", OccurredAt: base.Add(-2 * time.Minute), IngestedAt: base.Add(2 * time.Second)},
		{HazardID: "haz-push-e", Source: domain.SourceBMKG, SourceRefID: "ref-e", HazardType: domain.HazardTypeSeismic, Severity: domain.SeverityAwas, AreaName: "Ambon", OccurredAt: base.Add(-1 * time.Minute), IngestedAt: base.Add(3 * time.Second)},
	}
	for i := range events {
		if err := repo.SaveHazard(ctx, &events[i]); err != nil {
			t.Fatalf("SaveHazard %s failed: %v", events[i].HazardID, err)
		}
	}

	var walked []string
	var cursor string
	for {
		res, err := repo.QueryAuditEvents(ctx, AuditEventsFilter{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("QueryAuditEvents failed: %v", err)
		}
		for _, item := range res.Items {
			walked = append(walked, item.HazardID)
		}
		if res.NextCursor == nil {
			break
		}
		cursor = *res.NextCursor
	}
	wantOrder := []string{"haz-push-a", "haz-push-b", "haz-push-c", "haz-push-d", "haz-push-e"}
	if !slices.Equal(walked, wantOrder) {
		t.Fatalf("walked order = %v, want %v", walked, wantOrder)
	}

	// Offset cursor {"o": 2} and since/source filters must honor limit+offset pushdown.
	offRes, err := repo.QueryAuditEvents(ctx, AuditEventsFilter{Limit: 2, Cursor: "eyJvIjoyfQ"})
	if err != nil {
		t.Fatalf("offset QueryAuditEvents failed: %v", err)
	}
	if len(offRes.Items) != 2 || offRes.Items[0].HazardID != "haz-push-c" || offRes.Items[1].HazardID != "haz-push-d" || offRes.NextCursor == nil {
		t.Fatalf("unexpected offset cursor result: %+v", offRes)
	}

	bmkg := domain.SourceBMKG
	since := base.Add(1 * time.Second)
	fltRes, err := repo.QueryAuditEvents(ctx, AuditEventsFilter{Source: &bmkg, Since: &since, Limit: 10})
	if err != nil {
		t.Fatalf("filtered QueryAuditEvents failed: %v", err)
	}
	if len(fltRes.Items) != 2 || fltRes.Items[0].HazardID != "haz-push-d" || fltRes.Items[1].HazardID != "haz-push-e" {
		t.Fatalf("unexpected source+since result: %+v", fltRes.Items)
	}

	// Partial outage preserves exact coverage via replicas while surfacing degraded units.
	backends["shard-2"].SetHealthy(false)
	backends["shard-1-r1"].SetHealthy(false)
	partRes, err := repo.QueryAuditEvents(ctx, AuditEventsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("partial QueryAuditEvents failed: %v", err)
	}
	if !partRes.Partial || len(partRes.UnavailableUnits) != 2 || len(partRes.Items) != 5 {
		t.Fatalf("unexpected partial audit result: partial=%v unavailable=%v items=%d", partRes.Partial, partRes.UnavailableUnits, len(partRes.Items))
	}
}

func TestComputeAuditChecksumUsesAuditRefsAndDedupPrecedence(t *testing.T) {
	ctx := context.Background()
	primary := NewMemoryUnitBackend()
	replica := NewMemoryUnitBackend()
	groups := []ShardGroup{
		{
			ShardID: "shard-1",
			Units: []StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: primary},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: replica},
			},
		},
	}
	repo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("NewShardedHazardRepository failed: %v", err)
	}

	now := time.Date(2026, 11, 18, 5, 0, 0, 0, time.UTC)
	if err := primary.SaveHazard(ctx, &domain.HazardEvent{
		HazardID:     "haz-csum-1",
		Source:       domain.SourceBMKG,
		SourceRefID:  "ref-new-primary",
		HazardType:   domain.HazardTypeSeismic,
		Severity:     domain.SeveritySiaga,
		AreaName:     "Aceh",
		OccurredAt:   now,
		IngestedAt:   now,
		RowVersion:   2,
		FencingToken: 5,
	}); err != nil {
		t.Fatalf("save primary failed: %v", err)
	}
	if err := replica.SaveHazard(ctx, &domain.HazardEvent{
		HazardID:     "haz-csum-1",
		Source:       domain.SourceBMKG,
		SourceRefID:  "ref-stale-replica",
		HazardType:   domain.HazardTypeSeismic,
		Severity:     domain.SeveritySiaga,
		AreaName:     "Aceh",
		OccurredAt:   now,
		IngestedAt:   now,
		RowVersion:   1,
		FencingToken: 4,
	}); err != nil {
		t.Fatalf("save replica failed: %v", err)
	}

	// Mark primary stale in repo state so non-stale replica precedence is also exercised cleanly.
	csumPrimary, err := repo.ComputeAuditChecksum(ctx, nil, "ALL")
	if err != nil {
		t.Fatalf("ComputeAuditChecksum failed: %v", err)
	}
	eventsRes, err := repo.QueryAuditEvents(ctx, AuditEventsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("QueryAuditEvents failed: %v", err)
	}
	if len(eventsRes.Items) != 1 || eventsRes.Items[0].SourceRefID != "ref-new-primary" {
		t.Fatalf("expected primary record to win dedup, got %+v", eventsRes.Items)
	}
	if csumPrimary.Count != 1 {
		t.Fatalf("expected count=1, got %d", csumPrimary.Count)
	}

	repo.markUnitStale("shard-1", true)
	csumStale, err := repo.ComputeAuditChecksum(ctx, nil, "ALL")
	if err != nil {
		t.Fatalf("ComputeAuditChecksum with stale unit failed: %v", err)
	}
	eventsStale, err := repo.QueryAuditEvents(ctx, AuditEventsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("QueryAuditEvents with stale unit failed: %v", err)
	}
	if len(eventsStale.Items) != 1 || eventsStale.Items[0].SourceRefID != "ref-stale-replica" || csumStale.Checksum == csumPrimary.Checksum {
		t.Fatalf("expected non-stale replica to win over stale primary in both checksum and events, got item=%+v", eventsStale.Items)
	}
}

type countingBackendSpy struct {
	*MemoryUnitBackend
	getByIDsCalls  int
	saveBatchCalls int
}

func (s *countingBackendSpy) GetHazardsByIDs(ctx context.Context, hazardIDs []string) ([]domain.HazardEvent, error) {
	s.getByIDsCalls++
	return s.MemoryUnitBackend.GetHazardsByIDs(ctx, hazardIDs)
}

func (s *countingBackendSpy) SaveHazardsBatch(ctx context.Context, events []domain.HazardEvent) error {
	s.saveBatchCalls++
	return s.MemoryUnitBackend.SaveHazardsBatch(ctx, events)
}

func TestSyncShardReplicas_DiffFirstAndBatchedRecovery(t *testing.T) {
	ctx := context.Background()
	raw := map[string]*MemoryUnitBackend{
		"shard-1":    NewMemoryUnitBackend(),
		"shard-1-r1": NewMemoryUnitBackend(),
		"shard-2":    NewMemoryUnitBackend(),
		"shard-2-r1": NewMemoryUnitBackend(),
		"shard-3":    NewMemoryUnitBackend(),
		"shard-3-r1": NewMemoryUnitBackend(),
	}
	spies := make(map[string]*countingBackendSpy, len(raw))
	for k, v := range raw {
		spies[k] = &countingBackendSpy{MemoryUnitBackend: v}
	}
	groups := []ShardGroup{
		{
			ShardID: "shard-1",
			Units: []StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: spies["shard-1"]},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: spies["shard-1-r1"]},
			},
		},
		{
			ShardID: "shard-2",
			Units: []StorageUnit{
				{Unit: "shard-2", Node: "ruby", Role: domain.StorageRolePrimary, Backend: spies["shard-2"]},
				{Unit: "shard-2-r1", Node: "akane", Role: domain.StorageRoleReplica, Backend: spies["shard-2-r1"]},
			},
		},
		{
			ShardID: "shard-3",
			Units: []StorageUnit{
				{Unit: "shard-3", Node: "akane", Role: domain.StorageRolePrimary, Backend: spies["shard-3"]},
				{Unit: "shard-3-r1", Node: "aqua", Role: domain.StorageRoleReplica, Backend: spies["shard-3-r1"]},
			},
		},
	}
	repo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("NewShardedHazardRepository failed: %v", err)
	}

	const total = 550
	base := time.Date(2026, 11, 19, 1, 0, 0, 0, time.UTC)
	for i := range total {
		ts := base.Add(time.Duration(i) * time.Second)
		ev := makeSampleHazard(fmt.Sprintf("haz-batch-%04d", i), fmt.Sprintf("ref-batch-%04d", i), "Jawa Tengah", ts, ts)
		if err := repo.SaveHazard(ctx, &ev); err != nil {
			t.Fatalf("SaveHazard %d failed: %v", i, err)
		}
	}
	baseline, err := repo.ComputeAuditChecksum(ctx, nil, "ALL")
	if err != nil {
		t.Fatalf("baseline checksum failed: %v", err)
	}

	// Simulate ruby node wipe (shard-2 primary + shard-1-r1 replica).
	raw["shard-2"].SetHealthy(false)
	raw["shard-2"].Wipe()
	raw["shard-1-r1"].SetHealthy(false)
	raw["shard-1-r1"].Wipe()
	raw["shard-2"].SetHealthy(true)
	raw["shard-1-r1"].SetHealthy(true)

	if err := repo.SyncShardReplicas(ctx); err != nil {
		t.Fatalf("SyncShardReplicas recovery failed: %v", err)
	}

	if spies["shard-2"].saveBatchCalls == 0 || spies["shard-1-r1"].saveBatchCalls == 0 {
		t.Fatalf("expected batched writes on wiped units, got shard-2=%d shard-1-r1=%d",
			spies["shard-2"].saveBatchCalls, spies["shard-1-r1"].saveBatchCalls)
	}
	if spies["shard-2-r1"].getByIDsCalls == 0 || spies["shard-1"].getByIDsCalls == 0 {
		t.Fatalf("expected batch fetches from surviving peers, got shard-2-r1=%d shard-1=%d",
			spies["shard-2-r1"].getByIDsCalls, spies["shard-1"].getByIDsCalls)
	}
	if got := raw["shard-2"].Count() + raw["shard-1"].Count() + raw["shard-3"].Count(); got != total {
		t.Fatalf("expected total %d across recovered primaries, got %d", total, got)
	}
	after, err := repo.ComputeAuditChecksum(ctx, nil, "ALL")
	if err != nil || after.Count != baseline.Count || after.Checksum != baseline.Checksum {
		t.Fatalf("checksum mismatch after recovery: baseline=%+v after=%+v err=%v", baseline, after, err)
	}

	// Reset spy counters and verify steady-state sync transfers 0 batches/rows.
	for _, s := range spies {
		s.getByIDsCalls = 0
		s.saveBatchCalls = 0
	}
	if err := repo.SyncShardReplicas(ctx); err != nil {
		t.Fatalf("steady-state SyncShardReplicas failed: %v", err)
	}
	for name, s := range spies {
		if s.getByIDsCalls != 0 || s.saveBatchCalls != 0 {
			t.Fatalf("steady-state sync unit %s had unexpected calls getByIDs=%d saveBatch=%d",
				name, s.getByIDsCalls, s.saveBatchCalls)
		}
	}
}

func TestSyncShardReplicas_RespectsContextCancellation(t *testing.T) {
	repo, _, _ := buildThreeNodeShardedRepo(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := repo.SyncShardReplicas(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSyncShardReplicas_ReconcilesSatelliteCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo, backends, _ := buildThreeNodeShardedRepo(t)

	lease, ok, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceSatelit, "agg-1", 0, time.Minute)
	if err != nil || !ok {
		t.Fatalf("AcquireOrRenewSourceLease failed: ok=%v err=%v", ok, err)
	}
	fence := &domain.WriteFence{
		LeaseKey:     LeaseKeyForSource(domain.SourceSatelit),
		Source:       domain.SourceSatelit,
		HolderID:     "agg-1",
		FencingToken: lease.FencingToken,
	}
	wantCP := SatelliteCheckpoint{
		Stream:     "satellite",
		LastOffset: 42,
		State:      []byte(`{"cells":{"c1":2}}`),
	}
	if err := repo.SaveSatelliteCheckpoint(ctx, wantCP, fence); err != nil {
		t.Fatalf("SaveSatelliteCheckpoint failed: %v", err)
	}

	backends["shard-1-r1"].Wipe()
	if got, err := backends["shard-1-r1"].GetSatelliteCheckpoint(ctx, "satellite"); err != nil || got != nil {
		t.Fatalf("expected wiped shard-1-r1 checkpoint to be nil, got %+v err=%v", got, err)
	}

	if err := repo.SyncShardReplicas(ctx); err != nil {
		t.Fatalf("SyncShardReplicas failed: %v", err)
	}

	restored, err := backends["shard-1-r1"].GetSatelliteCheckpoint(ctx, "satellite")
	if err != nil || restored == nil {
		t.Fatalf("expected restored satellite checkpoint on shard-1-r1, got %+v err=%v", restored, err)
	}
	if restored.LastOffset != wantCP.LastOffset || string(restored.State) != string(wantCP.State) {
		t.Fatalf("restored checkpoint mismatch: got %+v want %+v", restored, wantCP)
	}
}

func TestParseShardTopology_RedactsCredentialsInError(t *testing.T) {
	cases := []struct {
		name string
		cfg  string
	}{
		{
			name: "malformed_entry",
			cfg:  "postgres://user:supersecret@db:5432/x",
		},
		{
			name: "malformed_unit",
			cfg:  "shard-1=shard-1|aqua|postgres://user:supersecret@db:5432/x",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseShardTopology("", tc.cfg, "", "", "")
			if err == nil {
				t.Fatal("expected ParseShardTopology error")
			}
			if strings.Contains(err.Error(), "supersecret") {
				t.Fatalf("expected error to redact DSN credentials, got %q", err.Error())
			}
			if !strings.Contains(err.Error(), "***") {
				t.Fatalf("expected masked credentials marker '***' in error, got %q", err.Error())
			}
		})
	}
}

func TestSyncShardReplicas_DoesNotResurrectRecoveredDLQWhenPrimaryMissedRecovery(t *testing.T) {
	ctx := context.Background()
	primary := NewMemoryUnitBackend()
	replica := NewMemoryUnitBackend()
	groups := []ShardGroup{
		{
			ShardID: "shard-1",
			Units: []StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: primary},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: replica},
			},
		},
	}
	repo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("NewShardedHazardRepository failed: %v", err)
	}

	msg := domain.UnprocessedMessage{
		MessageID:  "pvmbg-recover-primary-miss",
		Source:     domain.SourcePVMBG,
		RawPayload: []byte(`{"report_id":"r-miss"}`),
		ReceivedAt: time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC),
		Reason:     "schema_incompatible: missing volcano_id",
		Attempts:   3,
		Status:     domain.UnprocessedStatusParked,
	}
	if err := repo.SaveUnprocessedMessage(ctx, msg); err != nil {
		t.Fatalf("park failed: %v", err)
	}

	// Primary misses the recovery write; only the replica records Status=recovered.
	primary.SetHealthy(false)
	msg.Status = domain.UnprocessedStatusRecovered
	if err := repo.SaveUnprocessedMessage(ctx, msg); err != nil {
		t.Fatalf("recover on surviving replica failed: %v", err)
	}
	primary.SetHealthy(true)

	freshRepo, err := NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("fresh NewShardedHazardRepository failed: %v", err)
	}
	if err := freshRepo.SyncShardReplicas(ctx); err != nil {
		t.Fatalf("SyncShardReplicas failed: %v", err)
	}

	for name, unit := range map[string]*MemoryUnitBackend{"primary": primary, "replica": replica} {
		active, err := unit.QueryUnprocessed(ctx, nil)
		if err != nil {
			t.Fatalf("%s QueryUnprocessed failed: %v", name, err)
		}
		if len(active) != 0 {
			t.Fatalf("expected %s to keep recovered DLQ message closed, got %+v", name, active)
		}
	}
}

type stalledOutboxBackend struct {
	*MemoryUnitBackend
}

func (s *stalledOutboxBackend) RelayOutboxBatch(
	ctx context.Context,
	_ int,
	_ func(records []domain.OutboxEvent) (published []domain.OutboxEvent, quarantined []domain.OutboxEvent, err error),
) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestHazardRepo_StalledOutboxUnitDoesNotBlockHealthy(t *testing.T) {
	stalled := &stalledOutboxBackend{MemoryUnitBackend: NewMemoryUnitBackend()}
	healthy := NewMemoryUnitBackend()

	repo, err := NewShardedHazardRepository([]ShardGroup{
		{
			ShardID: "shard-1",
			Units: []StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: stalled},
			},
		},
		{
			ShardID: "shard-2",
			Units: []StorageUnit{
				{Unit: "shard-2", Node: "ruby", Role: domain.StorageRolePrimary, Backend: healthy},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewShardedHazardRepository failed: %v", err)
	}

	now := time.Date(2026, 11, 21, 9, 30, 0, 0, time.UTC)
	ev := makeSampleHazard("haz-outbox-healthy", "ref-outbox-healthy", "Yogyakarta", now, now)
	if err := healthy.SaveHazardWithOutbox(context.Background(), &ev, "corr-outbox-healthy"); err != nil {
		t.Fatalf("SaveHazardWithOutbox failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	started := time.Now()
	var relayed []domain.OutboxEvent
	err = repo.RelayOutboxBatch(ctx, 10, func(records []domain.OutboxEvent) ([]domain.OutboxEvent, []domain.OutboxEvent, error) {
		relayed = append(relayed, records...)
		return records, nil, nil
	})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("expected RelayOutboxBatch to succeed via healthy shard, got %v", err)
	}
	if elapsed >= 6*time.Second {
		t.Fatalf("expected stalled unit timeout (~3s) not to stall relay past 6s, took %v", elapsed)
	}
	if len(relayed) != 1 {
		t.Fatalf("expected 1 relayed outbox record from healthy shard, got %d", len(relayed))
	}
	remaining, err := healthy.FetchUnprocessedOutbox(context.Background(), 10)
	if err != nil {
		t.Fatalf("FetchUnprocessedOutbox failed: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected healthy shard outbox to be drained, remaining=%d", len(remaining))
	}
}
