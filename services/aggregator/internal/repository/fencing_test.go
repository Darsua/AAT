package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
)

func advanceAllBackendClocks(backends map[string]*MemoryUnitBackend, d time.Duration) {
	for _, b := range backends {
		b.AdvanceClock(d)
	}
}

func TestLeaderLease_ExclusiveAcquisitionRenewalAndFailover(t *testing.T) {
	repo, backends, _ := buildThreeNodeShardedRepo(t)
	ctx := context.Background()
	ttl := 15 * time.Second

	// 1. Only 1 of 3 contending replicas acquires the lease with fencing_token = 1.
	l1, ok1, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-1", 0, ttl)
	if err != nil || !ok1 || l1.FencingToken != 1 {
		t.Fatalf("expected agg-1 to acquire token 1, got lease=%+v ok=%v err=%v", l1, ok1, err)
	}
	l2, ok2, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-2", 0, ttl)
	if err != nil || ok2 || l2.HolderID != "agg-1" {
		t.Fatalf("expected agg-2 rejected while agg-1 holds lease, got lease=%+v ok=%v err=%v", l2, ok2, err)
	}
	l3, ok3, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-3", 0, ttl)
	if err != nil || ok3 || l3.HolderID != "agg-1" {
		t.Fatalf("expected agg-3 rejected while agg-1 holds lease, got lease=%+v ok=%v err=%v", l3, ok3, err)
	}

	// 2. Active leader renews preserving fencing_token = 1 while standby still loses.
	advanceAllBackendClocks(backends, 5*time.Second)
	renewed, okRenew, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-1", l1.FencingToken, ttl)
	if err != nil || !okRenew || renewed.FencingToken != 1 {
		t.Fatalf("expected agg-1 renewal preserving token 1, got lease=%+v ok=%v err=%v", renewed, okRenew, err)
	}
	if _, ok2Again, _ := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-2", 0, ttl); ok2Again {
		t.Fatal("expected agg-2 still rejected after agg-1 renewal")
	}

	// 3. Simulate agg-1 hard crash: coordinator time advances past 15s TTL -> agg-2 takes over within <=60s with token=2.
	advanceAllBackendClocks(backends, 16*time.Second)
	takeover, okTakeover, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-2", 0, ttl)
	if err != nil || !okTakeover || takeover.HolderID != "agg-2" || takeover.FencingToken != 2 {
		t.Fatalf("expected agg-2 takeover with token 2, got lease=%+v ok=%v err=%v", takeover, okTakeover, err)
	}

	// 4. Graceful ReleaseSourceLease lets agg-3 take over immediately with token=3 without waiting for TTL.
	if err := repo.ReleaseSourceLease(ctx, domain.SourceBMKG, "agg-2", takeover.FencingToken); err != nil {
		t.Fatalf("ReleaseSourceLease failed: %v", err)
	}
	handed, okHand, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-3", 0, ttl)
	if err != nil || !okHand || handed.HolderID != "agg-3" || handed.FencingToken != 3 {
		t.Fatalf("expected immediate handover to agg-3 with token 3, got lease=%+v ok=%v err=%v", handed, okHand, err)
	}
}

func TestFencing_FrozenLeaderRejectedAtStorage(t *testing.T) {
	repo, backends, _ := buildThreeNodeShardedRepo(t)
	ctx := context.Background()
	ttl := 15 * time.Second
	t0 := time.Date(2026, 11, 14, 16, 0, 0, 0, time.UTC)

	// Case A: agg-1 holds token 1, freezes, agg-2 takes over with token 2 -> stale write from agg-1 rejected at storage.
	l1, ok, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-1", 0, ttl)
	if err != nil || !ok {
		t.Fatalf("agg-1 lease acquisition failed: %v", err)
	}
	fence1 := domain.WriteFence{
		LeaseKey:     LeaseKeyForSource(domain.SourceBMKG),
		Source:       domain.SourceBMKG,
		HolderID:     "agg-1",
		FencingToken: l1.FencingToken,
	}

	advanceAllBackendClocks(backends, 16*time.Second)
	l2, ok, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-2", 0, ttl)
	if err != nil || !ok || l2.FencingToken != 2 {
		t.Fatalf("expected agg-2 takeover with token 2, got %+v ok=%v err=%v", l2, ok, err)
	}

	staleEv := makeSampleHazard("haz-stale-wake-1", "ref-stale-1", "Lombok", t0, t0)
	if err := repo.SaveHazardWithFence(ctx, &staleEv, "corr-stale", fence1); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("expected ErrStaleFencingToken for superseded leader agg-1, got %v", err)
	}
	if got, _ := repo.GetHazardByID(ctx, staleEv.HazardID); got != nil {
		t.Fatalf("stale write must not exist in canonical storage, found %+v", got)
	}
	if err := repo.SaveSourceCheckpoint(ctx, domain.SourceCheckpoint{
		Source:          domain.SourceBMKG,
		LastOccurredAt:  t0,
		LastSourceRefID: "ref-stale-1",
		FencingToken:    fence1.FencingToken,
		HolderID:        "agg-1",
	}); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("expected ErrStaleFencingToken on stale checkpoint update, got %v", err)
	}

	// Case B: Active holder agg-2 (token=2) freezes longer than TTL before ANY other replica takes over ->
	// waking up and writing with expired token=2 is STILL rejected at storage until refreshing its lease!
	fence2 := domain.WriteFence{
		LeaseKey:     LeaseKeyForSource(domain.SourceBMKG),
		Source:       domain.SourceBMKG,
		HolderID:     "agg-2",
		FencingToken: l2.FencingToken,
	}
	advanceAllBackendClocks(backends, 16*time.Second)

	expiredEv := makeSampleHazard("haz-expired-wake-2", "ref-exp-2", "Palu", t0.Add(time.Minute), t0.Add(time.Minute))
	if err := repo.SaveHazardWithFence(ctx, &expiredEv, "corr-expired", fence2); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("expected ErrStaleFencingToken for frozen leader whose TTL expired even with no challenger, got %v", err)
	}
	if got, _ := repo.GetHazardByID(ctx, expiredEv.HazardID); got != nil {
		t.Fatalf("expired leader write must not commit, found %+v", got)
	}

	// After refreshing authority, agg-2 receives new monotonic token 3 and write succeeds.
	refreshed, ok, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-2", fence2.FencingToken, ttl)
	if err != nil || !ok || refreshed.FencingToken != 3 {
		t.Fatalf("expected agg-2 refreshed lease token 3, got %+v ok=%v err=%v", refreshed, ok, err)
	}
	fence3 := domain.WriteFence{
		LeaseKey:     LeaseKeyForSource(domain.SourceBMKG),
		Source:       domain.SourceBMKG,
		HolderID:     "agg-2",
		FencingToken: refreshed.FencingToken,
	}
	if err := repo.SaveHazardWithFence(ctx, &expiredEv, "corr-refreshed", fence3); err != nil {
		t.Fatalf("expected write to succeed after lease refresh, got %v", err)
	}
}

func TestFencing_ClockSkewPlus45sAndMinus30s(t *testing.T) {
	repo, backends, _ := buildThreeNodeShardedRepo(t)
	ctx := context.Background()
	ttl := 15 * time.Second
	t0 := time.Date(2026, 11, 14, 17, 0, 0, 0, time.UTC)

	// Simulate node clocks skewed +45s (agg-fast) and -30s (agg-slow) creating skewed event timestamps/local beliefs,
	// while storage coordinator enforces lease expiry and fencing monotonically on coordinator clock.
	leaseFast, okFast, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourcePVMBG, "agg-fast", 0, ttl)
	if err != nil || !okFast || leaseFast.FencingToken != 1 {
		t.Fatalf("expected agg-fast to acquire token 1, got %+v ok=%v err=%v", leaseFast, okFast, err)
	}
	// Skewed +45s local belief cannot preempt active lease held by agg-fast when coordinator elapsed time is 0s.
	if _, okSlow, _ := repo.AcquireOrRenewSourceLease(ctx, domain.SourcePVMBG, "agg-slow", 0, ttl); okSlow {
		t.Fatal("agg-slow must not preempt active lease regardless of node clock skew")
	}

	// Coordinator clock advances +16s (agg-fast froze / failed to renew in storage time; agg-slow is -30s skewed).
	advanceAllBackendClocks(backends, 16*time.Second)

	leaseSlow, okSlow, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourcePVMBG, "agg-slow", 0, ttl)
	if err != nil || !okSlow || leaseSlow.FencingToken != 2 {
		t.Fatalf("expected agg-slow to acquire token 2 once coordinator TTL elapsed, got %+v ok=%v err=%v", leaseSlow, okSlow, err)
	}

	// Both replicas try committing for the same source period: only token 2 (agg-slow) succeeds; token 1 (agg-fast) fails at storage.
	evFast := makeSampleHazard("haz-skew-1", "pvmbg-skew-1", "Gunung Merapi", t0, t0.Add(45*time.Second))
	evFast.Source = domain.SourcePVMBG
	evFast.HazardType = domain.HazardTypeVolcanic

	errFast := repo.SaveHazardWithFence(ctx, &evFast, "corr-fast", domain.WriteFence{
		Source:       domain.SourcePVMBG,
		HolderID:     "agg-fast",
		FencingToken: leaseFast.FencingToken,
	})
	if !errors.Is(errFast, ErrStaleFencingToken) {
		t.Fatalf("expected skewed former leader agg-fast rejected at storage with ErrStaleFencingToken, got %v", errFast)
	}

	evSlow := makeSampleHazard("haz-skew-1", "pvmbg-skew-1", "Gunung Merapi", t0, t0.Add(-30*time.Second))
	evSlow.Source = domain.SourcePVMBG
	evSlow.HazardType = domain.HazardTypeVolcanic

	errSlow := repo.SaveHazardWithFence(ctx, &evSlow, "corr-slow", domain.WriteFence{
		Source:       domain.SourcePVMBG,
		HolderID:     "agg-slow",
		FencingToken: leaseSlow.FencingToken,
	})
	if errSlow != nil {
		t.Fatalf("expected active leader agg-slow to commit cleanly, got %v", errSlow)
	}
}

func TestLeaderLease_CoordinatorPrimaryFailoverAndWipeRecovery(t *testing.T) {
	repo, backends, _ := buildThreeNodeShardedRepo(t)
	ctx := context.Background()
	ttl := 15 * time.Second
	t0 := time.Date(2026, 11, 14, 18, 0, 0, 0, time.UTC)

	// 1. agg-1 acquires token 1 on BMKG.
	l1, ok1, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-1", 0, ttl)
	if err != nil || !ok1 || l1.FencingToken != 1 {
		t.Fatalf("expected agg-1 lease token 1, got %+v ok=%v err=%v", l1, ok1, err)
	}
	fence1 := domain.WriteFence{
		LeaseKey:     LeaseKeyForSource(domain.SourceBMKG),
		Source:       domain.SourceBMKG,
		HolderID:     "agg-1",
		FencingToken: l1.FencingToken,
	}

	// 2. Coordinator shard primary (shard-1 on aqua) goes offline; clock advances; agg-2 takes over via shard-1-r1 with token 2.
	backends["shard-1"].SetHealthy(false)
	advanceAllBackendClocks(backends, 16*time.Second)

	l2, ok2, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-2", 0, ttl)
	if err != nil || !ok2 || l2.FencingToken != 2 || l2.HolderID != "agg-2" {
		t.Fatalf("expected agg-2 takeover via replica with token 2, got %+v ok=%v err=%v", l2, ok2, err)
	}
	fence2 := domain.WriteFence{
		LeaseKey:     LeaseKeyForSource(domain.SourceBMKG),
		Source:       domain.SourceBMKG,
		HolderID:     "agg-2",
		FencingToken: l2.FencingToken,
	}

	evFailover := makeSampleHazard("haz-coord-failover-1", "ref-coord-1", "Ambon", t0, t0)
	if err := repo.SaveHazardWithFence(ctx, &evFailover, "corr-failover", fence2); err != nil {
		t.Fatalf("expected agg-2 fenced write while shard-1 primary offline, got %v", err)
	}
	if err := repo.SaveSourceCheckpoint(ctx, domain.SourceCheckpoint{
		Source:          domain.SourceBMKG,
		LastOccurredAt:  t0,
		LastSourceRefID: "ref-coord-1",
		FencingToken:    l2.FencingToken,
		HolderID:        "agg-2",
	}); err != nil {
		t.Fatalf("expected checkpoint save while shard-1 primary offline, got %v", err)
	}

	// 3. shard-1 returns after complete disk wipe (no leases/fences/checkpoints locally) BEFORE SyncShardReplicas runs.
	backends["shard-1"].Wipe()
	backends["shard-1"].SetHealthy(true)

	// Reconciled coordinator state allows agg-2's token 2 write, blocks agg-1's stale token 1, and blocks split-brain agg-3.
	evPostWipe := makeSampleHazard("haz-coord-failover-2", "ref-coord-2", "Kupang", t0.Add(time.Minute), t0.Add(time.Minute))
	if err := repo.SaveHazardWithFence(ctx, &evPostWipe, "corr-post-wipe", fence2); err != nil {
		t.Fatalf("expected agg-2 write to succeed after wiped primary returned, got %v", err)
	}
	evStale := makeSampleHazard("haz-coord-stale", "ref-coord-stale", "Maluku", t0.Add(2*time.Minute), t0.Add(2*time.Minute))
	if err := repo.SaveHazardWithFence(ctx, &evStale, "corr-stale", fence1); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("expected ErrStaleFencingToken for agg-1 after wiped primary returned, got %v", err)
	}
	if l3, ok3, err := repo.AcquireOrRenewSourceLease(ctx, domain.SourceBMKG, "agg-3", 0, ttl); err != nil || ok3 || l3.HolderID != "agg-2" {
		t.Fatalf("expected agg-3 blocked from split-brain acquisition, got %+v ok=%v err=%v", l3, ok3, err)
	}

	// 4. SyncShardReplicas recovers durable source_fences and checkpoint state onto shard-1 primary.
	if err := repo.SyncShardReplicas(ctx); err != nil {
		t.Fatalf("SyncShardReplicas failed: %v", err)
	}
	maxFence, err := backends["shard-1"].GetMaxSourceFence(ctx, domain.SourceBMKG)
	if err != nil || maxFence != 2 {
		t.Fatalf("expected shard-1 max fence 2 after sync, got %d (err=%v)", maxFence, err)
	}
	cp, err := backends["shard-1"].GetSourceCheckpoint(ctx, domain.SourceBMKG)
	if err != nil || cp == nil || cp.LastSourceRefID != "ref-coord-1" || cp.FencingToken != 2 {
		t.Fatalf("expected shard-1 checkpoint recovered after sync, got %+v (err=%v)", cp, err)
	}
}
