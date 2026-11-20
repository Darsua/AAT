# Problem 2: HA Manual Scaling (`3 -> 5 -> 2 -> 3`) & Exclusive Leader Leasing (M2)

**Recorded date: 2026-11-20 (15:13–15:14 UTC).** Live non-destructive verification of Problem 2 replica scaling (`#25`) and distributed PostgreSQL lease fencing across `deployment/aggregator`. Output recorded in [`ha-scaling-fencing.log`](ha-scaling-fencing.log).

## Procedure

1. Temporarily removed `hpa/aggregator` so manual replica targets (`3 -> 5 -> 2 -> 3`) were not overridden during the drill.
2. Recorded baseline pod placement, `leader_leases`, and `source_fences` on `bnpb_shard_1` (`store-aqua-0`).
3. Scaled `deployment/aggregator` `3 -> 5`, waited for rollout + 10 s, and inspected leases, fences, and per-pod outbound poll logs across all 5 pods.
4. Scaled `deployment/aggregator` `5 -> 2`, waited 16 s (exceeding the 15 s ingestion lease TTL), and captured automatic lease failover and monotonic fencing-token advancement on the surviving pods.
5. Scaled `deployment/aggregator` `2 -> 3`, re-applied [`autoscaling.yaml`](../../../../k8s/base/autoscaling.yaml), and confirmed `hpa/aggregator` and `hpa/client-api` active.

## Observed Fencing & Polling Results

| Phase | `aggregator` pods | Lease holder(s) | Fencing tokens (`BMKG`, `PVMBG`, `SATELIT`, `SENSOR_PANTAI`, `SYNC`) | Active pollers (`outbound_call` in 15 s) |
| --- | --- | --- | --- | --- |
| Baseline (`3`) | `lnwrg`, `nf9ld`, `vgrrx` | `nf9ld` (4 sources), `vgrrx` (`SYNC`) | `345, 342, 7, 23, 24` | Single holder per lease |
| Scale-up (`5`) | `52hx2`, `cn4s4`, `lnwrg`, `nf9ld`, `vgrrx` | Unchanged (`nf9ld` + `vgrrx`) | `345, 342, 7, 23, 24` (unchanged) | `nf9ld`: `11` polls (`lag_events=0`); other 4 pods: `0` polls |
| Scale-down (`2`) | `lnwrg`, `vgrrx` (`nf9ld` terminated) | `lnwrg` (`BMKG`, `PVMBG`), `vgrrx` (`SATELIT`, `SENSOR_PANTAI`, `SYNC`) | `346 (+1), 343 (+1), 8 (+1), 24 (+1), 24` | `lnwrg`: `11` HTTP polls; `vgrrx`: `3` coastal polls + satellite `lag_events=0` |
| Restored (`3`) | `lnwrg`, `vgrrx`, `vslgs` | Retained on `lnwrg` + `vgrrx` | `346, 343, 8, 24, 24` | Steady exclusive polling; `hpa/aggregator` active (`42%/70%` CPU) |
