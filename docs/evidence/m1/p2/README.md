# Problem 2: Concurrency and graceful degradation — historical M1 evidence

**Recorded date: 2026-10-08.** These M1 results describe the retained runs, not current validation, M2 capacity, or multi-node availability. Nothing was rerun for this documentation cleanup.

## Outage run

[`demo_p2.log`](demo_p2.log), from [`test_outage.sh`](../../../../scripts/test_outage.sh), records baseline PVMBG HTTP 200, an outage toggle causing HTTP 503, and gateway HTTP 200 with `meta.degraded=true` and `stale_since=2026-10-08T09:45:21.5830728Z` (sample total: 1,562 records). BMKG returned HTTP 200. After the outage toggle was cleared, the script reported recovery and cleared degradation.

Reads use persisted canonical data, not a gateway cache. These sampled checks show outage handling; they do not measure uninterrupted ingestion or availability throughout the interval.

## Load run with fixed 3-second PVMBG delay

[`load_test.log`](load_test.log) records [`load_test.js`](../../../../scripts/load_test.js), invoked through `make load-test` with `PVMBG_DELAY_MIN_MS=3000` and `PVMBG_DELAY_MAX_MS=3000` applied to the mock. Credentials came from environment configuration. The recorded runner was k6 v2.3.0.

Three scenarios ran concurrently for 60 seconds: 25 gateway VUs requesting `type=SEISMIC`, 25 requesting `type=VOLCANIC`, and 5 directly exercising slow PVMBG. Thus gateway concurrency was **50 VUs**, not 55. Gateway VUs obtained individual tokens and renewed every 40 seconds against a 60-second TTL.

| Recorded metric | Result |
| :--- | :--- |
| All HTTP requests | 29,434, about 474 requests/s |
| Gateway requests | 29,234; all HTTP 200, no 429 or other statuses |
| BMKG-only latency | p50 1.72 ms; p95 3.43 ms; p99 6.48 ms |
| Volcanic latency | p50 1.72 ms; p95 3.42 ms; p99 6.72 ms |
| Direct PVMBG latency | 3 s minimum and maximum, confirming active delay |
| Gateway error rate | 0/29,234 (0.00%) |
| `http_req_failed` | 0/29,434 (0.00%) |

The recorded p95 < 300 ms and error-rate < 1% thresholds passed. Canonical reads were decoupled from the delayed provider for this workload ([ADR 004](../../../adr/004-internal-communication-rest.md)); this is not a 500-connection, saturation, or failover benchmark.

**Earlier run:** one token from `setup()` was reused for the full run; 128/29,514 responses failed (0.43%), with p95 1.75 ms and p99 3.07 ms. Expiry at the 60-second boundary is an **inferred** cause because that run did not record status codes. Failures disappeared after per-VU renewal; the retained load log supports the later results above, not a new rerun.
