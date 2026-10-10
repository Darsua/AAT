# Problem 2 Demonstration & Evidence: Concurrency, Availability & Graceful Degradation

**Date:** 08 October 2026  
**Component Owner:** Member B (Darrel Adinarya Sunanda)  
**Target Problem:** Problem 2 (Konkurensi & Ketersediaan)

---

## 1. Overview & Acceptance Criteria
This directory records the empirical evidence for **Problem 2 (P2)**:
1. **P2-01 (Latency & Upstream Isolation):** Slow or hanging upstream data providers (e.g., PVMBG synthetic delay of up to 3 s; random 500 ms to 3 s by default, configurable with `PVMBG_DELAY_MIN_MS` / `PVMBG_DELAY_MAX_MS`) do not block or degrade concurrent BMKG ingestion or Client API reads. Read requests maintain low latency (p95 < 300ms).
2. **P2-02 (Sustained High-Concurrency Load Test):** System withstands sustained load with $\ge 50$ concurrent connections over 60 seconds with error rate $< 1\%$ and p95 response time $< 300\text{ ms}$, per specification requirements.
3. **P2-03 (Graceful Degradation & Outage Handling):** Upstream outage (simulated via PVMBG `POST /admin/outage` returning HTTP 503) is isolated by the Aggregator worker. Client API gracefully degrades, serving stale cached data with `meta.degraded = true` and `meta.stale_since` timestamp while BMKG queries remain fully operational.

---

## 2. Evidence Files
- [`demo_p2.log`](./demo_p2.log): Full execution trace of `./scripts/test_outage.sh` verifying baseline operation, PVMBG outage simulation (HTTP 503), graceful degradation response with `meta.degraded=true` and `stale_since`, BMKG provider isolation, and automatic self-healing recovery.
- [`load_test.log`](./load_test.log): k6 sustained high-concurrency load test execution trace with 50 Virtual Users (VUs) over 60 seconds against `http://localhost:8080/api/v1/hazards`.

---

## 3. Test Execution Summary & Verification

### Scenario A: Upstream Outage Simulation & Degraded Handling (`P2-03`)
**Command:**
```bash
./scripts/test_outage.sh
```

**Key Execution Stages:**
1. **Upstream Health Check:** Verified `mock-pvmbg` is healthy on `:8082`.
2. **Baseline Verification:** PVMBG `GET /volcanic-reports` returns HTTP 200.
3. **Outage Simulation:** Triggered `POST http://localhost:8082/admin/outage` with `{"is_outage": true}`. PVMBG immediately returns HTTP 503 Service Unavailable.
4. **Resilience & Degraded Response:** Aggregator detects consecutive failures and marks PVMBG tracking as degraded. Client API (`GET /api/v1/hazards`) continues serving requests without crashing (HTTP 200) with payload metadata:
   ```json
   {
     "meta": {
       "total": 1562,
       "limit": 5,
       "offset": 0,
       "degraded": true,
       "stale_since": "2026-10-08T09:45:21.5830728Z"
     }
   }
   ```
5. **Provider Isolation:** Queried `mock-bmkg` on `:8081`; returned HTTP 200. BMKG ingestion pipeline continues polling and upserting without interruption.
6. **Automatic Self-Healing:** Restored PVMBG via `{"is_outage": false}`. Poller automatically resumes, clears degraded state, and updates fresh records.

---

### Scenario B: Sustained Load Test with a 3 s PVMBG (`P2-01`, `P2-02`)
**Setup (fixed 3 s PVMBG delay) and command:**
```bash
PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 docker compose up -d mock-pvmbg
AUTH_INTERNAL_CLIENT_SECRET=internal-secret-2026 PVMBG_TOKEN=pvmbg-bearer-token-2026 make load-test
```
*(Runs `./scripts/load_test.js` via the `grafana/k6` container, k6 v2.3.0, or a native binary)*

**Configuration:** three scenarios run at the same time for 60 s
- `bmkg_only`: 25 VUs, `GET /api/v1/hazards?type=SEISMIC` (needs BMKG data only).
- `volcanic`: 25 VUs, `GET /api/v1/hazards?type=VOLCANIC` (needs PVMBG data).
- `pvmbg_direct`: 5 VUs, `GET mock-pvmbg:8082/volcanic-reports`, which keeps the slow upstream busy (each call takes 3 s).
- Gateway concurrency is 50 VUs. Each VU requests its own access token and renews it every 40 s, because the token TTL is 60 s.
- Thresholds: `bnpb_bmkg_only_latency_ms` p(95) < 300 ms, `bnpb_volcanic_latency_ms` p(95) < 300 ms, `bnpb_error_rate` < 1%, `http_req_failed` < 1%.

**Verification Outcome** (`load_test.log`):
- **Requests:** 29,434 HTTP requests (about 474 req/s), of which 29,234 were gateway requests (HTTP 200: 29,234; 429: 0; other statuses: 0).
- **BMKG-only latency:** p50 1.72 ms, p95 3.43 ms, p99 6.48 ms (Threshold: p95 < 300 ms, **PASS**).
- **Volcanic latency:** p50 1.72 ms, p95 3.42 ms, p99 6.72 ms (**PASS**).
- **PVMBG direct latency:** 3 s for every request (min = max = 3 s), which confirms the delay was active during the test.
- **Error rate:** 0.00% (0 of 29,234; `http_req_failed` 0 of 29,434) (Threshold: < 1.0%, **PASS**).
- The gateway reads from the Canonical Store, so a slow PVMBG does not affect gateway latency in this architecture.

**Earlier run and the 0.43% error rate:** the previous version of the script obtained one access token in `setup()` and used it for the whole run. The token TTL is 60 s, so requests at the very end of the run were rejected (128 of 29,514 responses, 0.43%). That run did not record status codes, so this cause is inferred; the failures disappeared after tokens were renewed per VU. The previous figures (p95 1.75 ms, p99 3.07 ms) are replaced by the results above.
