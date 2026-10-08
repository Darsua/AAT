# Problem 2 Demonstration & Evidence: Concurrency, Availability & Graceful Degradation

**Date:** 08 October 2026  
**Component Owner:** Member B (Darrel Adinarya Sunanda)  
**Target Problem:** Problem 2 (Konkurensi & Ketersediaan)

---

## 1. Overview & Acceptance Criteria
This directory records the empirical evidence for **Problem 2 (P2)**:
1. **P2-01 (Latency & Upstream Isolation):** Slow or hanging upstream data providers (e.g., PVMBG synthetic delay of 3s) do not block or degrade concurrent BMKG ingestion or Client API reads. Read requests maintain low latency (p95 < 300ms).
2. **P2-02 (Sustained High-Concurrency Load Test):** System withstands sustained load with $\ge 50$ concurrent connections over 60 seconds with error rate $< 1\%$ and p95 response time $< 300\text{ ms}$, per specification requirements.
3. **P2-03 (Graceful Degradation & Outage Handling):** Upstream outage (simulated via PVMBG `POST /admin/outage-mode` returning HTTP 503) is isolated by the Aggregator worker. Client API gracefully degrades, serving stale cached data with `meta.degraded = true` and `meta.stale_since` timestamp while BMKG queries remain fully operational.

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
3. **Outage Simulation:** Triggered `POST http://localhost:8082/admin/outage-mode` with `{"is_outage": true}`. PVMBG immediately returns HTTP 503 Service Unavailable.
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

### Scenario B: Sustained High-Concurrency Load Test (`P2-02`)
**Command:**
```bash
AUTH_INTERNAL_CLIENT_SECRET=secret123 make load-test
```
*(Runs `./scripts/load_test.js` via `grafana/k6` container or native binary)*

**Configuration:**
- **Virtual Users (VUs):** 50 concurrent connections
- **Duration:** 60 seconds sustained
- **Target Endpoint:** `GET http://localhost:8080/api/v1/hazards` with bearer token authentication
- **Acceptance Thresholds:**
  - `http_req_duration`: p(95) < 300ms, p(99) < 500ms
  - `http_req_failed` / `bnpb_error_rate`: < 1.0%

**Verification Outcome:**
- **Total Requests Handled:** >29,000 requests (~490 req/s sustained throughput)
- **p95 Latency:** ~1.75 ms (Threshold: < 300 ms — **PASS**)
- **p99 Latency:** ~4.18 ms (Threshold: < 500 ms — **PASS**)
- **Error Rate:** ~0.20% (Threshold: < 1.0% — **PASS**)
- Zero thread crashes, socket leaks, or unhandled panics observed across all containers.
