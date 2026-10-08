# Project Tracker: BNPB Disaster Coordination Platform

**Course:** IF4031 Arsitektur Aplikasi Terdistribusi (Milestone 1)  
**Target Project Completion:** **08 October 2026** (Internal target with 24h contingency buffer ahead of hard deadline)  
**Hard Submission Deadline:** **09 October 2026, 23:59 WIB**  
**Demonstration:** H+1 after deadline  
**Last Updated:** **05 October 2026**

---

## 1. Project Phase Timeline & Status

| Phase | Window | Focus / Goal | Exit Criteria | Status |
|---|---|---|---|---|
| **Phase 1: Alignment** | 21–22 Sep | Architecture decisions, service boundaries, frozen contracts, Docker Compose & CI | All ADRs, contracts, compose, and CI active on `main` | **COMPLETED ✅** |
| **Phase 2: Build** | 23–25 Sep | Independent service skeletons, database schema, auth models, broker topics | All mocks, DB, Auth, and Aggregator running | **COMPLETED ✅** |
| **Phase 3: Integration** | 28 Sep–05 Oct | Ingestion polling, canonical mapping, outbox relay, client facade, consumers | End-to-end event flow from mocks to consumers (Monday exit) | **IN PROGRESS** |
| **Phase 4: Validation** | 03–06 Oct | Automated P1–P5 test scenarios, ≥50 conn load test, in-person rehearsal | All acceptance criteria evidenced (Tuesday in-person alignment) | **IN PROGRESS** |
| **Phase 5: Closeout** | 06–08 Oct | Final PDF report, AI declaration form, clean-checkout QA, release tagging | `milestone-1` git tag pushed; report PDF ready | **PLANNED** |
| **Buffer Window** | 08–09 Oct | Final submission packaging, dry runs, and submission buffer | Zero code edits; form submitted ahead of 09 Oct 23:59 WIB deadline | **PLANNED** |

---

## 2. Team Master Task Tracker

### Phase 1: Alignment & Architecture (21–22 Sep) — COMPLETED ✅

- [x] **`M1-01`** *(All)* — **Kickoff, scope confirmation, and role confirmation** (21 Sep)  
  *Deliverable:* RACI boundary consensus, team roles assigned.
- [x] **`M1-02`** *(All)* — **Freeze service boundaries and end-to-end architecture** (21–22 Sep)  
  *Deliverable:* [`docs/adr/001-005`](../docs/adr/), service boundary definitions.
- [x] **`M1-03`** *(All)* — **Define mock HTTP contracts, HazardEvent, and event envelope** (21–22 Sep)  
  *Deliverable:* [`contracts/`](../contracts/) (6 OpenAPI & JSON Schema specifications).

---

### Phase 2: Foundation Build (23–25 Sep)

#### Member A (Aldoy) — Mock Services
- [x] **`A-01`** — **Build BMKG mock endpoints and health check** (23–24 Sep)<br>
  *Path:* [`services/mock-bmkg/`](../services/mock-bmkg/)  
  *Deliverable:* `GET /health` and `GET /seismic-events` responding on `:8081`.
- [x] **`A-02`** — **Build PVMBG mock endpoints and health check** (23–24 Sep)<br>
  *Path:* [`services/mock-pvmbg/`](../services/mock-pvmbg/)  
  *Deliverable:* `GET /health` and `GET /volcanic-reports` responding on `:8082`.
- [x] **`A-03`** — **Add seed generation, new-event generation, auth, and delay controls** (24–25 Sep)<br>
  *Deliverable:* `X-BMKG-Key` check, Bearer token check, 50–150ms BMKG latency, 500ms–3s PVMBG latency, and $\ge 1$ new event / 10s generation rate.
- [x] **`A-04`** — **Add PVMBG schema toggle and instant outage control** (25 Sep)<br>
  *Deliverable:* `POST /admin/schema-version` (`include_confidence_level`) and `POST /admin/outage` (`is_outage`).

#### Member B (Darrel) — Storage & Aggregator Core
- [x] **`B-01`** — **Create PostgreSQL schema, migrations, and volcano reference data** (23–24 Sep)  
  *Path:* [`services/aggregator/migrations/001_init.sql`](../services/aggregator/migrations/001_init.sql)  
  *Deliverable:* Canonical `hazards` table (JSONB attributes), static volcano catalog seeds (`VLC-001..006`), transactional `outbox_events` table, and `pgxpool` Go connection setup.
- [x] **`B-02`** — **Build Aggregator skeleton, configuration, health, and structured logging** (23–25 Sep)  
  *Path:* [`services/aggregator/`](../services/aggregator/)  
  *Deliverable:* Config parser from environment variables, correlation ID middleware (`X-Correlation-ID`), structured logger, and `:8083` server engine.

#### Member C (Jenka) — Authentication & Broker Provisioning
- [x] **`C-01`** — **Build Auth Service, client identities, token model, and environment config** (23–25 Sep)

  *Path:* [`services/auth/`](../services/auth/)

  *Deliverable:* `POST /auth/token` with 60s default TTL (`JWT_TTL_SECONDS`), HS256 JWT generation with `scope` claim (`hazards:read:summary` vs `hazards:read:all`), and `POST /auth/refresh` single-use token rotation with replay-driven session revocation.

  *Verification:* [`services/auth/main_test.go`](../services/auth/main_test.go) covers token claims, invalid credentials, scope escalation, refresh rotation/replay, expiry, and duplicate client IDs; `go test -race ./...` passes.
- [x] **`C-02`** — **Provision Redpanda/Kafka topic and consumer-group configuration** (23–25 Sep)

  *Path:* [`docker-compose.yml`](../docker-compose.yml)

  *Deliverable:* One-shot, idempotent `redpanda-init` provisioning for topic `bnpb.hazard-events` with 3 partitions and replication factor 1, plus configured consumer group IDs (`bnpb-dashboard-group`, `bnpb-notifier-group`). Consumer groups are created by Redpanda when their consumers first join.

---

### Phase 3: Integration & Core Pipeline (28–30 Sep)

#### Member B (Darrel) — Aggregator Pipeline & Outbox Relay
- [x] **`A-05`** — **Implement Aggregator adapters and dynamic source parsing** (28–30 Sep)  
  *Owner:* Member B (Darrel)  
  *Path:* [`services/aggregator/internal/adapter/`](../services/aggregator/internal/adapter/)  
  *Deliverable:* BMKG seismic & tsunami adapter with `X-BMKG-Key` auth; PVMBG volcanic adapter with Bearer auth, schemaless unmarshaling (`DynamicFields map[string]any`), and 503 outage detection.  
  *Verification:* [`services/aggregator/internal/adapter/adapter_test.go`](../services/aggregator/internal/adapter/adapter_test.go) passes with `-race`.
- [x] **`B-03`** — **Implement independent polling workers, cursors, overlap, and deduplication** (28–30 Sep)  
  *Owner:* Member B (Darrel)  
  *Path:* [`services/aggregator/internal/worker/poller.go`](../services/aggregator/internal/worker/poller.go)  
  *Deliverable:* Independent BMKG and PVMBG poller loops with 10s overlap window, cursor tracking, exponential backoff, and idempotent DB upsert on `(source, source_ref_id)`.  
  *Verification:* Verified live ingestion from both mock services; zero duplication across cycles.
- [x] **`B-04`** — **Implement HazardEvent mapping, severity rules, and attributes** (28–30 Sep)  
  *Owner:* Member B (Darrel)  
  *Path:* [`services/aggregator/internal/normalizer/normalizer.go`](../services/aggregator/internal/normalizer/normalizer.go)  
  *Deliverable:* Canonical mapping rules (spec §3.1–§3.3), seismic severity derivation (Awas/Siaga/Waspada override, magnitude thresholds), tsunami warning embedding, static volcano coordinate enrichment from `volcano_references`, and unmapped additive field forwarding into `attributes JSONB`.  
  *Verification:* [`services/aggregator/internal/normalizer/normalizer_test.go`](../services/aggregator/internal/normalizer/normalizer_test.go) passes with `-race`.
- [x] **`B-05`** — **Implement outbox and broker publisher** (30 Sep–01 Oct)  
  *Owner:* Member B (Darrel)  
  *Path:* [`services/aggregator/internal/repository/hazard_repo.go`](../services/aggregator/internal/repository/hazard_repo.go), [`services/aggregator/internal/worker/outbox_relay.go`](../services/aggregator/internal/worker/outbox_relay.go)  
  *Deliverable:* Atomic DB transaction committing `hazards` + `outbox_events` (`SaveHazardWithOutbox`), and background relay goroutine publishing canonical `HazardEvent` messages to Redpanda topic `bnpb.hazard-events` with `hazard_id` as key and `X-Correlation-ID` header.  
  *Verification:* [`services/aggregator/internal/worker/worker_test.go`](../services/aggregator/internal/worker/worker_test.go) passes with `-race`; verified live event fan-out in Redpanda.

#### Member C (Jenka) — Client API & Downstream Consumers
- [x] **`C-03`** — **Build read-only API, filters, pagination, and Aggregator client** (28–30 Sep)

  *Path:* [`services/client-api/`](../services/client-api/)

  *Deliverable:* `GET /api/v1/hazards` and `GET /api/v1/hazards/{id}` proxying to `aggregator:8083/internal/hazards`, with local JWT verification, Media summary-field protection, correlation-ID propagation, and support for `fields`, `since`, `type`, `severity`, `limit`, and `offset`.

  *Verification:* [`services/client-api/main_test.go`](../services/client-api/main_test.go) covers JWT rejection, Media `403`, raw/summary projection, filter forwarding, by-ID retrieval, correlation IDs, and Aggregator failure handling; `go test -race ./...` passes.
- [x] **`C-04`** — **Build Media, Field, and Internal client scripts** (29 Sep–01 Oct)

  *Deliverable:* A strict combined client verification script that enforces Media `403 Forbidden` for `fields=all`, allows Field/Internal full access, validates role-to-scope mapping, and exercises natural access-token expiration plus refresh-token rotation and replay rejection.

  *Verification:* [`scripts/test_auth_scopes.sh`](../scripts/test_auth_scopes.sh) completes the full 60-second TTL flow with zero failures; credentials are supplied through environment variables and no mock-token fallback is used.
- [x] **`C-05`** — **Implement dashboard and notification consumers with idempotency** (01 Oct)  
  *Path:* [`services/consumers/`](../services/consumers/)  
  *Deliverable:* ≥2 independent consumers with dedicated consumer groups consuming from `bnpb.hazard-events`, deduplicating events by `hazard_id`.
- [x] **`C-06`** — **Complete Dockerfiles, Compose wiring, and startup order** (29 Sep–05 Oct)
  *Deliverable:* Consumer containers start after Redpanda is healthy and topic initialization completes; consumer-group activity is verified through logs and broker offsets.

---

### Phase 4: Validation & Scenarios (03–05 Oct)

#### Member A (Aldoy) — P1 Evidence & Contract Tests
- [x] **`A-06`** — **Run live schema-evolution demo and capture evidence** (03–05 Oct)  
  *Script:* [`scripts/test_schema_change.sh`](../scripts/test_schema_change.sh)  
  *Deliverable:* Verified live schema evolution where PVMBG dynamic field `confidence_level` is ingested by Aggregator into canonical `attributes JSONB` without service restart. Output captured in [`report/evidence/p1/demo_p1.log`](../report/evidence/p1/demo_p1.log).
- [x] **`A-07`** — **Run mock contract tests and package P1 evidence** (03–05 Oct)  
  *Deliverable:* P1 test logs and evidence packaged; unit and contract tests for mock-bmkg and mock-pvmbg 100% passing ([`report/evidence/p1/mock_contract_tests.log`](../report/evidence/p1/mock_contract_tests.log)).

#### Member B (Darrel) — P2 & P4 Evidence & Load Testing
- [x] **`B-06`** — **Run latency, outage, recovery, and stale-data scenarios** (30 Sep–01 Oct)  
  *Script:* [`scripts/test_outage.sh`](../scripts/test_outage.sh)  
  *Deliverable:* Verified degraded state (`meta.degraded = true`, `meta.stale_since`) during PVMBG outage while BMKG remains 100% operational; automatic recovery to healthy state. Output captured in [`report/evidence/p2/demo_p2.log`](../report/evidence/p2/demo_p2.log).
- [x] **`B-07`** — **Test independent restart and storage ownership** (30 Sep–01 Oct)  
  *Script:* [`scripts/test_storage_restart.sh`](../scripts/test_storage_restart.sh)  
  *Deliverable:* Verified zero cascading failure when `aggregator` or `canonical-db` restarts; automatic connection pool recovery and self-healing. Output captured in [`report/evidence/p4/storage_restart.log`](../report/evidence/p4/storage_restart.log).
- [x] **`B-08`** — **Run sustained load test with ≥50 parallel connections for 60 seconds** (01 Oct)  
  *Script:* [`scripts/load_test.js`](../scripts/load_test.js)  
  *Deliverable:* k6 load test results: 50 VUs, 60s duration, 29,514 requests (~491 req/s), p95 latency = 1.75ms (threshold < 300ms), p99 = 3.07ms, error rate = 0.43% (threshold < 1%). Output captured in [`report/evidence/p2/load_test.log`](../report/evidence/p2/load_test.log).

#### Member C (Jenka) — P3 & P5 Evidence & Observability
- [x] **`C-07`** — **Run cross-credential, scope, refresh, and secret checks** (03–05 Oct)
  *Scripts:* [`scripts/test_cross_credential.sh`](../scripts/test_cross_credential.sh) and [`scripts/test_auth_scopes.sh`](../scripts/test_auth_scopes.sh)  
  *Deliverable:* Verified BMKG/PVMBG cross-credential `401` rejection, Media raw-field `403`, natural 60s access-token expiration, refresh rotation, and replay rejection.
- [x] **`C-08`** — **Run fan-out, consumer-stop, recovery, and new-consumer scenarios** (03–05 Oct)
  *Script:* [`scripts/test_consumer_recovery.sh`](../scripts/test_consumer_recovery.sh)  
  *Deliverable:* Verified identical fan-out, lag accumulation during downtime, committed-offset recovery to zero lag, and a dynamic 3rd consumer without publisher modification.
- [x] **`C-09`** — **Audit correlation IDs, latency logs, health checks, and metrics** (03–05 Oct)
  *Script:* [`scripts/test_observability.sh`](../scripts/test_observability.sh)
  *Deliverable:* Verified Client API → Aggregator correlation propagation and HTTP latency logs; a directly produced Kafka test event verifies correlation headers at both consumers. Also verifies dependency health and consumer-group lag metrics. This script does not claim to test the Aggregator outbox path.

#### Joint Validation (All)
- [ ] **`ALL-01`** — **Integrated rehearsal of P1–P5 from clean Compose startup** (06 Oct, In-person on campus)  
  *Deliverable:* Rehearsal execution of all 5 demonstration scripts in sequence (`make demo-p1`, `demo-p2`, `demo-p3`, `demo-p5`, `load-test`) and cross-service oral defense review.

---

### Phase 5: Closeout & Submission Prep (06–08 Oct)

- [ ] **`A-08`** *(Aldoy)* — **Write P1 and mock-service sections with screenshots/logs** (06–07 Oct)  
  *Deliverable:* Drafted report sections for Problem 1 and mock architectures.
- [ ] **`B-09`** *(Darrel)* — **Write P2/P4 sections with load and storage evidence** (03–07 Oct)  
  *Deliverable:* 3-way storage comparative evaluation (PostgreSQL+JSONB vs MongoDB vs Strict SQL), concurrency analysis, and k6 benchmark graphs.
- [ ] **`C-10`** *(Jenka)* — **Write P3/P5 sections with auth and broker evidence** (06–07 Oct)  
  *Deliverable:* RBAC security design, token lifecycle documentation, and pub-sub recovery architecture.
- [ ] **`ALL-02`** *(All)* — **Finalize README, runbook, .env.example, and AI declaration** (07–08 Oct)  
  *Deliverable:* Root [`README.md`](../README.md) verified against rubric; signed ITB AI Declaration Form attached.
- [ ] **`ALL-03`** *(All)* — **Clean-checkout QA, final commit, and milestone-1 tag** (08 Oct)  
  *Deliverable:* Pristine `git checkout` test in clean directory; tag `milestone-1` pushed to GitHub.
- [ ] **`ALL-04`** *(All)* — **Prepare final submission package and handoff with one-day buffer** (08–09 Oct)  
  *Deliverable:* Final PDF report, GitHub repository link, and submission form ready 24 hours ahead of hard deadline.

---

## 3. Demonstration & Evidence Matrix (P1–P5)

Use this matrix to track and link evidence artifacts for the final report:

| ID | Problem | Scenario | Test Script / Command | Expected Evidence | Owner | Status | Evidence Location |
|---|---|---|---|---|---|---|---|
| **`P1-01`** | P1 | Baseline PVMBG Schema | `curl :8082/volcanic-reports` | Schema without `confidence_level` produces canonical `HazardEvent` | Aldoy | Verified ✅ | [`report/evidence/p1/demo_p1.log`](../report/evidence/p1/demo_p1.log) |
| **`P1-02`** | P1 | Live Schema Evolution | `POST :8082/admin/schema-version` | `confidence_level` appears dynamically; Aggregator captures it in `attributes` | Aldoy | Verified ✅ | [`report/evidence/p1/demo_p1.log`](../report/evidence/p1/demo_p1.log) |
| **`P1-03`** | P1 | Unknown Field Policy | Compare before/after DB attributes | Dynamic fields forwarded without crash or schema migration | Aldoy | Verified ✅ | [`report/evidence/p1/demo_p1.log`](../report/evidence/p1/demo_p1.log) |
| **`P2-01`** | P2 | Latency Isolation | PVMBG delay = 3s; query BMKG | BMKG queries respond with p95 < 300ms regardless of PVMBG slowness | Darrel | Verified ✅ | [`report/evidence/p2/`](../report/evidence/p2/) |
| **`P2-02`** | P2 | Sustained Load Test | `make load-test` (k6) | 50 VUs, 60s sustained, error rate < 1%, p95 < 300ms (p95=1.75ms, 491 req/s) | Darrel | Verified ✅ | [`report/evidence/p2/load_test.log`](../report/evidence/p2/load_test.log) |
| **`P2-03`** | P2 | PVMBG Outage Simulation | `make demo-p2` | BMKG stays normal; PVMBG flagged as stale (`meta.degraded = true`) | Darrel | Verified ✅ | [`report/evidence/p2/demo_p2.log`](../report/evidence/p2/demo_p2.log) |
| **`P3-01`** | P3 | Cross-Domain Credential Rejection | `scripts/test_cross_credential.sh` | BMKG key on PVMBG returns 401/403; PVMBG token on BMKG returns 401/403 | Jenka | Planned | `report/evidence/p3/` |
| **`P3-02`** | P3 | Media Authorization (RBAC) | `scripts/test_auth_scopes.sh` | Summary fields return 200; `fields=all` strictly rejected with 403 Forbidden | Jenka | Planned | `report/evidence/p3/` |
| **`P3-03`** | P3 | Natural 60s TTL & Refresh Rotation | `scripts/test_auth_scopes.sh` | Expired token returns 401; refresh returns new token; old refresh token burned | Jenka | Planned | `report/evidence/p3/` |
| **`P4-01`** | P4 | Independent Container Restart | `make demo-p4` | Unrelated services remain functional; clean reconnect to database | Darrel | Verified ✅ | [`report/evidence/p4/storage_restart.log`](../report/evidence/p4/storage_restart.log) |
| **`P4-02`** | P4 | Flexible Canonical Storage | `make demo-p4` | Records before and after schema evolution coexist seamlessly | Darrel | Verified ✅ | [`report/evidence/p4/storage_restart.log`](../report/evidence/p4/storage_restart.log) |
| **`P5-01`** | P5 | Pub-Sub Fan-Out | Publish event to Redpanda | Both `dashboard` and `notifier` consumer groups receive identical event | Jenka | Planned | `report/evidence/p5/` |
| **`P5-02`** | P5 | Consumer Outage | Stop one consumer, publish events | Publisher and surviving consumer continue processing normally | Jenka | Planned | `report/evidence/p5/` |
| **`P5-03`** | P5 | Consumer Downtime Recovery | Restart stopped consumer | Resumes from last committed checkpoint and consumes missed backlog | Jenka | Planned | `report/evidence/p5/` |
| **`P5-04`** | P5 | Dynamic 3rd Consumer Addition | Spin up new consumer group | New consumer replays stream without publisher reconfiguration or restart | Jenka | Planned | `report/evidence/p5/` |

---

## 4. Operational Rules

1. **Adjusted Phase Roadmap & Milestones:** Phase 3 wraps up remotely on Monday 05 October once downstream consumers are integrated. Joint in-person validation and rehearsal (`ALL-01`) takes place Tuesday 06 October on campus. Report consolidation occurs 06–08 October, preserving an 08–09 October contingency buffer before the hard submission deadline on 09 October 2026, 23:59 WIB.
2. **Strict RACI Boundaries:** Do not write code or business logic in another member's assigned services.
3. **No Shared Go Packages:** Every service must be 100% self-contained (`pkg/` and `common/` are forbidden).
4. **Git Branch & PR Workflow:** Work on feature branches, ensure `make test` and Docker builds pass, and merge into `main` via reviewed Pull Requests.
