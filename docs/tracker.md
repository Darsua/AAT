# Project Tracker: BNPB Disaster Coordination Platform

**Course:** IF4031 Arsitektur Aplikasi Terdistribusi (Milestone 1)  
**Target Project Completion:** **02 October 2026** (Internal target with 7-day contingency buffer)  
**Hard Submission Deadline:** **09 October 2026, 23:59 WIB**  
**Demonstration:** H+1 after deadline  

---

## 1. Project Phase Timeline & Status

| Phase | Window | Focus / Goal | Exit Criteria | Status |
|---|---|---|---|---|
| **Phase 1: Alignment** | 21–22 Sep | Architecture decisions, service boundaries, frozen contracts, Docker Compose & CI | All ADRs, contracts, compose, and CI active on `main` | **COMPLETED ✅** |
| **Phase 2: Build** | 23–25 Sep | Independent service skeletons, database schema, auth models, broker topics | All mocks, DB, Auth, and Aggregator running | **IN PROGRESS** |
| **Phase 3: Integration** | 28–30 Sep | Ingestion polling, canonical mapping, outbox relay, client facade, consumers | End-to-end event flow from mocks to consumers | **NOT STARTED** |
| **Phase 4: Validation** | 30 Sep–01 Oct | Automated P1–P5 test scenarios, ≥50 conn load test, failure injection | All acceptance criteria evidenced | **NOT STARTED** |
| **Phase 5: Closeout** | 01–02 Oct | Final PDF report, AI declaration form, clean-checkout QA, release tagging | `milestone-1` git tag pushed; report PDF ready | **NOT STARTED** |
| **Buffer Period** | 03–08 Oct | 7-day contingency buffer for rehearsal, edge-case bugfixes, and grading prep | Zero new feature code; dry runs only | **NOT STARTED** |

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
- [ ] **`A-01`** — **Build BMKG mock endpoints and health check** (23–24 Sep)  
  *Path:* [`services/mock-bmkg/`](../services/mock-bmkg/)  
  *Deliverable:* `GET /health` and `GET /seismic-events` responding on `:8081`.
- [ ] **`A-02`** — **Build PVMBG mock endpoints and health check** (23–24 Sep)  
  *Path:* [`services/mock-pvmbg/`](../services/mock-pvmbg/)  
  *Deliverable:* `GET /health` and `GET /volcanic-reports` responding on `:8082`.
- [ ] **`A-03`** — **Add seed generation, new-event generation, auth, and delay controls** (24–25 Sep)  
  *Deliverable:* `X-BMKG-Key` check, Bearer token check, 50–150ms BMKG latency, 500ms–3s PVMBG latency, and $\ge 1$ new event / 10s generation rate.
- [ ] **`A-04`** — **Add PVMBG schema toggle and instant outage control** (25 Sep)  
  *Deliverable:* `POST /admin/schema-version` (`include_confidence_level`) and `POST /admin/outage` (`is_outage`).

#### Member B (Darrel) — Storage & Aggregator Core
- [ ] **`B-01`** — **Create PostgreSQL schema, migrations, and volcano reference data** (23–24 Sep)  
  *Path:* [`services/aggregator/migrations/001_init.sql`](../services/aggregator/migrations/001_init.sql)  
  *Deliverable:* Canonical `hazards` table (JSONB attributes), static volcano catalog seeds (`VLC-001..006`), transactional `outbox_events` table, and `pgxpool` Go connection setup.
- [ ] **`B-02`** — **Build Aggregator skeleton, configuration, health, and structured logging** (23–25 Sep)  
  *Path:* [`services/aggregator/`](../services/aggregator/)  
  *Deliverable:* Config parser from environment variables, correlation ID middleware (`X-Correlation-ID`), structured logger, and `:8083` server engine.

#### Member C (Jenka) — Authentication & Broker Provisioning
- [ ] **`C-01`** — **Build Auth Service, client identities, token model, and environment config** (23–25 Sep)  
  *Path:* [`services/auth/`](../services/auth/)  
  *Deliverable:* `POST /auth/token` with 60s TTL (`JWT_TTL_SECONDS`), HS256 JWT generation with `scope` claim (`hazards:read:summary` vs `hazards:read:all`), and `POST /auth/refresh` single-use token rotation.
- [ ] **`C-02`** — **Provision Redpanda/Kafka topic and consumer-group configuration** (23–25 Sep)  
  *Path:* [`docker-compose.yml`](../docker-compose.yml)  
  *Deliverable:* Verified topic `bnpb.hazard-events` with 3 partitions and consumer groups (`bnpb-dashboard-group`, `bnpb-notifier-group`).

---

### Phase 3: Integration & Core Pipeline (28–30 Sep)

#### Member A (Aldoy) — Upstream Data Adapters
- [ ] **`A-05`** — **Build BMKG/PVMBG adapters and dynamic source parsing** (28–30 Sep)  
  *Deliverable:* Stable upstream data delivery under continuous polling without panics.

#### Member B (Darrel) — Aggregator Pipeline & Outbox Relay
- [ ] **`B-03`** — **Implement independent polling workers, cursors, overlap, and deduplication** (28–30 Sep)  
  *Deliverable:* 2–5s polling loops for BMKG and PVMBG with `since` query parameter, error backoff, and idempotent DB upsert on `(source, source_ref_id)`.
- [ ] **`B-04`** — **Implement HazardEvent mapping, severity rules, and attributes** (28–30 Sep)  
  *Deliverable:* Canonical mapping rules (spec §3.1–§3.3), static volcano coordinate enrichment, tsunami correlation, and additive unmapped field forwarding to `attributes JSONB`.
- [ ] **`B-05`** — **Implement outbox and broker publisher** (30 Sep–01 Oct)  
  *Deliverable:* Atomic DB transaction committing `hazards` + `outbox_events`, and background relay goroutine publishing canonical `HazardEvent` messages to Redpanda (`hazard_id` as key).

#### Member C (Jenka) — Client API & Downstream Consumers
- [ ] **`C-03`** — **Build read-only API, filters, pagination, and Aggregator client** (28–30 Sep)  
  *Path:* [`services/client-api/`](../services/client-api/)  
  *Deliverable:* `GET /api/v1/hazards` proxying to `aggregator:8083/internal/hazards`, supporting `fields`, `since`, `type`, `severity`, `limit`, and `offset`.
- [ ] **`C-04`** — **Build Media, Field, and Internal client scripts** (29 Sep–01 Oct)  
  *Deliverable:* Enforce Media 403 Forbidden when requesting raw fields or `fields=all`. Allow Field/Internal full access.
- [ ] **`C-05`** — **Implement dashboard and notification consumers with idempotency** (01 Oct)  
  *Path:* [`services/consumers/`](../services/consumers/)  
  *Deliverable:* ≥2 independent consumers with dedicated consumer groups consuming from `bnpb.hazard-events`, deduplicating events by `hazard_id`.
- [ ] **`C-06`** — **Complete Dockerfiles, Compose wiring, health checks, and startup order** (29 Sep–01 Oct)  
  *Deliverable:* Full multi-service container startup order verified via health checks.

---

### Phase 4: Validation & Scenarios (30 Sep–01 Oct)

#### Member A (Aldoy) — P1 Evidence & Contract Tests
- [ ] **`A-06`** — **Run live schema-evolution demo and capture evidence** (30 Sep–01 Oct)  
  *Script:* [`scripts/test_schema_change.sh`](../scripts/test_schema_change.sh)  
  *Deliverable:* Evidence showing `confidence_level` dynamically ingested without restart.
- [ ] **`A-07`** — **Run mock contract tests and package P1 evidence** (01 Oct)  
  *Deliverable:* P1 test logs and screenshots saved in evidence folder.

#### Member B (Darrel) — P2 & P4 Evidence & Load Testing
- [ ] **`B-06`** — **Run latency, outage, recovery, and stale-data scenarios** (30 Sep–01 Oct)  
  *Script:* [`scripts/test_outage.sh`](../scripts/test_outage.sh)  
  *Deliverable:* Evidence of degraded state (`meta.degraded = true`, `meta.stale_since`) during PVMBG outage while BMKG remains 100% operational.
- [ ] **`B-07`** — **Test independent restart and storage ownership** (30 Sep–01 Oct)  
  *Deliverable:* Zero cascading failure when `aggregator` or `canonical-db` restarts.
- [ ] **`B-08`** — **Run sustained load test with ≥50 parallel connections for 60 seconds** (01 Oct)  
  *Script:* [`scripts/load_test.js`](../scripts/load_test.js)  
  *Deliverable:* k6 summary metrics: 50 VUs, 60s duration, p95 latency < 300ms, error rate < 1% (reporting 429s separately).

#### Member C (Jenka) — P3 & P5 Evidence & Observability
- [ ] **`C-07`** — **Run cross-credential, scope, refresh, and secret checks** (30 Sep–01 Oct)  
  *Scripts:* [`scripts/test_cross_credential.sh`](../scripts/test_cross_credential.sh) and [`scripts/test_auth_scopes.sh`](../scripts/test_auth_scopes.sh)  
  *Deliverable:* Evidence of BMKG/PVMBG cross-credential 401/403 rejection, Media 403 raw field blocking, 60s natural TTL expiration, and refresh token rotation.
- [ ] **`C-08`** — **Run fan-out, consumer-stop, recovery, and new-consumer scenarios** (30 Sep–01 Oct)  
  *Script:* [`scripts/test_consumer_recovery.sh`](../scripts/test_consumer_recovery.sh)  
  *Deliverable:* Evidence of identical fan-out, consumer downtime offset recovery, and dynamic 3rd consumer joining without publisher modification.
- [ ] **`C-09`** — **Audit correlation IDs, latency logs, health checks, and metrics** (01 Oct)  
  *Deliverable:* Full log trail proving end-to-end `X-Correlation-ID` propagation.

#### Joint Validation (All)
- [ ] **`ALL-01`** — **Integrated rehearsal of P1–P5 from clean Compose startup** (01 Oct)  
  *Deliverable:* Rehearsal execution of all 5 demonstration scripts in sequence (`make demo-p1`, `demo-p2`, `demo-p3`, `demo-p5`, `load-test`).

---

### Phase 5: Closeout & Submission Prep (01–02 Oct)

- [ ] **`A-08`** *(Aldoy)* — **Write P1 and mock-service sections with screenshots/logs** (01–02 Oct)  
  *Deliverable:* Drafted report sections for Problem 1 and mock architectures.
- [ ] **`B-09`** *(Darrel)* — **Write P2/P4 sections with load and storage evidence** (01–02 Oct)  
  *Deliverable:* 3-way storage comparative evaluation (PostgreSQL+JSONB vs MongoDB vs Strict SQL), concurrency analysis, and k6 benchmark graphs.
- [ ] **`C-10`** *(Jenka)* — **Write P3/P5 sections with auth and broker evidence** (01–02 Oct)  
  *Deliverable:* RBAC security design, token lifecycle documentation, and pub-sub recovery architecture.
- [ ] **`ALL-02`** *(All)* — **Finalize README, runbook, .env.example, and AI declaration** (02 Oct)  
  *Deliverable:* Root [`README.md`](../README.md) verified against rubric; signed ITB AI Declaration Form attached.
- [ ] **`ALL-03`** *(All)* — **Clean-checkout QA, final commit, and milestone-1 tag** (02 Oct)  
  *Deliverable:* Pristine `git checkout` test in clean directory; tag `milestone-1` pushed to GitHub.
- [ ] **`ALL-04`** *(All)* — **Prepare final submission package and handoff with one-week buffer** (02 Oct)  
  *Deliverable:* Final PDF report, GitHub repository link, and submission form ready 7 days ahead of deadline.

---

## 3. Demonstration & Evidence Matrix (P1–P5)

Use this matrix to track and link evidence artifacts for the final report:

| ID | Problem | Scenario | Test Script / Command | Expected Evidence | Owner | Status | Evidence Location |
|---|---|---|---|---|---|---|---|
| **`P1-01`** | P1 | Baseline PVMBG Schema | `curl :8082/volcanic-reports` | Schema without `confidence_level` produces canonical `HazardEvent` | Aldoy | Planned | `report/evidence/p1/` |
| **`P1-02`** | P1 | Live Schema Evolution | `POST :8082/admin/schema-version` | `confidence_level` appears dynamically; Aggregator captures it in `attributes` | Aldoy | Planned | `report/evidence/p1/` |
| **`P1-03`** | P1 | Unknown Field Policy | Compare before/after DB attributes | Dynamic fields forwarded without crash or schema migration | Aldoy | Planned | `report/evidence/p1/` |
| **`P2-01`** | P2 | Latency Isolation | PVMBG delay = 3s; query BMKG | BMKG queries respond with p95 < 300ms regardless of PVMBG slowness | Darrel | Planned | `report/evidence/p2/` |
| **`P2-02`** | P2 | Sustained Load Test | `make load-test` (k6) | 50 VUs, 60s sustained, error rate < 1%, p95 < 300ms | Darrel | Planned | `report/evidence/p2/` |
| **`P2-03`** | P2 | PVMBG Outage Simulation | `make demo-p2` | BMKG stays normal; PVMBG flagged as stale (`meta.degraded = true`) | Darrel | Planned | `report/evidence/p2/` |
| **`P3-01`** | P3 | Cross-Domain Credential Rejection | `scripts/test_cross_credential.sh` | BMKG key on PVMBG returns 401/403; PVMBG token on BMKG returns 401/403 | Jenka | Planned | `report/evidence/p3/` |
| **`P3-02`** | P3 | Media Authorization (RBAC) | `scripts/test_auth_scopes.sh` | Summary fields return 200; `fields=all` strictly rejected with 403 Forbidden | Jenka | Planned | `report/evidence/p3/` |
| **`P3-03`** | P3 | Natural 60s TTL & Refresh Rotation | `scripts/test_auth_scopes.sh` | Expired token returns 401; refresh returns new token; old refresh token burned | Jenka | Planned | `report/evidence/p3/` |
| **`P4-01`** | P4 | Independent Container Restart | `docker compose restart aggregator` | Unrelated services remain functional; clean reconnect to database | Darrel | Planned | `report/evidence/p4/` |
| **`P4-02`** | P4 | Flexible Canonical Storage | Query DB hazards table | Records before and after schema evolution coexist seamlessly | Darrel | Planned | `report/evidence/p4/` |
| **`P5-01`** | P5 | Pub-Sub Fan-Out | Publish event to Redpanda | Both `dashboard` and `notifier` consumer groups receive identical event | Jenka | Planned | `report/evidence/p5/` |
| **`P5-02`** | P5 | Consumer Outage | Stop one consumer, publish events | Publisher and surviving consumer continue processing normally | Jenka | Planned | `report/evidence/p5/` |
| **`P5-03`** | P5 | Consumer Downtime Recovery | Restart stopped consumer | Resumes from last committed checkpoint and consumes missed backlog | Jenka | Planned | `report/evidence/p5/` |
| **`P5-04`** | P5 | Dynamic 3rd Consumer Addition | Spin up new consumer group | New consumer replays stream without publisher reconfiguration or restart | Jenka | Planned | `report/evidence/p5/` |

---

## 4. Operational Rules

1. **Target Completion Date is 02 October 2026:** All feature code, integration, tests, and evidence capture must finish by 02 October. The 03–08 October window is strictly a contingency buffer for report polishing, rehearsals, and bugfixes.
2. **Strict RACI Boundaries:** Do not write code or business logic in another member's assigned services.
3. **No Shared Go Packages:** Every service must be 100% self-contained (`pkg/` and `common/` are forbidden).
4. **Git Branch & PR Workflow:** Work on feature branches, ensure `make test` and Docker builds pass, and merge into `main` via reviewed Pull Requests.
