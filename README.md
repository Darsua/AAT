# BNPB Disaster Coordination Platform (Platform Koordinasi Kebencanaan BNPB)
### IF4031 Arsitektur Aplikasi Terdistribusi — Milestone 1 (2026)

[![Go](https://img.shields.io/badge/Go-1.22-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2-2496ED?style=flat&logo=docker)](https://docs.docker.com/compose/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%20Alpine-4169E1?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![Redpanda](https://img.shields.io/badge/Redpanda-Kafka%20API-FF2C33?style=flat&logo=redpanda)](https://redpanda.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An enterprise-grade, resilient, event-driven disaster early warning and coordination platform designed for the Indonesian National Board for Disaster Management (**BNPB** — *Badan Nasional Penanggulangan Bencana*). 

The platform continuously ingests, harmonizes, enriches, and broadcasts critical hazard telemetry from external national monitoring agencies (**BMKG** for seismic/tsunami events and **PVMBG** for volcanic activities), persists canonical hazard events with transactional outbox guarantees, and delivers scope-governed disaster intelligence to downstream emergency operations, media, and regional responders.

---

## Table of Contents
- [1. Engineering Team & Ownership Matrix](#1-engineering-team--ownership-matrix)
- [2. Architecture Overview](#2-architecture-overview)
- [3. Port Mapping & Service Directory](#3-port-mapping--service-directory)
- [4. Architecture Decision Records (ADRs)](#4-architecture-decision-records-adrs)
- [5. Frozen Data Contracts](#5-frozen-data-contracts)
- [6. System Prerequisites](#6-system-prerequisites)
- [7. Quickstart Guide](#7-quickstart-guide)
- [8. Demonstration & Grading Runbook](#8-demonstration--grading-runbook)
  - [Evaluator Quick Verification Matrix](#evaluator-quick-verification-matrix)
  - [Problem 1 (P1): Dynamic Live Schema Evolution](#problem-1-p1-dynamic-live-schema-evolution)
  - [Problem 2 (P2): Upstream Outage Simulation & 50-Connection Load Test](#problem-2-p2-upstream-outage-simulation--50-connection-load-test)
  - [Problem 3 (P3): Scoped Access Control, Media 403 & 60s TTL Token Refresh](#problem-3-p3-scoped-access-control-media-403--60s-ttl-token-refresh)
  - [Problem 4 (P4): Independent Container Rebuild & Restart Resilience](#problem-4-p4-independent-container-rebuild--restart-resilience)
  - [Problem 5 (P5): Pub-Sub Fan-Out, Consumer Downtime Recovery & Dynamic 3rd Consumer](#problem-5-p5-pub-sub-fan-out-consumer-downtime-recovery--dynamic-3rd-consumer)
- [9. Configuration Reference (`.env.example`)](#9-configuration-reference-envexample)
- [10. Project Management & Service Guides](#10-project-management--service-guides)

---

## 1. Engineering Team & Ownership Matrix

| Member | Student ID (NIM) | Role & Responsibility Boundaries | Assigned Scope |
| :--- | :--- | :--- | :--- |
| **Darrel Adinarya Sunanda** | `13523061` | **Member B (Aggregator & Canonical Persistence)**<br/>• `services/aggregator`: Ingestion workers, canonical schema harmonization, static volcano catalog enrichment, and internal REST API.<br/>• `canonical-db`: PostgreSQL 16 schema with `attributes JSONB` & Transactional Outbox. | **Problem 2** (Graceful Degradation)<br/>**Problem 4** (Fault Isolation & Rebuilds) |
| **Jennifer Khang** | `13524110` | **Member C (Auth, Client Gateway & Pub-Sub)**<br/>• `services/auth`: Token issuance, scope management, and 60-second TTL refresh rotation.<br/>• `services/client-api`: Public API gateway, scope projection (Media vs. Field), and correlation tracing.<br/>• `services/consumers`: Independent consumer groups (`dashboard`, `notifier`). | **Problem 3** (RBAC & Token Refresh)<br/>**Problem 5** (Pub-Sub & Downtime Recovery) |
| **Aldoy Fauzan Avanza** | `18223113` | **Member A (External Mock Sources)**<br/>• `services/mock-bmkg`: Seismic & tsunami alert endpoints with API key auth.<br/>• `services/mock-pvmbg`: Volcanic alert endpoints with Bearer auth, live dynamic schema toggles, and outage simulation. | **Problem 1** (Live Schema Evolution) |

---

## 2. Architecture Overview

The system strictly adheres to microservices decoupling principles. Direct function calls and shared business logic libraries across services are explicitly prohibited. Communication occurs exclusively over HTTP/REST (synchronous query path) and Redpanda/Kafka (asynchronous pub-sub event fan-out).

```mermaid
%%{init: {"htmlLabels": false, "flowchart": {"htmlLabels": false}}}%%
flowchart TB
    CLI["Clients<br/>Media, field teams, command centre"]:::actor

    subgraph GW["Access layer"]
        direction LR
        AUTH["auth :8084<br/>POST /auth/token<br/>POST /auth/refresh<br/>JWT HS256, TTL 60 s"]:::svc
        CAPI["client-api :8080<br/>GET /api/v1/hazards<br/>verifies JWT locally<br/>scope filter, X-Correlation-ID"]:::svc
    end

    AGG["aggregator :8083<br/>poll, normalise, enrich<br/>transactional outbox relay"]:::svc

    subgraph UP["Upstream agencies (simulated)"]
        direction LR
        BMKG["mock-bmkg :8081<br/>X-BMKG-Key"]:::ext
        PVMBG["mock-pvmbg :8082<br/>Bearer token"]:::ext
    end

    DB[("canonical-db :5432<br/>PostgreSQL 16<br/>hazards, outbox_events")]:::store

    subgraph BUS["Event bus"]
        direction LR
        INIT["redpanda-init<br/>one-shot topic bootstrap"]:::broker
        RP[["redpanda :9092<br/>topic bnpb.hazard-events<br/>3 partitions"]]:::broker
    end

    subgraph CON["Event consumers"]
        direction LR
        DASH["dashboard-consumer<br/>bnpb-dashboard-group"]:::cons
        NOTI["notifier-consumer<br/>bnpb-notifier-group"]:::cons
        AUDIT["evaluator (test script)<br/>bnpb-evaluator-audit-group"]:::cons
    end

    CLI -->|"1. POST /auth/token"| AUTH
    CLI -->|"2. Bearer JWT"| CAPI
    CAPI -->|"GET /internal/hazards<br/>X-Correlation-ID"| AGG
    AGG -->|"GET every 3 s"| BMKG
    AGG -->|"GET every 3 s"| PVMBG
    AGG <-->|"SQL transaction"| DB
    AGG -->|"publish claimed outbox events<br/>key = hazard_id"| RP
    INIT -.->|"create topic"| RP
    RP --> DASH
    RP --> NOTI
    RP -.->|"new group, no aggregator change"| AUDIT

    classDef svc fill:#eaf1fb,stroke:#1f4e8c,color:#0b2545
    classDef store fill:#e8f5e9,stroke:#2e7d32,color:#10300f
    classDef broker fill:#fdecea,stroke:#c62828,color:#4a0d0d
    classDef ext fill:#fff3e0,stroke:#ef6c00,color:#4a2600
    classDef cons fill:#f3e5f5,stroke:#6a1b9a,color:#2a0a3a
    classDef actor fill:#ffffff,stroke:#616161,color:#212121
    classDef plan fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray:4 3,color:#424242
```

---

## 3. Port Mapping & Service Directory

| Service Name | Directory | Container Port | Host Port | Network Scope | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`client-api`** | `services/client-api/` | `8080` | `8080:8080` | Public (`bnpb-public`) | Public API Gateway. Enforces JWT scopes, redacts raw telemetry for media, propagates `X-Correlation-ID`. |
| **`mock-bmkg`** | `services/mock-bmkg/` | `8081` | `8081:8081` | Public / Internal | Upstream mock for BMKG. Injects 50–150ms simulated latency, requires `X-BMKG-Key`. |
| **`mock-pvmbg`** | `services/mock-pvmbg/` | `8082` | `8082:8082` | Public / Internal | Upstream mock for PVMBG. Injects 500ms–3s latency, requires Bearer token, supports schema/outage admin toggles. |
| **`aggregator`** | `services/aggregator/` | `8083` | *None* (see note) | Internal Only (`bnpb-internal`) | Core orchestrator. Polls sources, performs static volcano joins, writes to Postgres + Outbox, exposes `/internal/hazards`. Reachable only from containers on `bnpb-internal`: that network is `internal: true`, so the `127.0.0.1:8083` mapping in `docker-compose.yml` is not published (`docker ps` shows `8083/tcp` only). |
| **`auth`** | `services/auth/` | `8084` | `8084:8084` | Public (`bnpb-public`) | Authentication Authority. Issues short-lived JWTs (60s TTL) and rotates refresh tokens. |
| **`canonical-db`** | `services/aggregator/migrations/` | `5432` | *None* | Internal Only (`bnpb-internal`) | PostgreSQL 16 Alpine canonical datastore. Isolated from host access for security. |
| **`redpanda`** | — | `9092` / `19092` | `19092:19092` | Public / Internal | Kafka API event broker. Handles topic `bnpb.hazard-events` and independent consumer group cursors. |
| **`dashboard-consumer` / `notifier-consumer`** | `services/consumers/` | *None* | *None* | Internal Only (`bnpb-internal`) | Independent consumer groups for dashboard statistics and urgent notifications. |

---

## 4. Architecture Decision Records (ADRs)

All foundational architectural decisions are codified in central ADRs under [`docs/adr/`](docs/adr/):

* **[ADR 001: Standardizing Microservices Implementation on Go](docs/adr/001-service-language-all-go.md)**  
  *Justification for standardizing all 6 logical services on Go 1.22 instead of Python/FastAPI or a polyglot stack. The seven Go containers measured 24.4–32.6 MB per image and 55.8 MiB total idle memory, while the 50-VU load test remained within the Milestone 1 thresholds.*
* **[ADR 002: Selecting Redpanda as Kafka-Compatible Message Broker for Event Fan-Out](docs/adr/002-message-broker-redpanda.md)**  
  *Justification for Redpanda and pure Go `segmentio/kafka-go`, based on Kafka protocol compatibility, a single native broker binary, and the `rpk` tooling used by this project. Details consumer group offset tracking, at-least-once delivery, and consumer downtime recovery.*
* **[ADR 003: Canonical Data Storage using PostgreSQL 16 with JSONB and Transactional Outbox](docs/adr/003-canonical-storage-postgres-jsonb.md)**  
  *Mandatory 3-way comparative evaluation (PostgreSQL + JSONB vs. MongoDB vs. Strict SQL). Details why MongoDB was rejected (multi-document transactions strictly require replica sets) and why strict relational was rejected (violates P1 live schema changes). Details the Transactional Outbox pattern eliminating dual-write hazards.*
* **[ADR 004: Internal Service-to-Service Communication via HTTP/REST with Decoupled Abstractions](docs/adr/004-internal-communication-rest.md)**  
  *Justification for HTTP/REST between Client API and Aggregator. Explains protocol homogeneity, seamless dynamic JSON attributes (P1), terminal operability (`curl`), measured Milestone 1 latency, and interface-driven decoupling (`HazardService`); no direct gRPC comparison was benchmarked.*
* **[ADR 005: Stateless Service Architecture and Milestone 2 Scalability Roadmap](docs/adr/005-stateless-architecture-and-m2-readiness.md)**  
  *Roadmap documenting the stateless request path, explicit Milestone 1 process-local state, and the externalization steps required for horizontal scaling, alongside deterministic identifiers, DB-level idempotency (`UNIQUE(source, source_ref_id)`), caching, and circuit breakers.*

---

## 5. Frozen Data Contracts

All service boundaries are governed by formal contracts located in [`contracts/`](contracts/):
* **[`contracts/hazard_event.json`](contracts/hazard_event.json)**: Canonical JSON Schema classifying fields into **Summary Fields** (public/media) and **Raw Fields** (restricted).
* **[`contracts/bmkg.yaml`](contracts/bmkg.yaml)**: OpenAPI 3.0 specification for `mock-bmkg` (`:8081`).
* **[`contracts/pvmbg.yaml`](contracts/pvmbg.yaml)**: OpenAPI 3.0 specification for `mock-pvmbg` (`:8082`).
* **[`contracts/aggregator_internal.yaml`](contracts/aggregator_internal.yaml)**: OpenAPI 3.0 specification for `aggregator` internal endpoints (`:8083`).
* **[`contracts/auth_api.yaml`](contracts/auth_api.yaml)**: OpenAPI 3.0 specification for `auth` token authority (`:8084`).
* **[`contracts/client_api.yaml`](contracts/client_api.yaml)**: OpenAPI 3.0 specification for public `client-api` (`:8080`).

---

## 6. System Prerequisites

Ensure the host workstation has the following tools installed:
* **Docker Engine** (v24.0+) and **Docker Compose** (v2.20+)
* **Go** (v1.22+)
* **`curl`** and **`jq`** (for running verification scripts)
* *(Optional)* **`k6`** (for running load tests locally; automatically falls back to Docker container if not installed)

---

## 7. Quickstart Guide

### 7.1 Clone and Environment Setup
```bash
git clone https://github.com/Darsua/AAT.git
cd AAT

# Copy default environment variables template
cp .env.example .env
```

### 7.2 Boot the Full Platform
Start the full platform, including the dashboard and notifier consumers, with a single command:
```bash
make up
# or: docker compose up -d --build
```

### 7.3 Verify Service Health
Verify that all five HTTP services report healthy (`"status": "UP"`):
```bash
curl -s http://localhost:8081/health | jq .  # mock-bmkg
curl -s http://localhost:8082/health | jq .  # mock-pvmbg
curl -s http://localhost:8084/health | jq .  # auth
curl -s http://localhost:8080/health | jq .  # client-api
docker compose exec client-api curl -s http://aggregator:8083/health | jq . # aggregator (internal)
```

To tail the logs across all containers:
```bash
make logs
```

To gracefully tear down the platform and clean volumes:
```bash
make down
# or: docker compose down -v
```

---

## 8. Demonstration & Grading Runbook

Automated test scripts under [`scripts/`](scripts/) and Makefile targets provide instant verification for each assigned problem.

### Evaluator Quick Verification Matrix

| Problem | Focus Area | Make Target | Direct Script / Command | Key Verification Criterion | Target Ports | Tracker Reference |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Problem 1 (P1)** | Dynamic Schema Evolution | `make demo-p1` | `./scripts/test_schema_change.sh` | Additive `confidence_level` dynamically ingested into Postgres JSONB without restart/migrations. | `8082`, `8080` | [`P1-01..P1-03`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5) |
| **Problem 2 (P2)** | Outage Handling & Concurrency | `make demo-p2`<br>`make load-test` | `./scripts/test_outage.sh`<br>`k6 run scripts/load_test.js` | BMKG stays healthy during PVMBG outage (`degraded: true`); 50 VUs sustained with p95 < 300ms. | `8082`, `8081`, `8080` | [`P2-01..P2-03`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5) |
| **Problem 3 (P3)** | Scoped RBAC & Token Lifecycle | `make demo-p3` | `./scripts/test_cross_credential.sh`<br>`./scripts/test_auth_scopes.sh` | Media role gets `403 Forbidden` on raw fields / `fields=all`; 60s TTL token refresh rotation works. | `8084`, `8080`, `8081`, `8082` | [`P3-01..P3-03`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5) |
| **Problem 4 (P4)** | Container Isolation & Rebuild | — | `docker compose restart aggregator`<br>`docker compose up -d --build client-api` | Zero cascading failures; clean DB reconnection on restart; schema coexistence in DB. | `8083`, `8080`, `5432` | [`P4-01..P4-02`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5) |
| **Problem 5 (P5)** | Pub-Sub Fan-Out & Recovery | `make demo-p5` | `./scripts/test_consumer_recovery.sh` | Independent consumer group replay from offset after downtime; dynamic 3rd consumer addition. | `19092` | [`P5-01..P5-04`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5) |

---

### Problem 1 (P1): Dynamic Live Schema Evolution
**Evidence Tracker Reference:** [`P1-01..P1-03`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5)  
**Goal**: Verify that PVMBG can introduce an additive field (`confidence_level: 0.95`) at runtime, and that the Aggregator persists it into PostgreSQL's `attributes JSONB` column without schema breaking, crashes, or migrations.

```bash
make demo-p1
# or: ./scripts/test_schema_change.sh
```

**Manual Step-by-Step Verification**:
1. Verify baseline PVMBG payload does not contain `confidence_level`:
   ```bash
   curl -s -X POST http://localhost:8082/admin/schema-version \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer pvmbg-bearer-token-2026" \
     -d '{"include_confidence_level": false}'

   curl -s -H "Authorization: Bearer pvmbg-bearer-token-2026" http://localhost:8082/volcanic-reports | jq '.[0]'
   ```
2. Enable dynamic schema evolution on PVMBG:
   ```bash
   curl -s -X POST http://localhost:8082/admin/schema-version \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer pvmbg-bearer-token-2026" \
     -d '{"include_confidence_level": true}'
   ```
3. Query PVMBG to confirm `confidence_level` is present:
   ```bash
   curl -s -H "Authorization: Bearer pvmbg-bearer-token-2026" http://localhost:8082/volcanic-reports | jq '.[0]'
   ```
4. Wait 3 seconds for Aggregator ingestion, then query Client API with Field Responder token to verify the dynamic attribute is preserved in canonical storage:
   ```bash
   # Request full fields from Client API
   curl -s -H "Authorization: Bearer <field_token>" "http://localhost:8080/api/v1/hazards?type=VOLCANIC&fields=all" | jq '.data[0].attributes'
   ```

---

### Problem 2 (P2): Upstream Outage Simulation & 50-Connection Load Test
**Evidence Tracker Reference:** [`P2-01..P2-03`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5)  
**Goal**: Simulate an unexpected outage of PVMBG (`mock-pvmbg`). Verify that:
1. BMKG seismic reports continue to ingest and query normally (fault isolation).
2. Volcanic queries gracefully return cached/stale data with `degraded: true` and `stale_since` timestamp.
3. The platform sustains a 50-connection concurrent load test without crashing or starvation.

```bash
make demo-p2
# or: ./scripts/test_outage.sh
```

**Executing the 50-Connection Sustained Load Test**:
```bash
make load-test
# or: k6 run scripts/load_test.js
```
*Evaluates 50 Virtual Users (VUs) sustaining requests against `http://localhost:8080/api/v1/hazards` for 60 seconds. Verifies p95 latency < 300ms and error rate < 1% per IF4031 M1 spec.*

**Manual Outage Simulation**:
```bash
# 1. Trigger PVMBG outage
curl -s -X POST http://localhost:8082/admin/outage \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer pvmbg-bearer-token-2026" \
  -d '{"is_outage": true}'

# 2. Verify mock-pvmbg returns 503 Service Unavailable
curl -i -H "Authorization: Bearer pvmbg-bearer-token-2026" http://localhost:8082/volcanic-reports

# 3. Verify mock-bmkg remains completely operational (HTTP 200)
curl -i -H "X-BMKG-Key: bmkg-secret-key-2026" http://localhost:8081/seismic-events

# 4. Restore PVMBG service
curl -s -X POST http://localhost:8082/admin/outage \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer pvmbg-bearer-token-2026" \
  -d '{"is_outage": false}'
```

---

### Problem 3 (P3): Scoped Access Control, Media 403 & 60s TTL Token Refresh
**Evidence Tracker Reference:** [`P3-01..P3-03`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5)  
**Goal**: Verify Role-Based Access Control (RBAC):
1. **Media Role**: Accessing summary fields returns `200 OK`. Requesting raw telemetry (`fields=all`) strictly returns **`403 Forbidden`**.
2. **Field / Internal Role**: Accessing full telemetry (`fields=all`) returns `200 OK`.
3. **60-Second TTL**: Tokens expire after 60 seconds; callers refresh tokens using `POST /auth/refresh`.

```bash
# Load local credentials without committing them, then override Docker-internal
# service URLs with their host-facing addresses.
set -a
source .env
set +a

AUTH_URL=http://localhost:8084 \
CLIENT_API_URL=http://localhost:8080 \
BMKG_URL=http://localhost:8081 \
BMKG_KEY="${BMKG_API_KEY}" \
PVMBG_URL=http://localhost:8082 \
make demo-p3

# Quick C-04 iteration without waiting for natural access-token expiry:
AUTH_URL=http://localhost:8084 \
CLIENT_API_URL=http://localhost:8080 \
SKIP_TTL_WAIT=1 \
./scripts/test_auth_scopes.sh
```

The P3 scripts require `curl` and `jq`. `test_auth_scopes.sh` fails immediately
when credentials, required tools, or live service responses are missing; it does
not substitute mock tokens or fabricated HTTP responses.

**Manual Verification Flow**:
```bash
# 1. Acquire Media Token
MEDIA_TOKEN=$(curl -s -X POST http://localhost:8084/auth/token \
  -H "Content-Type: application/json" \
  -d '{"client_id":"test-media-client","client_secret":"secret123","requested_scope":"media"}' | jq -r .access_token)

# 2. Media queries summary fields -> HTTP 200 OK
curl -i -H "Authorization: Bearer ${MEDIA_TOKEN}" "http://localhost:8080/api/v1/hazards?fields=summary"

# 3. Media attempts to query raw fields -> HTTP 403 Forbidden!
curl -i -H "Authorization: Bearer ${MEDIA_TOKEN}" "http://localhost:8080/api/v1/hazards?fields=all"

# 4. Acquire Field Token
FIELD_TOKEN=$(curl -s -X POST http://localhost:8084/auth/token \
  -H "Content-Type: application/json" \
  -d '{"client_id":"test-field-client","client_secret":"secret123","requested_scope":"field"}' | jq -r .access_token)

# 5. Field queries raw fields -> HTTP 200 OK
curl -i -H "Authorization: Bearer ${FIELD_TOKEN}" "http://localhost:8080/api/v1/hazards?fields=all"

# 6. Rotate Refresh Token
curl -s -X POST http://localhost:8084/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<refresh_token>"}' | jq .
```

---

### Problem 4 (P4): Independent Container Rebuild & Restart Resilience
**Evidence Tracker Reference:** [`P4-01..P4-02`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5)  
**Goal**: Verify zero tight coupling by demonstrating that any single container can be stopped, restarted, or rebuilt without cascading crashes across dependent services.

```bash
# 1. Restart Aggregator independently
docker compose restart aggregator
docker compose ps aggregator

# 2. Rebuild Client API independently while others run
docker compose up -d --build client-api
docker compose ps client-api

# 3. Restart canonical PostgreSQL database
docker compose restart canonical-db
# Observe aggregator logs: reconnects cleanly to database
docker compose logs --tail=20 aggregator
```

---

### Problem 5 (P5): Pub-Sub Fan-Out, Consumer Downtime Recovery & Dynamic 3rd Consumer
**Evidence Tracker Reference:** [`P5-01..P5-04`](docs/tracker.md#3-demonstration--evidence-matrix-p1p5)  
The Compose services use the fixed names `dashboard-consumer` and
`notifier-consumer`, with independent groups `bnpb-dashboard-group` and
`bnpb-notifier-group`.

**Goal**: Demonstrate asynchronous event streaming to downstream consumers:
1. `dashboard-consumer` and `notifier-consumer` receive events in real time.
2. Stop `notifier-consumer` for 20 seconds while new events are published; upon restart, verify it recovers and consumes all missed messages from its committed offset.
3. Dynamically spin up an unexpected 3rd consumer without restarting or reconfiguring the publisher.

```bash
make demo-p5
# or: ./scripts/test_consumer_recovery.sh
```

**Step-by-Step Verification**:
```bash
# 1. Inspect live consumer logs receiving events
docker compose logs -f dashboard-consumer notifier-consumer

# 2. Simulate Notifier consumer downtime
docker compose stop notifier-consumer

# 3. Wait for new events to be polled and committed to Redpanda by aggregator
sleep 15

# 4. Restart Notifier consumer
docker compose start notifier-consumer

# 5. Verify Notifier resumes from its last committed offset and processes all missed events:
docker compose logs --tail=50 notifier-consumer

# 6. Dynamically add a 3rd Consumer (Evaluator Audit Service) without touching the aggregator:
docker compose run --rm \
  -e REDPANDA_BROKERS=redpanda:9092 \
  -e CONSUMER_GROUP=bnpb-evaluator-audit-group \
  -e TOPIC=bnpb.hazard-events \
  dashboard-consumer
```

---

## 9. Configuration Reference (`.env.example`)

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `CLIENT_API_PORT` | `8080` | Host port exposed for Client-Facing API Gateway. |
| `MOCK_BMKG_PORT` | `8081` | Host port exposed for Mock BMKG service. |
| `MOCK_PVMBG_PORT` | `8082` | Host port exposed for Mock PVMBG service. |
| `AGGREGATOR_PORT` | `8083` | Internal port for Aggregator service (not exposed to host). |
| `AUTH_PORT` | `8084` | Host port exposed for Auth Authority service. |
| `REDPANDA_PORT` | `19092` | Host port exposed for external Redpanda Kafka client testing. |
| `POSTGRES_USER` | `bnpb_user` | PostgreSQL database master username. |
| `POSTGRES_PASSWORD`| `bnpb_secret`| PostgreSQL database master password. |
| `POSTGRES_DB` | `bnpb_hazards` | Canonical PostgreSQL database name. |
| `DATABASE_URL` | `postgres://bnpb_user:bnpb_secret@canonical-db:5432/bnpb_hazards?sslmode=disable` | Full PostgreSQL connection URI. |
| `REDPANDA_BROKERS` | `redpanda:9092` | Internal Kafka broker address for services. |
| `KAFKA_TOPIC_HAZARDS` | `bnpb.hazard-events` | Redpanda topic for canonical hazard event broadcasts. |
| `KAFKA_TOPIC_PARTITIONS` | `3` | Number of partitions provisioned for the hazard-event topic. |
| `CONSUMER_GROUP_DASHBOARD` | `bnpb-dashboard-group` | Consumer group ID for dashboard subscriber. |
| `CONSUMER_GROUP_NOTIFIER` | `bnpb-notifier-group` | Consumer group ID for notification alert subscriber. |
| `BMKG_URL` | `http://mock-bmkg:8081` | Internal URL for polling BMKG mock. |
| `BMKG_API_KEY` | `bmkg-secret-key-2026` | API key required in `X-BMKG-Key` header for BMKG access. |
| `PVMBG_URL` | `http://mock-pvmbg:8082` | Internal URL for polling PVMBG mock. |
| `PVMBG_TOKEN` | `pvmbg-bearer-token-2026` | Bearer token required for PVMBG mock access. |
| `POLL_INTERVAL_SECS` | `3` | Frequency (in seconds) at which Aggregator polls upstream mocks. |
| `AUTH_URL` | `http://auth:8084` | Internal URL for Auth service. |
| `AGGREGATOR_URL` | `http://aggregator:8083` | Internal URL for Aggregator service queries. |
| `JWT_SECRET` | `super-secret-jwt-key-2026` | Shared HMAC-SHA256 secret for signing and verifying JWTs. |
| `JWT_TTL_SECONDS` | `60` | Expiration lifetime for issued JWT access tokens. |
| `AUTH_MEDIA_CLIENT_ID` | `test-media-client` | Client identity for Media role authentication. |
| `AUTH_MEDIA_CLIENT_SECRET` | `secret123` | Shared secret for Media client authentication. |
| `AUTH_FIELD_CLIENT_ID` | `test-field-client` | Client identity for Field responder role authentication. |
| `AUTH_FIELD_CLIENT_SECRET` | `secret123` | Shared secret for Field responder client authentication. |
| `AUTH_INTERNAL_CLIENT_ID` | `test-internal-client` | Client identity for Internal operations role authentication. |
| `AUTH_INTERNAL_CLIENT_SECRET` | `secret123` | Shared secret for Internal operations client authentication. |

---

## 10. Project Management & Service Guides

### 10.1 Project Management & Guardrails
* [Master Team Project Tracker & Evidence Matrix](docs/tracker.md) — 38-task breakdown, Gantt timeline, and grading evidence mapping.
* [AI Agent Operating Guidelines & Guardrails](AGENTS.md) — Non-negotiable course rules, RACI ownership, and verification checklist.

### 10.2 Service Developer Guides
For localized developer instructions, internal contracts, and implementation checklists:
* [Aggregator & Persistence Developer Guide](services/aggregator/README.md) (Member B — Darrel)
* [Auth Service Developer Guide](services/auth/README.md) (Member C — Jenka)
* [Client API Developer Guide](services/client-api/README.md) (Member C — Jenka)
* [Downstream Consumers Developer Guide](services/consumers/README.md) (Member C — Jenka)
* [BMKG Mock Developer Guide](services/mock-bmkg/README.md) (Member A — Aldoy)
* [PVMBG Mock Developer Guide](services/mock-pvmbg/README.md) (Member A — Aldoy)
