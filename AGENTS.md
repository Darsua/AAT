# AI Agent Operating Guidelines & Guardrails (AGENTS.md)

This repository implements the **BNPB Disaster Coordination Platform** for **IF4031 Milestone 1**.
AI agents (Antigravity, Cursor, Claude Code, Copilot) assisting developers in this repository **must strictly follow these rules**.

---

## 1. Non-Negotiable Course Constraints (DO NOT VIOLATE)

1. **Zero Shared Code or Internal Libraries:**
   - **DO NOT** create a `pkg/`, `shared/`, or `common/` package.
   - **DO NOT** import packages across service boundaries (e.g. `client-api` must never import from `bnpb/aggregator`).
   - Every microservice is an independent Go module and must be 100% self-contained. Struct duplication across services is intentional and required by course policy.
2. **Strict Storage Ownership:**
   - `canonical-db` (PostgreSQL) is owned **exclusively** by `services/aggregator`.
   - `client-api` and downstream consumers must **never** connect directly to the database. All data retrieval goes through the Aggregator HTTP API (`http://aggregator:8083/internal/hazards`).
3. **Frozen Contracts Are Read-Only:**
   - Files in `contracts/` (`hazard_event.json`, `bmkg.yaml`, `pvmbg.yaml`, `aggregator_internal.yaml`, `client_api.yaml`, `auth_api.yaml`) are frozen.
   - **DO NOT** modify contract field names, types, or endpoints without explicit developer instructions. Implement code to match the contracts, not vice versa.
4. **Secret Handling & Configuration:**
   - **DO NOT** hardcode credentials, tokens, or API keys in Go source code or test scripts.
   - Always read credentials via environment variables. If a new variable is introduced, add it with a safe default to `.env.example` and `docker-compose.yml`.

---

## 2. Team RACI Scope Boundaries

Respect service ownership. When tasked with implementing or debugging a feature, **do not touch services outside the assigned member's scope**:

| Member | Assigned Services | Responsibilities |
|---|---|---|
| **Member A (Aldoy)** | `services/mock-bmkg/`<br/>`services/mock-pvmbg/` | Upstream mocks, historical seeds, dynamic event generator, latency injection, schema version toggle (`/admin/schema-version`), and outage toggle (`/admin/outage`). |
| **Member B (Darrel)** | `services/aggregator/`<br/>`services/aggregator/migrations/` | Ingestion polling workers, canonical mapping, static volcano enrichment, Transactional Outbox relay, and `GET /internal/hazards`. |
| **Member C (Jenka)** | `services/auth/`<br/>`services/client-api/`<br/>`services/consumers/` | JWT issuance & refresh (`POST /auth/*`), public API gateway & scope filtering (`GET /api/v1/hazards`), and downstream Kafka consumer groups (`dashboard`, `notifier`). |

---

## 3. Go Workspace & Code Conventions

- **Workspace Mode:** This project uses a Go 1.22 multi-module workspace (`go.work`). Run `go work sync` if new dependencies are added to an individual module.
- **Dependencies:** Standard library first (`net/http`). Only vetted libraries are permitted:
  - PostgreSQL: `github.com/jackc/pgx/v5`
  - Kafka / Redpanda: `github.com/segmentio/kafka-go`
  - JWT Security: `github.com/golang-jwt/jwt/v5`
- **Dynamic Fields (Problem 1):** PVMBG additive fields (like `confidence_level`) must be dynamically ingested into the canonical `attributes` (`map[string]any` / JSONB) without schema recompilation or dropping unmapped fields.
- **Observability:** Propagate `X-Correlation-ID` through incoming/outgoing HTTP headers and Kafka message headers.
- **Timestamps:** Always format and parse timestamps in UTC ISO-8601 (`time.RFC3339`).
- **Dockerfiles:** Multi-stage builds using `golang:1.22-alpine` as builder and `alpine:3.20` as runtime. Always run containers as non-root `USER nobody:nobody`.

---

## 4. Verification Commands

Before concluding any code modification, always verify:
```bash
# 1. Build and vet all Go services
go vet ./services/aggregator/... ./services/auth/... ./services/client-api/... ./services/mock-bmkg/... ./services/mock-pvmbg/...
make test

# 2. Validate Docker Compose syntax
docker compose config --quiet

# 3. Check for forbidden unencrypted .env files
git status
```
