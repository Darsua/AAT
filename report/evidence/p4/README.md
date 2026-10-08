# Problem 4 Demonstration & Evidence: Independent Service Rebuilds & Flexible Canonical Storage

**Date:** 08 October 2026  
**Component Owner:** Member B (Darrel Adinarya Sunanda)  
**Target Problem:** Problem 4 (Penyimpanan Data & Batasan Layanan)

---

## 1. Overview & Acceptance Criteria
This directory records the empirical evidence for **Problem 4 (P4)**:
1. **P4-01 (Independent Container Restart & Zero Cascading Failure):** Individual containers (`aggregator`, `canonical-db`) can be stopped, rebuilt, or restarted independently without crashing or degrading unrelated services (`client-api`, `mock-bmkg`, `mock-pvmbg`, `auth`). Reconnected services self-heal automatically without operator intervention.
2. **P4-02 (Flexible Canonical Storage & Heterogeneous Coexistence):** The canonical data model in PostgreSQL (`hazards` table) seamlessly stores heterogeneous records across different schema versions (`baseline` vs. `evolved` with dynamic fields like `confidence_level`) using the `attributes JSONB` column without requiring DDL alterations (`ALTER TABLE`), database locks, or schema migrations.

---

## 2. Evidence Files
- [`storage_restart.log`](./storage_restart.log): Execution log of `./scripts/test_storage_restart.sh` capturing:
  1. Direct SQL inspection of the PostgreSQL `hazards` table showing heterogeneous records with varying attribute keys.
  2. Independent restart of the `aggregator` container while `client-api` and upstream mock services remain online and healthy.
  3. Re-establishment of the PostgreSQL `pgx` connection pool upon container restart.
  4. Independent restart of the `canonical-db` container, verifying client API resilience and end-to-end self-healing query execution.

---

## 3. Test Execution Summary

### Verification Step 1: Flexible Canonical Storage Coexistence (`P4-02`)
Direct database queries on `canonical-db` demonstrate that records generated under the baseline schema and records generated after the live schema evolution coexist in the same relational table:
- **Baseline Records:** Contain domain attributes such as `ash_column_height_m`, `eruption_count_24h`, and `volcano_id`.
- **Evolved Records:** Contain dynamic fields (`confidence_level`) seamlessly preserved in `attributes JSONB`.
- **Relational Columns:** Core indexed dimensions (`hazard_id`, `source`, `hazard_type`, `severity`, `occurred_at`) remain strictly typed and indexed for high-performance filtering.

### Verification Step 2 & 3: Aggregator Lifecycle Isolation (`P4-01`)
- `aggregator` restarted via `docker compose restart aggregator`.
- During container stop and restart, health checks on `client-api` (:8080), `mock-bmkg` (:8081), and `mock-pvmbg` (:8082) returned HTTP 200 without interruption.
- Aggregator connection pool re-established connection cleanly and resumed outbox publishing and poller workers.

### Verification Step 4: Canonical Database Lifecycle & Reconnection (`P4-01`)
- `canonical-db` restarted via `docker compose restart canonical-db`.
- Upon PostgreSQL readiness, Client API queries (`GET /api/v1/hazards`) immediately succeeded with HTTP 200, verifying that database connection pools reconnect transparently without requiring application container restarts.

---

## 4. Architectural Evaluation: Storage Options Trade-Off Analysis

| Evaluation Criterion | Option 1: PostgreSQL + JSONB (Selected) | Option 2: MongoDB (Pure Document Store) | Option 3: Strict Relational SQL (DDL per Change) |
|---|---|---|---|
| **ACID & Outbox Transactionality** | **Strong (Winner):** Atomic commit of `hazards` + `outbox_events` in a single transaction guarantees zero data loss or dual-write anomalies. | **Weak / Complex:** Multi-document transactions require distributed replica sets with notable performance overhead. | **Strong:** Full transactional guarantees. |
| **Schema Evolution Agility** | **High:** Core fields remain relational; unknown or evolving fields map into `JSONB` without table locks. | **High:** Naturally schemaless. | **Low:** Requires runtime `ALTER TABLE` or complex Entity-Attribute-Value (EAV) patterns, risking lock contention. |
| **Query Performance & Indexing** | **High:** GIN indexing on `attributes` allows efficient sub-document queries alongside standard B-tree indexes. | **Moderate:** Excellent document queries, but lacks join capabilities if needed for future relational extensions. | **High:** Direct column indexing, but suffers under EAV table bloat. |
| **Operational Simplicity** | **High:** Single battle-tested relational engine satisfies both ACID transactional outbox and flexible storage needs. | **Moderate:** Introduces additional infrastructure footprint and cluster management overhead. | **Low:** Heavy migration operational burden during continuous schema evolution. |
