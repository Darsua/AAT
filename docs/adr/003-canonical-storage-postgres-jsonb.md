# ADR 003: Canonical Data Storage using PostgreSQL 16 with JSONB and Transactional Outbox

---

## Status and scope

This records M1 canonical storage and pending-message relay behavior, **not event sourcing or an audit history**. M2 contracts frozen in [#34](https://github.com/Darsua/AAT/issues/34) are broader than the implemented runtime. Sharded storage ([#23](https://github.com/Darsua/AAT/issues/23), Darrel), Aggregator HA ([#25](https://github.com/Darsua/AAT/issues/25), Darrel), and audit ([#30](https://github.com/Darsua/AAT/issues/30), Darrel) remain future work. Historical measurements and their updated evidence links below are preserved.

## Context
In the BNPB Disaster Coordination Platform, `services/aggregator` serves as the single source of truth for unified disaster intelligence. It addresses four interrelated requirements spanning Problems 1, 4, and 5:

1. **Canonical Schema Harmonization (Problem 1 & 4)**: The aggregator pulls heterogeneous event models from external agencies:
   * BMKG provides seismic and tsunami alerts with coordinates, magnitude, depth, and tsunami potential.
   * PVMBG provides volcanic activity reports referencing volcano codes, alert statuses, crater smoke heights, and seismicity counts.
   The storage layer must harmonize these into a standardized canonical model (`HazardEvent`) supporting unified querying and filtering.
2. **Dynamic Live Schema Evolution (Problem 1)**: PVMBG must be able to introduce additive fields (such as `confidence_level: 0.95` or instrument sensor metadata) at runtime without requiring database migrations, container redeployments, or service restarts, while ensuring downstream consumers receive the enriched data intact.
3. **Static Reference Enrichment (Problem 4)**: PVMBG reports supply an agency volcano identifier (e.g., `VLC-001`), but omit geographic coordinates. The aggregator must cross-reference a static volcano catalog to enrich canonical events with authoritative `latitude` and `longitude`.
4. **Reliable Dual-Write Elimination (Problem 5)**: Ingested events must be committed to durable storage *and* broadcast to the Redpanda broker. A direct dual-write approach (`db.Save() -> kafka.Publish()`) risks severe data inconsistency: if Kafka is momentarily unreachable, the DB transaction succeeds but the event is never published; conversely, if the DB fails after publishing, phantom events are dispatched.

---

## Decision
We adopt **PostgreSQL 16 (Alpine)** as our canonical storage engine, utilizing a **Hybrid Relational + JSONB Schema** combined with the **Transactional Outbox Pattern**:

1. **Relational Envelope + JSONB Payload**: Fixed envelope fields (`hazard_id`, `source`, `source_ref_id`, `hazard_type`, `severity`, `occurred_at`, coordinates) are stored in strictly-typed relational columns for fast B-Tree indexing and query filtering. Dynamic, additive fields are captured in an `attributes JSONB NOT NULL DEFAULT '{}'` column.
2. **Static Volcano Catalog**: Stored in a relational table `volcano_references` with primary key `volcano_id`.
3. **Transactional Outbox Table**: An `outbox_events` row per source record stores the latest broker payload atomically with the hazard upsert. Changed payloads replace that row and reset publication status, so unpublished revisions can coalesce. Published payloads are also replaceable; this is not an immutable event log or Audit API.
4. **Asynchronous Outbox Relay**: An internal background worker inside `aggregator` polls unpublished outbox records with `SELECT ... FOR UPDATE SKIP LOCKED`, dispatches them to Redpanda via `segmentio/kafka-go`, and marks them `published = TRUE`.

---

## Outbox flow and storage alternatives

```mermaid
sequenceDiagram
    autonumber
    participant Ing as aggregator (ingestion)
    participant DB as canonical-db
    participant Rel as aggregator (outbox relay)
    participant RP as redpanda

    Ing->>DB: One transaction: UPSERT hazards and latest outbox payload
    Note over Ing,DB: One row per source record; pending revisions can coalesce
    loop every 500 ms
        Rel->>DB: Phase 1: claim with FOR UPDATE SKIP LOCKED and 30 s lease
        DB-->>Rel: claimed rows
        alt payload invalid or hazard_id missing
            Rel->>DB: Complete quarantine: published = true, quarantine event_type
        else payload valid
            Rel->>RP: Phase 2: publish outside transaction, key = hazard_id
            alt publish succeeds
                RP-->>Rel: ack
                Rel->>DB: Phase 3: mark published only if payload unchanged
            else publish fails
                Rel->>DB: Attempt to release unpublished claims for next-tick retry
            end
        end
    end
    Note over Rel,DB: Failed release or crash: lease expiry is fallback; replay is possible
```

### Comparative Matrix

| Evaluation Criteria | PostgreSQL 16 + JSONB (Chosen) | Pure Document Store: MongoDB 7.0 (Rejected) | Strict Normalized SQL: MySQL 8 (Rejected) |
| :--- | :--- | :--- | :--- |
| **P1: Dynamic Additive Schema** | JSONB stores dynamic attributes without DDL; the implemented PVMBG adapter captures unknown fields. BMKG typed DTOs drop them. | BSON can store dynamic attributes; adapter capture is still needed. | A column-per-attribute model needs DDL. **MySQL 8 also supports JSON** and can use a hybrid design; this is a modeling trade-off, not absence of JSON support. |
| **Multi-Document ACID Transactions** | **Native & Lightweight**. Standard `BEGIN ... COMMIT` handles `hazards` + `outbox_events` atomically. | **Complex & Fragile**. Multi-document transactions require a **Replica Set** (a single-member set is enough). A standalone `mongod` rejects them. | **Native**. Standard ACID transaction support. |
| **Relational Reference Enrichment** | **Native & Indexed**. Fast B-Tree lookup on `volcano_references(volcano_id)`. | **Requires Aggregation**. Must use `$lookup` aggregation pipelines or dual-query client joins. | **Native & Indexed**. Standard foreign key / join semantics. |
| **Idle Memory Footprint** | **47.7 MiB** (measured, `postgres:16-alpine` in the running stack). | 76.2 MiB (measured, `mongo:7.0`, empty, default settings). | 385 MiB (measured, `mysql:8.0`, default settings). |
| **Docker Compose Ergonomics** | **Zero Setup**. Single lightweight container with standard `/docker-entrypoint-initdb.d/`. | **High Friction**. Requires starting `mongod` with `--replSet` and running `rs.initiate()` before transactions work. | Moderate. Requires table migrations and health check scripts. |
| **Indexing Flexibility** | GIN indexes on `attributes`, B-Tree indexes on timestamp, severity, and type. | Compound B-tree and multi-key indexes. | B-Tree indexes plus generated-column/functional indexes for selected JSON expressions; a different indexing approach from PostgreSQL GIN. |

---

## Detailed Rationale for Choices & Rejections

### Why Pure Document Store (MongoDB) Was Rejected
1. **Multi-Document Transaction Constraint**: The Transactional Outbox pattern requires an atomic transaction spanning two logical collections (`hazards` and `outbox_events`). In MongoDB, **multi-document ACID transactions are not available on standalone instances**. Tested on `mongo:7.0` (see [historical MongoDB transaction findings](../evidence/m1/adr-audit/README.md#mongodb-transaction-findings)): a transaction command sent to a standalone mongod fails with:
   ```
   MongoServerError: Transaction numbers are only allowed on a replica set member or mongos
   ```
   A replica set with a single member is sufficient (the same test committed a two-document transaction after `rs.initiate()` on one node), so no multi-node cluster is needed. It still requires `--replSet` and an `rs.initiate()` step (a sidecar or init script) before the application can start, and a node started with `--replSet` rejects writes until that step has run. This adds an ordering dependency to Docker Compose startup that PostgreSQL does not have.
2. **Memory Footprint**: *Measured on 2026-10-10: Docker 29.8.2, 13.5 GiB host RAM, single sample, idle about 45 s to 2 min after the service was ready. Raw figures and method are in [historical measurement evidence](../evidence/m1/adr-audit/README.md#methodology-and-limitations). Python and other third-party figures come from minimal stand-ins (a hello-world FastAPI app; stock images with default settings), not from BNPB services.* An empty `mongo:7.0` container idled at 76.2 MiB (1.18 GB image, ready in 1.2 s), which is about 1.6x the PostgreSQL figure rather than a large multiple. Memory is therefore **not** a deciding argument against MongoDB; the transaction constraint above and the join-free reference lookup below are.
3. **Volcano Reference Lookups**: Relational joins for volcano coordinates (`VLC-001` → latitude/longitude) are awkward in document models and require multi-stage `$lookup` aggregation pipelines instead of simple, indexed SQL lookups.

### Why Strict Normalized Relational SQL Was Rejected
1. **Column-per-Attribute Limitation**: Representing every source attribute as a dedicated SQL column requires a migration before storing a newly introduced field in its own column. Rejecting that strict model does **not** reject MySQL's JSON capabilities: MySQL 8 can also retain additive attributes in a JSON column. PostgreSQL + JSONB was selected for this implementation's hybrid schema, indexing, and transaction workflow.
2. **Sparse Column Anti-Pattern**: Pre-allocating hundreds of nullable columns for prospective future agency fields degrades query cache density and creates severe database hygiene issues.

### Why PostgreSQL + JSONB is the Optimal Solution
PostgreSQL 16 combines the structural rigor of relational databases with the schemaless flexibility of document stores:
1. **Strict Envelope Indexing**: High-frequency filter fields (`occurred_at DESC`, `hazard_type`, `severity`, `source`) live in dedicated columns indexed with standard B-Trees.
2. **Dynamic Evolution in `attributes`**: JSONB stores arbitrary nested JSON, queryable with operators such as `attributes->>'confidence_level'` and indexable with GIN. The implemented PVMBG adapter explicitly preserves unknown fields; BMKG's typed DTOs discard unknown fields before storage.
3. **Lightweight ACID Outbox**: Writing to `hazards` and `outbox_events` happens in a single `BEGIN ... COMMIT` block. If the server crashes mid-flight, both writes roll back cleanly.

---

## Schema Architecture

The authoritative schema, indexes, compatibility alterations, and six-volcano seed are in [001_init.sql](../../services/aggregator/migrations/001_init.sql). Key boundaries are:

- `hazards`: typed query envelope plus dynamic `attributes JSONB`; `UNIQUE (source, source_ref_id)` prevents duplicate canonical rows for a source record.
- `volcano_references`: keyed coordinate lookup for PVMBG enrichment.
- `outbox_events`: latest per-source-record payload, publication status, correlation ID, and claim lease; a partial index supports unpublished-row polling. It is mutable pending-message storage, not revision history.

---

## Transactional Outbox Worker Loop

The [outbox relay](../../services/aggregator/internal/worker/outbox_relay.go), with claim and completion queries in [`RelayOutboxBatch`](../../services/aggregator/internal/repository/hazard_repo.go), provides **at-least-once retry semantics for pending valid payloads** while durable storage survives and dependencies recover. It does not retain every revision: upserts replace the per-source-record payload. A tick (default 500 ms) runs three phases so that no database lock is held while talking to the broker:

1. **Claim (short transaction)**: select up to 50 unpublished rows whose lease is empty or expired, using `FOR UPDATE SKIP LOCKED`, and set a 30-second lease on them.
2. **Publish (outside any transaction)**: for each row the relay reads `hazard_id` from the payload and writes a Kafka message with `Key = hazard_id`, `Value = payload` and an `X-Correlation-ID` header (falling back to `outbox-relay-<id>`). Rows whose payload is malformed or lacks `hazard_id` are quarantined instead of published. If the broker write fails, the code attempts to clear leases (`claim_expires_at = NULL`) so the batch can retry on the next tick. That release is best-effort; if it fails or the process crashes, lease expiry provides the fallback.
3. **Complete (short transaction)**: published rows are set to `published = TRUE` only if the payload is unchanged (`WHERE id = $1 AND payload = $2::jsonb`), so a row whose payload was replaced mid-flight is not marked as published; quarantined rows are marked `published = TRUE` with `event_type = 'hazard_event_quarantined'`; any other claimed rows have their lease cleared.

This avoids the direct database/broker dual-write gap, with explicit limits:
* A transaction committed before a crash retains both canonical state and pending payload if PostgreSQL data survives. A crash between publish and completion permits replay after lease expiry, hence duplicate delivery ([ADR 002](002-message-broker-redpanda.md)). An intervening upsert can replace the pending revision; this is not complete revision recovery.
* Claiming with `SKIP LOCKED` and hashing `hazard_id` do not establish event-time or revision order across concurrent relays. M2 HA must address these semantics explicitly.
* If Redpanda is down, the relay retries on every tick (fixed interval, no exponential backoff) while the database remains healthy and capable of accepting queries.
