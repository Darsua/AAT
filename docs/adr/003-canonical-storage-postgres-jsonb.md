# ADR 003: Canonical Data Storage using PostgreSQL 16 with JSONB and Transactional Outbox

---

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
3. **Transactional Outbox Table**: An `outbox_events` table stores pending broker messages written atomically within the same database transaction as the hazard record.
4. **Asynchronous Outbox Relay**: An internal background worker inside `aggregator` polls unpublished outbox records with `SELECT ... FOR UPDATE SKIP LOCKED`, dispatches them to Redpanda via `segmentio/kafka-go`, and marks them `published = TRUE`.

---

## Detailed 3-Way Comparative Evaluation (Problem 4 Report)

The course specification explicitly requires a rigorous comparative evaluation of storage alternatives to justify the persistence architecture.

```mermaid
flowchart TD
    subgraph Ingestion["Ingestion Flow"]
        Input["Incoming Agency Event\n(BMKG / PVMBG)"]
        Transform["Canonical Harmonization &\nStatic Volcano Lookup"]
    end

    subgraph AtomicTx["Single PostgreSQL ACID Transaction"]
        Hazards["INSERT INTO hazards\n(Strict Envelope + JSONB attributes)"]
        Outbox["INSERT INTO outbox_events\n(payload: HazardEvent JSON, published: false)"]
    end

    subgraph AsyncRelay["Asynchronous Background Relay"]
        RelayWorker["Outbox Relay Worker\n(SELECT ... FOR UPDATE SKIP LOCKED)"]
        Redpanda["Redpanda Broker\n(Topic: bnpb.hazard-events)"]
        MarkDone["UPDATE outbox_events\nSET published = true"]
    end

    Input --> Transform
    Transform --> AtomicTx
    Hazards --- Outbox
    AtomicTx --> RelayWorker
    RelayWorker -->|1. Produce| Redpanda
    Redpanda -->|2. ACK| MarkDone
```

### Comparative Matrix

| Evaluation Criteria | PostgreSQL 16 + JSONB (Chosen) | Pure Document Store: MongoDB 7.0 (Rejected) | Strict Normalized SQL: MySQL 8 (Rejected) |
| :--- | :--- | :--- | :--- |
| **P1: Dynamic Additive Schema** | **Native**. Unknown fields unmarshal directly into `attributes JSONB` without DDL migrations. | **Native**. BSON documents natively accept dynamic additive attributes. | **Fails**. Any new field (`confidence_level`) requires `ALTER TABLE` or triggers SQL column errors. |
| **Multi-Document ACID Transactions** | **Native & Lightweight**. Standard `BEGIN ... COMMIT` handles `hazards` + `outbox_events` atomically. | **Complex & Fragile**. Multi-document transactions strictly require a **Replica Set**. Standalone single nodes crash with transaction errors. | **Native**. Standard ACID transaction support. |
| **Relational Reference Enrichment** | **Native & Indexed**. Fast B-Tree lookup on `volcano_references(volcano_id)`. | **Requires Aggregation**. Must use `$lookup` aggregation pipelines or dual-query client joins. | **Native & Indexed**. Standard foreign key / join semantics. |
| **Idle Memory Footprint** | **~22–28 MB** (PostgreSQL Alpine container). | **~350–650 MB** (WiredTiger engine pre-allocates substantial cache). | ~180–300 MB (InnoDB buffer pool defaults). |
| **Docker Compose Ergonomics** | **Zero Setup**. Single lightweight container with standard `/docker-entrypoint-initdb.d/`. | **High Friction**. Requires complex init script to initiate replica set (`rs.initiate()`) before transactions work. | Moderate. Requires table migrations and health check scripts. |
| **Indexing Flexibility** | GIN indexes on `attributes`, B-Tree indexes on timestamp, severity, and type. | Compound B-tree and multi-key indexes. | B-Tree indexes on fixed columns only; JSON columns cannot be natively foreign-keyed. |

---

## Detailed Rationale for Choices & Rejections

### Why Pure Document Store (MongoDB) Was Rejected
1. **Multi-Document Transaction Constraint**: The Transactional Outbox pattern requires an atomic transaction spanning two logical collections (`hazards` and `outbox_events`). In MongoDB, **multi-document ACID transactions are strictly forbidden on standalone instances**; attempting to execute `session.startTransaction()` on a standalone mongod instance results in:
   ```
   MongoServerError: Transaction numbers are only allowed on a replica set member or mongos
   ```
   To run transactions in Docker Compose, one must configure a multi-node replica set or execute a custom bash sidecar running `rs.initiate()`. This introduces significant operational brittleness during automated grading runs.
2. **Memory Footprint**: MongoDB's WiredTiger storage engine aggressively pre-allocates memory for internal caches, routinely consuming 400 MB to 1 GB RAM even at complete idle. In a setup running eight concurrent containers, this wastes scarce host resources.
3. **Volcano Reference Lookups**: Relational joins for volcano coordinates (`VLC-001` → latitude/longitude) are awkward in document models and require multi-stage `$lookup` aggregation pipelines instead of simple, indexed SQL lookups.

### Why Strict Normalized Relational SQL Was Rejected
1. **Direct Failure on Problem 1**: Strict relational modeling requires predefined columns for every attribute. When `mock-pvmbg` is toggled via `POST /admin/schema-version` to inject `confidence_level`, an `INSERT` statement without an `ALTER TABLE` migration will fail with `column "confidence_level" does not exist`.
2. **Sparse Column Anti-Pattern**: Pre-allocating hundreds of nullable columns for prospective future agency fields degrades query cache density and creates severe database hygiene issues.

### Why PostgreSQL + JSONB is the Optimal Solution
PostgreSQL 16 combines the structural rigor of relational databases with the schemaless flexibility of document stores:
1. **Strict Envelope Indexing**: High-frequency filter fields (`occurred_at DESC`, `hazard_type`, `severity`, `source`) live in dedicated columns indexed with standard B-Trees.
2. **Dynamic Evolution in `attributes`**: The `attributes JSONB` column stores arbitrary nested JSON. Unrecognized fields sent by BMKG or PVMBG are stored without DDL migrations and can be queried using Postgres JSON operators (`attributes->>'confidence_level'`) or indexed with GIN if needed.
3. **Lightweight ACID Outbox**: Writing to `hazards` and `outbox_events` happens in a single `BEGIN ... COMMIT` block. If the server crashes mid-flight, both writes roll back cleanly.

---

## Schema Architecture

> [!NOTE]
> This is a verbatim copy of `services/aggregator/migrations/001_init.sql` — the actual mounted migration file is the single source of truth. If you change one, update the other; they must never diverge.

```sql
-- Migration 001_init.sql: Core Aggregator schema and volcano reference seed data
-- Database target: PostgreSQL 16 Alpine

-- 1. Hazards Table
CREATE TABLE IF NOT EXISTS hazards (
    hazard_id VARCHAR(128) PRIMARY KEY,
    source VARCHAR(32) NOT NULL,
    source_ref_id VARCHAR(128) NOT NULL,
    hazard_type VARCHAR(32) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    area_name TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_source_ref UNIQUE (source, source_ref_id)
);

-- Indexes for hazards query performance
CREATE INDEX IF NOT EXISTS idx_hazards_occurred_at ON hazards(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hazards_type_occurred ON hazards(hazard_type, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hazards_severity_occurred ON hazards(severity, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hazards_attributes_gin ON hazards USING gin(attributes);

-- 2. Volcano References Table
CREATE TABLE IF NOT EXISTS volcano_references (
    volcano_id VARCHAR(32) PRIMARY KEY,
    volcano_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

-- Seed data for 6 Indonesian volcanoes
INSERT INTO volcano_references (volcano_id, volcano_name, latitude, longitude) VALUES
    ('VLC-001', 'Gunung Merapi', -7.5407, 110.4456),
    ('VLC-002', 'Gunung Anak Krakatau', -6.1020, 105.4230),
    ('VLC-003', 'Gunung Semeru', -8.1077, 112.9224),
    ('VLC-004', 'Gunung Marapi', -0.3811, 100.4739),
    ('VLC-005', 'Gunung Sinabung', 3.1694, 98.3925),
    ('VLC-006', 'Gunung Ruang', 2.3014, 125.3697)
ON CONFLICT (volcano_id) DO UPDATE SET
    volcano_name = EXCLUDED.volcano_name,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;

-- 3. Transactional Outbox Table
CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL UNIQUE,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    correlation_id VARCHAR(64) NOT NULL DEFAULT '',
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    claim_expires_at TIMESTAMPTZ
);

-- Idempotent column addition and alterations for existing volumes
ALTER TABLE hazards ALTER COLUMN hazard_id TYPE VARCHAR(128);
ALTER TABLE hazards ALTER COLUMN source_ref_id TYPE VARCHAR(128);
ALTER TABLE outbox_events ALTER COLUMN event_id TYPE VARCHAR(128);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(64) NOT NULL DEFAULT '';
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;

-- Partial index for high-throughput polling of unpublished events
CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON outbox_events(id) WHERE published = FALSE;
```

---

## Transactional Outbox Worker Loop

The outbox relay (`services/aggregator/internal/worker/outbox_relay.go`, with the claim and completion queries in `internal/repository/hazard_repo.go`, `RelayOutboxBatch`) guarantees **at-least-once message delivery** without dual-write failure. A tick (default 500 ms) runs three phases so that no database lock is held while talking to the broker:

1. **Claim (short transaction)**: select up to 50 unpublished rows whose lease is empty or expired and set a 30-second lease on them.
   ```sql
   WITH claimable AS (
       SELECT id FROM outbox_events
       WHERE published = FALSE AND (claim_expires_at IS NULL OR claim_expires_at < NOW())
       ORDER BY id ASC LIMIT $1
       FOR UPDATE SKIP LOCKED
   )
   UPDATE outbox_events o
   SET claim_expires_at = NOW() + INTERVAL '30 seconds'
   FROM claimable c WHERE o.id = c.id
   RETURNING o.id, o.event_id, o.event_type, o.payload, o.correlation_id, o.published, o.created_at, o.published_at;
   ```
2. **Publish (outside any transaction)**: for each row the relay reads `hazard_id` from the payload and writes a Kafka message with `Key = hazard_id`, `Value = payload` and an `X-Correlation-ID` header (falling back to `outbox-relay-<id>`). Rows whose payload is malformed or lacks `hazard_id` are quarantined instead of published. If the broker write fails, the lease is cleared (`claim_expires_at = NULL`) and the batch is retried on the next tick.
3. **Complete (short transaction)**: published rows are set to `published = TRUE` only if the payload is unchanged (`WHERE id = $1 AND payload = $2::jsonb`), so a row whose payload was replaced mid-flight is not marked as published; quarantined rows are marked `published = TRUE` with `event_type = 'hazard_event_quarantined'`; any other claimed rows have their lease cleared.

This architecture ensures zero dual-write vulnerabilities:
* If the aggregator crashes immediately after inserting a hazard, the outbox record is already committed in Postgres and is dispatched after restart. A crash between publish and completion leaves the lease to expire after 30 seconds, after which the row is re-sent (hence the at-least-once guarantee and the mandatory idempotent consumers in [ADR 002](002-message-broker-redpanda.md)).
* If Redpanda is down, the relay retries on every tick (fixed interval, no exponential backoff) while the database remains healthy and capable of accepting queries.
