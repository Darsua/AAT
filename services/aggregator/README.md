# Service: Aggregator Core

**Assigned Owner:** Member B (Darrel)  
**Module Name:** `bnpb/aggregator`  
**Internal Port:** `8083`  
**Dependencies:** `github.com/jackc/pgx/v5`, `github.com/segmentio/kafka-go`  
**Contracts:** [`contracts/aggregator_internal.yaml`](../../contracts/aggregator_internal.yaml), [`contracts/hazard_event.json`](../../contracts/hazard_event.json)

---

## 1. System Architecture & Role

The `aggregator` service is the central ingestion and event-sourcing engine in the BNPB disaster management platform. It operates strictly on the internal private network (not exposed to the public internet).

```
 +--------------------+       +--------------------+
 |  mock-bmkg (:8081) |       | mock-pvmbg (:8082) |
 +--------------------+       +--------------------+
           \                           /
      HTTP  \                         /  HTTP
   (Poller)  \                       /  (Poller)
              v                     v
       +------------------------------------+
       |          Aggregator (:8083)        |
       |  - Upstream Poller Workers         |
       |  - Normalization Engine            |
       |  - Transactional Outbox Relay      |
       +------------------------------------+
              |                      ^
   ACID Write |                      | Internal Read
   (Event +   |                      | (Proxied by client-api)
    Outbox)   v                      |
       +--------------+     +--------------------+
       |  PostgreSQL  |     | client-api (:8080) |
       +--------------+     +--------------------+
              |
         Outbox Poll
              |
              v
       +------------------------------------+
       |  Redpanda / Kafka (:9092)          |
       |  Topic: bnpb.hazard-events         |
       +------------------------------------+
              /                       \
             v                         v
   +---------------------+   +--------------------+
   | Dashboard Consumer  |   | Notifier Consumer  |
   +---------------------+   +--------------------+
```

---

## 2. Core Responsibilities

1. **Upstream Ingestion & Polling Workers:**
   - Periodically poll `mock-bmkg:8081/seismic-events` (using header `X-BMKG-Key`) and `/tsunami-warnings`.
   - Periodically poll `mock-pvmbg:8082/volcanic-reports` (using `Authorization: Bearer <token>`).
   - Implement exponential backoff, jitter, and circuit breaking against PVMBG latency and outages.
   - Handle schema drift gracefully when PVMBG toggles schema versions.

2. **Deduplication & Canonical Normalization:**
   - Transform disparate upstream payloads into the canonical `domain.HazardEvent` struct.
   - Deduplicate incoming events using composite unique key `(source, source_ref_id)` to ensure idempotent ingestion (matches the `uq_source_ref` constraint in §4).

3. **Transactional Outbox Pattern:**
   - In a single PostgreSQL ACID transaction (`pgx.Tx`):
     - Insert/Upsert normalized `hazards`.
     - Insert message into `outbox_events` with status `published = FALSE`.
   - Outbox publisher loop asynchronously drains unpublished records, publishes them to Redpanda topic `bnpb.hazard-events` using `segmentio/kafka-go`, and marks them `published = TRUE`.
   - Guarantees **at-least-once delivery** to downstream consumers without dual-write inconsistency.
   - **Message envelope (frozen — required by both producer and consumers):**
     - **Key:** `hazard_id` (UTF-8 string). Ensures all updates to the same hazard land on the same partition and preserve order.
     - **Value:** The canonical `HazardEvent` JSON object, exactly as defined in `contracts/hazard_event.json` — no additional wrapper, envelope object, or metadata fields. Consumers unmarshal the message value directly into `HazardEvent`.
     - **Headers:** `X-Correlation-ID` (string, optional) carrying the correlation ID from the ingestion request that produced this event, for end-to-end tracing.
     - No `event_type` discriminator is needed on the wire since the topic carries only one message shape (`HazardEvent`); `outbox_events.event_type` is an internal-only bookkeeping column and is never published.

4. **Internal Serving Interface:**
   - Expose `GET /internal/hazards` on port `8083` for `client-api` facade consumption.
   - Support query parameters: `type`, `severity`, `since`, `limit`.

---

## 3. Canonical Mapping Rules & Event Envelope (Frozen)

These are the exact, graded mapping rules from the official M1 spec. They are frozen — do not deviate without updating this document and the report.

### 3.1 `SeismicEvent` -> `HazardEvent`

| Canonical field | Derived from |
|---|---|
| `source_ref_id` | `event_id` |
| `hazard_type` | Always `SEISMIC` |
| `area_name` | `region_name` |
| `latitude`, `longitude` | `epicenter_lat`, `epicenter_lon` |
| `occurred_at` | `occurred_at` |
| `severity` | See §3.3 below |
| `attributes` | `magnitude`, `depth_km`, `potential_tsunami`, plus the correlated `TsunamiWarning` fields (as `tsunami_warning`) if one exists for this event |

### 3.2 `VolcanicReport` -> `HazardEvent`

| Canonical field | Derived from |
|---|---|
| `source_ref_id` | `report_id` |
| `hazard_type` | Always `VOLCANIC` |
| `area_name` | Volcano name, looked up from `volcano_id` via the static `volcano_references` table |
| `latitude`, `longitude` | Static coordinates from `volcano_references`, looked up by `volcano_id` (NOT from the upstream payload — PVMBG does not provide coordinates) |
| `occurred_at` | `reported_at` |
| `severity` | `alert_level` mapped 1:1 to the canonical enum (`Normal`→`NORMAL`, `Waspada`→`WASPADA`, `Siaga`→`SIAGA`, `Awas`→`AWAS`) |
| `attributes` | `eruption_count_24h`, `ash_column_height_m`, plus `confidence_level` if present in the upstream payload |

### 3.3 Severity Derivation for Seismic Events

Applied in this exact priority order:
1. If a correlated `TsunamiWarning` exists with `threat_level == Awas` -> severity is `AWAS`.
2. Else if a correlated `TsunamiWarning` exists -> severity follows that warning's `threat_level` directly (`Waspada`→`WASPADA`, `Siaga`→`SIAGA`).
3. Else (no tsunami warning at all) -> derive from `magnitude` alone:
   - `magnitude < 5.0` -> `NORMAL`
   - `5.0 <= magnitude < 6.5` -> `WASPADA`
   - `magnitude >= 6.5` -> `SIAGA`
   - (Note: `AWAS` for a seismic event is only ever reached via an `Awas`-level tsunami warning, never from magnitude alone.)

Correlation between a `SeismicEvent` and its `TsunamiWarning` is done via `TsunamiWarning.related_event_id == SeismicEvent.event_id`.

### 3.4 Unknown / Unmapped Field Policy (Declared per P1 Criterion 4)

> **Policy: additive-forward.** Any upstream field that has no canonical counterpart above — including fields that do not exist yet at ingestion-start time, such as PVMBG's `confidence_level` introduced later via `POST /admin/schema-version` — is forwarded verbatim into `attributes` (JSONB). No unknown field is ever dropped, and no unknown field ever causes ingestion to fail. Fields that DO have a canonical counterpart (e.g. `magnitude`, `depth_km`) are placed into `attributes` explicitly as documented in §3.1/§3.2 above, not duplicated arbitrarily.
>
> Explicitly **not** supported: removal of a previously-required field, or a type change to an existing field (e.g. `alert_level` changing from string enum to integer). Both are considered breaking, non-additive changes outside this policy's scope, consistent with the additive-only JSON schema evolution declared in ADR 004.

### 3.5 Kafka Message Envelope (Frozen)

Shared contract between the Aggregator publisher and all downstream consumers:
- **Key:** `hazard_id` (UTF-8 string). Ensures all updates to the same hazard land on the same Kafka partition to preserve ordering.
- **Value:** The canonical `HazardEvent` JSON object, exactly as defined in `contracts/hazard_event.json` — no additional wrapper object, envelope object, or envelope metadata. Consumers unmarshal the message payload directly into `HazardEvent`.
- **Headers:** `X-Correlation-ID` (string, optional) carrying the correlation ID from the ingestion request that produced this event, for end-to-end tracing.
- No `event_type` discriminator is needed on the wire since the topic carries only one message shape (`HazardEvent`); `outbox_events.event_type` is an internal-only bookkeeping column and is never published.

---

## 4. Database Schema Design (PostgreSQL)

Mounted automatically from [`migrations/001_init.sql`](migrations/001_init.sql):

```sql
-- 1. Canonical Hazards Table (Envelope + JSONB Attributes)
CREATE TABLE IF NOT EXISTS hazards (
    hazard_id VARCHAR(64) PRIMARY KEY,
    source VARCHAR(32) NOT NULL,
    source_ref_id VARCHAR(64) NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_hazards_occurred_at ON hazards(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hazards_type_occurred ON hazards(hazard_type, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hazards_attributes_gin ON hazards USING gin(attributes);

-- 2. Volcano Reference Catalog (Static Coordinates Enrichment)
CREATE TABLE IF NOT EXISTS volcano_references (
    volcano_id VARCHAR(32) PRIMARY KEY,
    volcano_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

-- 3. Transactional Outbox (Guarantees At-Least-Once Delivery to Redpanda)
CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON outbox_events(id) WHERE published = FALSE;
```

---

## 5. Configuration & Environment Variables

| Variable | Default Value | Description |
|---|---|---|
| `PORT` | `8083` | Internal HTTP listening port |
| `DATABASE_URL` | `postgres://bnpb_user:bnpb_secret@canonical-db:5432/bnpb_hazards?sslmode=disable` | PostgreSQL connection string |
| `REDPANDA_BROKERS` | `redpanda:9092` | Redpanda / Kafka broker addresses |
| `TOPIC` | `bnpb.hazard-events` | Outbox publish target topic (set via `KAFKA_TOPIC_HAZARDS` in `.env`; see §3.5 for the message envelope) |
| `BMKG_URL` | `http://mock-bmkg:8081` | Upstream BMKG service base URL |
| `BMKG_KEY` | `bmkg-secret-key-2026` | API Key for BMKG feed (`X-BMKG-Key`) |
| `PVMBG_URL` | `http://mock-pvmbg:8082` | Upstream PVMBG service base URL |
| `PVMBG_TOKEN` | `pvmbg-bearer-token-2026` | Bearer token for PVMBG feed |
| `POLL_INTERVAL_SECS` | `3` | Frequency (in seconds) of upstream polling |

---

## 6. Domain Models Reference

Defined in [`internal/domain/models.go`](internal/domain/models.go) (matching [`contracts/hazard_event.json`](../../contracts/hazard_event.json)):
- `Source`: `BMKG`, `PVMBG`.
- `HazardType`: `SEISMIC`, `VOLCANIC`.
- `Severity`: `NORMAL`, `WASPADA`, `SIAGA`, `AWAS`.
- `HazardEvent`: Canonical representation with Summary Fields (`hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at`) and Raw Fields (`source_ref_id`, `latitude`, `longitude`, `attributes`).
- `VolcanoReference`: Static coordinates for volcano enrichment (`volcano_id`, `volcano_name`, `latitude`, `longitude`).
- `OutboxEvent`: Encapsulates atomic message payload and publish state.

---

## 7. Development & Runbook

### Local Execution (Prerequisites: PostgreSQL and Redpanda running)
```bash
# In services/aggregator
export DATABASE_URL="postgres://bnpb_user:bnpb_secret@localhost:5432/bnpb_hazards?sslmode=disable"
export REDPANDA_BROKERS="localhost:19092"
export BMKG_URL="http://localhost:8081"
export PVMBG_URL="http://localhost:8082"

go run cmd/server/main.go
```

### Build & Run with Docker
```bash
docker build -t bnpb/aggregator:latest services/aggregator/
docker run -p 8083:8083 \
  -e DATABASE_URL="postgres://bnpb_user:bnpb_secret@canonical-db:5432/bnpb_hazards?sslmode=disable" \
  -e REDPANDA_BROKERS="redpanda:9092" \
  bnpb/aggregator:latest
```

### Verification Endpoints
```bash
# 1. Health check
curl -i http://localhost:8083/health

# 2. Query internal hazards
curl -i http://localhost:8083/internal/hazards
```

---

## 8. Implementation Plan for Darrel

- [x] Wire `pgxpool.New(ctx, databaseURL)` with connection pooling configurations (MaxConns: 25, MinConns: 5).
- [x] Implement database migrations (auto-migrate or migrate tool).
- [x] Implement BMKG poller worker (`internal/worker/poller.go`).
- [x] Implement PVMBG poller worker with circuit breaker and schema adapter (`internal/worker/poller.go`).
- [x] Implement atomic ingestion repository inserting into `hazards` and `outbox_events` (`internal/repository/hazard_repo.go`).
- [x] Implement outbox publisher loop using `kafka-go.Writer` streaming to `bnpb.hazard-events` (`internal/worker/outbox_relay.go`).
- [x] Implement `GET /internal/hazards` handler querying PostgreSQL with optional pagination & filtering (`internal/api/hazard_handler.go`).
