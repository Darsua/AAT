# Service: Aggregator Core

**Assigned Owner:** Member B (Darrel)  
**Module Name:** `bnpb/aggregator`  
**Internal Port:** `8083`  
**Dependencies:** `github.com/jackc/pgx/v5`, `github.com/segmentio/kafka-go`

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
   - Deduplicate incoming events using composite unique key `(source, source_event_id)` to ensure idempotent ingestion.

3. **Transactional Outbox Pattern:**
   - In a single PostgreSQL ACID transaction (`pgx.Tx`):
     - Insert/Upsert normalized `hazards`.
     - Insert message into `outbox_events` with status `published = FALSE`.
   - Outbox publisher loop asynchronously drains unpublished records, publishes them to Redpanda topic `bnpb.hazard-events` using `segmentio/kafka-go`, and marks them `published = TRUE`.
   - Guarantees **at-least-once delivery** to downstream consumers without dual-write inconsistency.

4. **Internal Serving Interface:**
   - Expose `GET /internal/hazards` on port `8083` for `client-api` facade consumption.
   - Support query parameters: `type`, `severity`, `since`, `limit`.

---

## 3. Database Schema Design (PostgreSQL)

Mounted automatically from [`migrations/001_init.sql`](file:///home/darrel/Projects/AAT/services/aggregator/migrations/001_init.sql):

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

## 4. Configuration & Environment Variables

| Variable | Default Value | Description |
|---|---|---|
| `PORT` | `8083` | Internal HTTP listening port |
| `DATABASE_URL` | `postgres://bnpb_user:bnpb_secret@canonical-db:5432/bnpb_hazards?sslmode=disable` | PostgreSQL connection string |
| `REDPANDA_BROKERS` | `redpanda:9092` | Redpanda / Kafka broker addresses |
| `BMKG_URL` | `http://mock-bmkg:8081` | Upstream BMKG service base URL |
| `BMKG_KEY` | `bmkg-secret-key-2026` | API Key for BMKG feed (`X-BMKG-Key`) |
| `PVMBG_URL` | `http://mock-pvmbg:8082` | Upstream PVMBG service base URL |
| `PVMBG_TOKEN` | `pvmbg-bearer-token-2026` | Bearer token for PVMBG feed |
| `POLL_INTERVAL_SECS` | `3` | Frequency (in seconds) of upstream polling |

---

## 5. Domain Models Reference

Defined in [`internal/domain/models.go`](file:///home/darrel/Projects/AAT/services/aggregator/internal/domain/models.go) (matching [`contracts/hazard_event.json`](file:///home/darrel/Projects/AAT/contracts/hazard_event.json)):
- `Source`: `BMKG`, `PVMBG`.
- `HazardType`: `SEISMIC`, `VOLCANIC`.
- `Severity`: `NORMAL`, `WASPADA`, `SIAGA`, `AWAS`.
- `HazardEvent`: Canonical representation with Summary Fields (`hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at`) and Raw Fields (`source_ref_id`, `latitude`, `longitude`, `attributes`).
- `VolcanoReference`: Static coordinates for volcano enrichment (`volcano_id`, `volcano_name`, `latitude`, `longitude`).
- `OutboxEvent`: Encapsulates atomic message payload and publish state.

---

## 6. Development & Runbook

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

## 7. Implementation Plan for Darrel

- [ ] Wire `pgxpool.New(ctx, databaseURL)` with connection pooling configurations (MaxConns: 25, MinConns: 5).
- [ ] Implement database migrations (auto-migrate or migrate tool).
- [ ] Implement BMKG poller worker (`internal/ingest/bmkg.go`).
- [ ] Implement PVMBG poller worker with circuit breaker and schema adapter (`internal/ingest/pvmbg.go`).
- [ ] Implement atomic ingestion repository inserting into `hazards` and `outbox_events`.
- [ ] Implement outbox publisher loop using `kafka-go.Writer` streaming to `bnpb.hazard-events`.
- [ ] Implement `GET /internal/hazards` handler querying PostgreSQL with optional pagination & filtering.
