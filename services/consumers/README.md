# Pub-Sub Consumers: Dashboard & Emergency Notifier

**Assigned Owner:** Member C (Jenka)  
**Parent Directory:** `services/consumers/`  
**Dependencies:** `github.com/segmentio/kafka-go`  
**Broker:** Redpanda (`redpanda:9092`)  
**Topic:** `bnpb.hazard-events`  

---

## 1. Architecture Overview

The BNPB platform uses an asynchronous Publish-Subscribe model backed by Redpanda (Kafka-compatible event store) to decouple real-time disaster alerts from downstream processing systems.

```
       +------------------------------------+
       |         aggregator (:8083)         |
       |     Transactional Outbox Relay     |
       +------------------------------------+
                         |
                         | Publishes Hazard Events
                         v
       +------------------------------------+
       |      Redpanda / Kafka (:9092)      |
       |     Topic: bnpb.hazard-events      |
       +------------------------------------+
                   /            \
                  /              \
       bnpb-dashboard-group    bnpb-notifier-group
                /                  \
               v                    v
+------------------------+   +------------------------+
|   Dashboard Consumer   |   |   Notifier Consumer    |
| - Live incident stats  |   | - Urgent alert filters |
| - Geo analytics        |   | - SMS/Push/Dispatches  |
+------------------------+   +------------------------+
```

---

## 2. Consumer Group Specifications

The specification mandates at least **2 independent downstream consumer services** with distinct consumer groups. Here are the recommended reference consumer roles (which you can structure, name, and implement as you see fit under `services/consumers/`):

### 2.1 `bnpb-dashboard-group` (Reference: e.g. `dashboard/`)
- **Role:** Ingests all hazard events to update live situational awareness dashboards, aggregation counts, and historical geo-visualizations.
- **Processing Characteristics:** Bulk throughput, resilient to brief rendering delays.

### 2.2 `bnpb-notifier-group` (Reference: e.g. `notifier/`)
- **Role:** Real-time alerting engine. Filters incoming hazards for critical criteria (e.g. Earthquakes M ≥ 6.0, Tsunami Warnings, Volcano Alert Level `AWAS`/`SIAGA`).
- **Processing Characteristics:** Low latency, triggers emergency dispatches (SMS gateway, Webhooks, Push notifications) to BNPB field teams.

> [!IMPORTANT]
> Because each service uses an independent Consumer Group ID (`bnpb-dashboard-group` vs `bnpb-notifier-group`), Redpanda tracks separate offset pointers for each group. Downtime or processing slowness in one service never affects or starves the other! Once you create your consumer services, uncomment the corresponding service blocks in root `docker-compose.yml`!

---

## 3. Deduplication & Idempotency Requirements

Because the aggregator uses an **At-Least-Once** outbox delivery mechanism, consumer failures or broker rebalances can deliver duplicate messages.

Consumers must guarantee **Idempotent Processing**:
1. **Deterministic Unique Key:** Extract canonical event ID `event.HazardID` (JSON: `hazard_id`) or composite key `event.Source + ":" + event.SourceRefID`.
2. **Deduplication Store:** Before executing non-idempotent actions (such as sending an SMS to rescue teams or incrementing an incident counter):
   - Check if `event.HazardID` has already been marked as processed in an in-memory set (with LRU/TTL expiration) or Redis `SETNX event:<hazard_id> 1 EX 86400`.
   - If present: Skip side-effect execution and commit the message offset immediately.
   - If new: Execute side-effect and mark the ID as processed.

---

## 4. Downtime Recovery Testing Procedure

To prove fault tolerance and message durability, execute this verification runbook:

### Step 1: Start Up Infrastructure
```bash
# From workspace root (with docker compose)
docker compose up -d redpanda canonical-db aggregator dashboard-consumer notifier-consumer
```

### Step 2: Verify Initial Healthy Streaming
Verify both consumer containers are connected to Redpanda topic `bnpb.hazard-events` and actively reading.
```bash
docker compose logs -f dashboard-consumer
docker compose logs -f notifier-consumer
```

### Step 3: Simulate Consumer Downtime
Stop the `dashboard-consumer` container to simulate a service crash or maintenance window:
```bash
docker compose stop dashboard-consumer
```

### Step 4: Ingest Events While Consumer is Down
Generate or poll new hazard events through `aggregator`:
```bash
# Trigger an event ingestion in aggregator or wait for poller cycle
# Check Redpanda topic lag for consumer groups (from host use port 19092):
rpk group describe bnpb-dashboard-group --brokers localhost:19092
rpk group describe bnpb-notifier-group --brokers localhost:19092
```
*Expected Result:*
- `bnpb-notifier-group` lag remains **0** (actively processing and logging dispatches).
- `bnpb-dashboard-group` lag **accumulates** (messages are safely buffered on the partition log).

### Step 5: Recover the Offline Consumer
Restart the stopped container:
```bash
docker compose start dashboard-consumer
docker compose logs -f dashboard-consumer
```

### Step 6: Verify Recovery & Zero Data Loss
1. Confirm `dashboard-consumer` reconnects with `GroupID: bnpb-dashboard-group`.
2. Confirm it resumes reading from the **last committed offset** (no messages skipped).
3. Confirm all backlogged events are processed and committed.
4. Check group lag returns to **0**:
```bash
rpk group describe bnpb-dashboard-group --brokers localhost:19092
```

---

## 5. Development Runbook

### Environment Variables
| Variable | Default Value | Description |
|---|---|---|
| `REDPANDA_BROKERS` | `localhost:19092` (host) or `redpanda:9092` (Docker) | Comma-separated broker endpoints |
| `TOPIC` | `bnpb.hazard-events` | Target topic |
| `CONSUMER_GROUP` | `bnpb-dashboard-group` / `bnpb-notifier-group` | Unique consumer group identifier |

### Running Locally
```bash
# Run dashboard consumer
cd services/consumers/dashboard
export REDPANDA_BROKERS="localhost:19092"
export TOPIC="bnpb.hazard-events"
export CONSUMER_GROUP="bnpb-dashboard-group"
go run main.go

# In a separate terminal, run notifier consumer
cd services/consumers/notifier
export REDPANDA_BROKERS="localhost:19092"
export TOPIC="bnpb.hazard-events"
export CONSUMER_GROUP="bnpb-notifier-group"
go run main.go
```

### Build Docker Images
```bash
docker build -t bnpb/consumer-dashboard:latest services/consumers/dashboard/
docker build -t bnpb/consumer-notifier:latest services/consumers/notifier/
```

---

## 6. Implementation Checklist for Jenka

- [ ] Implement JSON unmarshaling into canonical `HazardEvent` model (matching `contracts/hazard_event.json`).
- [ ] Implement an in-memory or Redis-backed idempotency cache (keyed by `hazard_id`).
- [ ] `dashboard`: Implement aggregation logic (e.g. count of active events by type and severity).
- [ ] `notifier`: Implement alert filter rule engine (thresholds: M ≥ 6.0, Tsunami SIAGA/AWAS, Volcano AWAS).
- [ ] Ensure offsets are committed only after successful processing (`CommitMessages`).
