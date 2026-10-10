# ADR 005: Stateless Service Architecture and Milestone 2 Scalability Roadmap

---

## Context
IF4031 is structured across progressive milestones. Milestone 1 establishes the baseline distributed architecture (six services, canonical storage, broker event fan-out, authentication scopes, and resilience under basic failure). Milestone 2 expands the platform to handle:
* **Horizontal multi-instance scaling** across compute services.
* **Volume spikes and high-throughput traffic bursts**.
* **Cascading failures and network partitions** requiring circuit breaking and fallback strategies.
* **Cache layers and distributed rate limiting**.

Authoritative state should live in durable infrastructure before services are scaled horizontally. Milestone 1 may use bounded process-local state where the trade-off is explicit, but that state must be externalized before running multiple replicas.

---

## Decision
We keep the synchronous read path stateless, delegate canonical data and message offsets to `canonical-db` and `redpanda`, derive **deterministic, source-based hazard identifiers**, enforce **database-level idempotency constraints**, and decouple services via **domain interfaces**. The Milestone 1 Auth refresh-token store and consumer deduplication/counters are documented exceptions that must move to durable shared storage before those services are horizontally scaled.

```mermaid
flowchart TB
    subgraph M1["Milestone 1: single-instance baseline (implemented)"]
        direction LR
        C1["client-api :8080"]:::svc
        A1["aggregator :8083"]:::svc
        DB1[("canonical-db :5432")]:::store
        RP1[["redpanda :9092<br/>3 partitions"]]:::broker
        CG1["consumers<br/>1 replica per group"]:::cons
        C1 --> A1
        A1 --> DB1
        A1 --> RP1
        RP1 --> CG1
    end

    subgraph M2["Milestone 2: horizontal scale-out (plan only)"]
        direction LR
        LB["Nginx or Envoy<br/>load balancer"]:::plan
        CN["client-api<br/>replicas 1..N"]:::plan
        RED[("Redis<br/>read-through cache")]:::plan
        AN["aggregator"]:::plan
        RPN[["redpanda<br/>6 partitions"]]:::plan
        CGN["consumer workers<br/>share one group"]:::plan
        LB --> CN
        CN --> RED
        RED -->|"cache miss"| AN
        RPN --> CGN
    end
    M1 ~~~ M2

    classDef svc fill:#eaf1fb,stroke:#1f4e8c,color:#0b2545
    classDef store fill:#e8f5e9,stroke:#2e7d32,color:#10300f
    classDef broker fill:#fdecea,stroke:#c62828,color:#4a0d0d
    classDef ext fill:#fff3e0,stroke:#ef6c00,color:#4a2600
    classDef cons fill:#f3e5f5,stroke:#6a1b9a,color:#2a0a3a
    classDef actor fill:#ffffff,stroke:#616161,color:#212121
    classDef plan fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray:4 3,color:#424242
```

---

## Stateless Request Paths and Explicit Milestone 1 Exceptions

### 1. Request and Session State
* **`client-api`**: Holds no local state. Authentication is verified statelessly via cryptographic signature verification on self-contained JWT tokens (HMAC-SHA256 with 60-second expiration). Any incoming request can be served by any `client-api` container replica.
* **`auth`**: Issues signed JWTs containing user identities and a `scope` claim (`hazards:read:summary` or `hazards:read:all`; see `contracts/auth_api.yaml`). Access-token verification requires only the shared verification key. Refresh-token rotation is process-local in Milestone 1, so an Auth restart invalidates outstanding refresh tokens and multiple Auth replicas would require a shared Redis or database store.
* **Consumers**: Redpanda durably stores committed offsets, but the current deduplication sets and dashboard counters are process-local. They are sufficient for the single-replica Milestone 1 baseline; durable idempotency and counters must be externalized before multi-replica deployment.
* **`aggregator`**: Each poller keeps its cursor in process memory and bootstraps it from the latest `occurred_at` stored in `canonical-db` on start (`GetLatestOccurredAt`), re-reading with a 10-second overlap window. Restarting the aggregator therefore does not lose progress, and the overlap is made safe by the idempotent upsert below.

### 2. Deterministic Identifiers
`hazard_id` is not a random or sequential value. `formatHazardID` in `services/aggregator/internal/normalizer/normalizer.go` builds it from a source prefix and the upstream reference (for example `haz-bmkg-<event_id>` and `haz-pvmbg-<report_id>`), bounded in length. The outbox `event_id` is derived the same way from the source and reference (`formatOutboxEventID`).
* **Advantages**: no database round-trip or sequence is needed to generate an identifier, any replica derives the same identifier for the same upstream record, and re-ingesting a record updates the existing row instead of creating a duplicate.
* **Trade-off**: identifiers are not time-ordered, so inserts do not benefit from B-Tree append locality. Time-ordered keys such as UUIDv7 remain an option if insert contention becomes a measured problem in Milestone 2.

### 3. Database-Level Idempotency Guarantees
To prevent duplicate records during network retries or multi-instance polling, idempotency is enforced at the storage engine level:
```sql
CONSTRAINT uq_source_ref UNIQUE (source, source_ref_id)
```
Ingestion queries utilize atomic upserts (`ON CONFLICT (source, source_ref_id) DO UPDATE ...` or `DO NOTHING`). Even if multiple worker routines or replicas poll the same event simultaneously, duplicate records can never enter the canonical store.

---

## Milestone 2 Evolution Path

> **Status: plan only.** Nothing in this section is implemented in Milestone 1. Items here (replicas behind a load balancer, Redis read-through cache, circuit breaker, six partitions) are design intentions to be validated when Milestone 2 starts; they are not claims about the current code.

### 1. Multi-Instance Horizontal Scaling
* **`client-api` Scaling**:
  * Because `client-api` is 100% stateless, scaling from 1 to N instances requires only setting `deploy.replicas: N` in Docker Compose or Kubernetes, fronted by an Nginx or Envoy reverse proxy with round-robin routing.
* **Consumer Group Auto-Rebalancing**:
  * Kafka/Redpanda automatically rebalances partitions when multiple instances share a consumer group. Before scaling the current consumers, their process-local deduplication and side-effect state must move to shared durable storage so a rebalance or restart cannot repeat a non-idempotent action.

### 2. Traffic Spikes and Read-Through Caching
* During major disaster events, read queries on `GET /hazards` spike exponentially.
* In Milestone 1, `client-api` uses the decoupled `HazardService` interface (see [ADR 004](004-internal-communication-rest.md)):
  ```go
  type HazardService interface {
      GetHazards(ctx context.Context, filter QueryFilter) (*HazardResponse, error)
  }
  ```
* In Milestone 2, we introduce Redis read-through caching via a simple decorator pattern with **zero changes to HTTP handlers**:
  ```go
  type CachedHazardService struct {
      next  HazardService
      redis *redis.Client
  }

  func (c *CachedHazardService) GetHazards(ctx context.Context, filter QueryFilter) (*HazardResponse, error) {
      cacheKey := generateFilterKey(filter)
      if cached, err := c.redis.Get(ctx, cacheKey).Bytes(); err == nil {
          var resp HazardResponse
          json.Unmarshal(cached, &resp)
          return &resp, nil
      }
      resp, err := c.next.GetHazards(ctx, filter)
      if err == nil {
          c.redis.Set(ctx, cacheKey, resp, 5*time.Second)
      }
      return resp, err
  }
  ```

### 3. Fault Tolerance & Circuit Breaking
Problem 2 introduces external provider outages (e.g., PVMBG downtime).
* In Milestone 1, `aggregator` gracefully returns the last-known canonical state with a `degraded: true` and `stale_since` indicator.
* In Milestone 2, we wrap external HTTP polling with a **Circuit Breaker** (e.g., `sony/gobreaker`):
  ```mermaid
  stateDiagram-v2
      direction LR
      [*] --> Closed
      Closed --> Open: consecutive failures > threshold (e.g. 5)
      Open --> HalfOpen: cooldown expires (e.g. 30 s)
      HalfOpen --> Closed: probe succeeds
      HalfOpen --> Open: probe fails
  ```
  * **Closed State**: Requests flow normally to `mock-pvmbg`.
  * **Open State**: Immediate fallback to canonical cache; prevents polling worker threads from exhausting sockets or thundering-herd effects on recovering providers.
  * **Half-Open State**: Canary probe sent to verify provider recovery.

### 4. Topic Partitioning & High Throughput
In Milestone 1, `bnpb.hazard-events` uses three partitions provisioned by the one-shot `redpanda-init` Compose service. For Milestone 2:
* Increase partitions from 3 to 6.
* Partition messages by key (`Key: []byte(event.HazardType)` or `Key: []byte(event.AreaName)`). Note that Milestone 1 keys by `hazard_id`, which orders updates per hazard (see [ADR 002](002-message-broker-redpanda.md)); changing the key changes the ordering guarantee from per hazard to per category or area, so this trade-off must be re-evaluated before the change.
* Would give chronological ordering per hazard category while enabling 6 concurrent consumer workers per group.

---

## Summary of Architectural Continuity

| Architecture Aspect | Milestone 1 Foundation | Milestone 2 Evolution | Code Changes Required |
| :--- | :--- | :--- | :--- |
| **Identity Generation** | Deterministic `hazard_id` derived from source and source reference | Same scheme (replica-safe because it needs no coordination) | None |
| **Data Idempotency** | PostgreSQL Unique Constraints | Distributed Deduplication Table | None |
| **Client Gateway** | Single stateless `client-api` | N instances behind Envoy | Compose configuration only |
| **Auth Refresh State** | Process-local rotation store | Shared Redis or database store | Replace refresh-token store |
| **Consumer Scaling** | Single worker per group with process-local deduplication | Multi-worker consumer group with shared idempotency state | Externalize deduplication and side effects |
| **Query Acceleration** | Direct DB indexing | Redis decorator on `HazardService` | Add 1 decorator struct |
| **Provider Outages** | Polling retry + Degraded flag | Dynamic Circuit Breaker | Add wrapper in polling client |
