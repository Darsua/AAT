# ADR 005: Stateless Service Architecture and Milestone 2 Scalability Roadmap

---

## Context
IF4031 is structured across progressive milestones. Milestone 1 establishes the baseline distributed architecture (six services, canonical storage, broker event fan-out, authentication scopes, and resilience under basic failure). Milestone 2 expands the platform to handle:
* **Horizontal multi-instance scaling** across compute services.
* **Volume spikes and high-throughput traffic bursts**.
* **Cascading failures and network partitions** requiring circuit breaking and fallback strategies.
* **Cache layers and distributed rate limiting**.

An architecture that relies on in-memory state, non-deterministic identifiers, or tight temporal coupling in Milestone 1 will require extensive, risky rewrites in Milestone 2. We must ensure every design decision in Milestone 1 inherently supports zero-rewrite evolution into Milestone 2.

---

## Decision
We mandate **complete statelessness for all compute services**, delegate state exclusively to the persistent storage layer (`canonical-db`) and message broker (`redpanda`), adopt **time-ordered UUIDv7 identifiers**, enforce **database-level idempotency constraints**, and decouple services via **domain interfaces**.

```mermaid
flowchart TD
    subgraph M1Baseline["Milestone 1: Single-Instance Baseline"]
        C1["client-api:8080"]
        A1["aggregator:8083"]
        DB1[("canonical-db:5432\nPostgreSQL")]
        RP1[("redpanda:9092\nPartition 0")]
        CG1["consumers (1 replica)"]
    end

    subgraph M2Evolution["Milestone 2: Horizontal Scale-Out (Zero Rewrite)"]
        LB["Nginx / Envoy Load Balancer"]
        C_N1["client-api (Replica 1)"]
        C_N2["client-api (Replica 2)"]
        Redis[("Redis Cluster\n(Read-Through Cache)")]
        A_N1["aggregator (Replica 1 - Leader)"]
        A_N2["aggregator (Replica 2 - Follower)"]
        RP_N[("redpanda:9092\nPartitions 0, 1, 2")]
        CG_N1["consumer (Worker 1)"]
        CG_N2["consumer (Worker 2)"]
    end

    LB --> C_N1 & C_N2
    C_N1 & C_N2 --> Redis
    Redis --> A_N1 & A_N2
    RP_N --> CG_N1 & CG_N2
```

---

## Architectural Principles for Statelessness

### 1. Zero In-Memory Session State in Compute Services
* **`client-api`**: Holds no local state. Authentication is verified statelessly via cryptographic signature verification on self-contained JWT tokens (HMAC-SHA256 with 60-second expiration). Any incoming request can be served by any `client-api` container replica.
* **`auth`**: Issues signed JWTs containing user identities and scopes (`media`, `field`, `internal`). Token verification requires only the shared verification key, eliminating central session lookup bottlenecks during high-throughput queries.
* **`aggregator`**: Polling cursors and ingestion timestamps are committed transactionally to `canonical-db`. Restarting or replicating the aggregator does not lose cursor progress.

### 2. Time-Ordered Identifiers (UUIDv7)
Traditional sequential auto-increment integer IDs (`SERIAL`) require central database sequences, causing contention during high-concurrency inserts across distributed instances. Traditional random UUIDv4 identifiers cause B-Tree index fragmentation and poor cache locality.
* **Solution**: We adopt **UUIDv7** for all `hazard_id` generation. UUIDv7 combines a 48-bit millisecond Unix timestamp with 74 bits of cryptographically random data:
  ```
  018e6e5b-3b4e-7b3b-8f3a-2a1c9e8d4f00
  |--- Timestamp ---|-- Ver --|-- Rand --|
  ```
* **Advantages**:
  * Globally unique and generated locally in Go with zero database round-trips.
  * Monotonically increasing: Maintains sequential B-Tree append locality, maximizing PostgreSQL insert performance and reducing table bloat.

### 3. Database-Level Idempotency Guarantees
To prevent duplicate records during network retries or multi-instance polling, idempotency is enforced at the storage engine level:
```sql
CONSTRAINT uq_source_ref UNIQUE (source, source_ref_id)
```
Ingestion queries utilize atomic upserts (`ON CONFLICT (source, source_ref_id) DO UPDATE ...` or `DO NOTHING`). Even if multiple worker routines or replicas poll the same event simultaneously, duplicate records can never enter the canonical store.

---

## Milestone 2 Evolution Path (Zero-Rewrite Roadmap)

### 1. Multi-Instance Horizontal Scaling
* **`client-api` Scaling**:
  * Because `client-api` is 100% stateless, scaling from 1 to $N$ instances requires only setting `deploy.replicas: N` in Docker Compose or Kubernetes, fronted by an Nginx or Envoy reverse proxy with round-robin routing.
* **Consumer Group Auto-Rebalancing**:
  * In Kafka/Redpanda, scaling downstream consumers is completely native. When multiple container instances share the same `group_id` (`group.dashboard`), Redpanda automatically rebalances topic partitions across all active consumer instances without code modifications.

### 2. Traffic Spikes and Read-Through Caching
* During major disaster events, read queries on `GET /hazards` spike exponentially.
* In Milestone 1, `client-api` uses the decoupled `HazardService` interface (see [ADR 004](file:///home/darrel/Projects/AAT/docs/adr/004-internal-communication-rest.md)):
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
      [*] --> Closed
      Closed --> Open: Consecutive Failures > Threshold (e.g. 5)
      Open --> HalfOpen: Cooldown Timer Expires (e.g. 30s)
      HalfOpen --> Closed: Probe Request Succeeds
      HalfOpen --> Open: Probe Request Fails
  ```
  * **Closed State**: Requests flow normally to `mock-pvmbg`.
  * **Open State**: Immediate fallback to canonical cache; prevents polling worker threads from exhausting sockets or thundering-herd effects on recovering providers.
  * **Half-Open State**: Canary probe sent to verify provider recovery.

### 4. Topic Partitioning & High Throughput
In Milestone 1, `bnpb.hazard-events` uses a single partition. For Milestone 2:
* Increase partitions from 1 to 6.
* Partition messages by key (`Key: []byte(event.HazardType)` or `Key: []byte(event.AreaName)`).
* Guarantees strict chronological ordering per hazard category while enabling 6 concurrent consumer workers per group.

---

## Summary of Architectural Continuity

| Architecture Aspect | Milestone 1 Foundation | Milestone 2 Evolution | Code Changes Required |
| :--- | :--- | :--- | :--- |
| **Identity Generation** | UUIDv7 (time-ordered) | Distributed UUIDv7 | None |
| **Data Idempotency** | PostgreSQL Unique Constraints | Distributed Deduplication Table | None |
| **Client Gateway** | Single stateless `client-api` | N instances behind Envoy | Compose configuration only |
| **Consumer Scaling** | Single worker per group | Multi-worker Consumer Group | Compose configuration only |
| **Query Acceleration** | Direct DB indexing | Redis decorator on `HazardService` | Add 1 decorator struct |
| **Provider Outages** | Polling retry + Degraded flag | Dynamic Circuit Breaker | Add wrapper in polling client |
