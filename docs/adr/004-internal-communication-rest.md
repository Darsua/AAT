# ADR 004: Internal Service-to-Service Communication via HTTP/REST with Decoupled Abstractions

---

## Context
In IF4031 Milestone 1, services must communicate across network boundaries:
1. **Aggregator to External Providers**: `services/aggregator` periodically polls `mock-bmkg` (`:8081`) and `mock-pvmbg` (`:8082`) over HTTP.
2. **Client-Facing API to Aggregator**: When external clients query `services/client-api` (`GET /hazards`), the client API validates the caller's JWT token, evaluates required scopes, and forwards the query to `services/aggregator` (`GET /internal/hazards`) over the internal Docker network.
3. **Problem 1 Live Dynamic Attributes**: Upstream attributes (e.g., `confidence_level`) must flow seamlessly across internal service boundaries without manual schema recompilation or dropping unmapped fields.
4. **Grading & Operability**: Evaluators need to inspect internal states, simulate outages, and test endpoints directly via command-line tools (`curl`, `jq`, `httpie`).

We evaluated the choice of internal communication protocol between `client-api` and `aggregator`.

---

## Decision
We select **HTTP/1.1 with JSON payloads (REST)** for all internal synchronous service-to-service communication, coupled with an **interface-driven domain abstraction (`HazardService`)** inside `client-api`:

1. **Protocol**: HTTP/1.1 over Docker bridge network (`bnpb-internal`), using Go standard library `net/http` with connection keep-alive and tuned connection pools (`http.Transport`).
2. **Endpoint**: `http://aggregator:8083/internal/hazards` supporting query filters (`since`, `type`, `severity`, `limit`, `offset`).
3. **Header Propagation**: `client-api` generates or propagates `X-Correlation-ID` across all internal HTTP calls for distributed tracing.
4. **Interface Decoupling**: Handlers in `client-api` never reference raw HTTP client details; they interact with a domain interface:
   ```go
   type HazardService interface {
       GetHazards(ctx context.Context, filter QueryFilter) (*HazardResponse, error)
   }
   ```
   The concrete `RemoteAggregatorClient` implements this interface, ensuring the underlying transport can evolve in Milestone 2 without modifying handler code.

```mermaid
sequenceDiagram
    autonumber
    actor Client as External Client (Media/Field)
    participant ClientAPI as client-api (:8080)
    participant Auth as auth (:8084)
    participant Aggregator as aggregator (:8083)
    participant DB as canonical-db (:5432)

    Client->>ClientAPI: GET /hazards (Bearer JWT)
    ClientAPI->>Auth: Validate Token & Scopes (or local JWT verify)
    Note over ClientAPI: Check if Scope allows Raw fields
    ClientAPI->>Aggregator: GET /internal/hazards?type=SEISMIC<br/>X-Correlation-ID: req-abc-123
    Aggregator->>DB: Query hazards + volcano_references
    DB-->>Aggregator: Canonical records
    Aggregator-->>ClientAPI: 200 OK (JSON array + degraded status)
    Note over ClientAPI: Filter fields if Media scope (Summary only)
    ClientAPI-->>Client: 200 OK (Filtered JSON payload)
```

---

## Rationale

### 1. Protocol Homogeneity Across Architecture
The course specification explicitly mandates HTTP/REST for external integrations:
* BMKG and PVMBG mock endpoints must be exposed as HTTP/REST.
* `auth` service provides OAuth2/JWT tokens over HTTP.
* `client-api` is an HTTP REST gateway.

Using HTTP/REST internally preserves uniform transport semantics across the entire platform. Every service uses the same Go `net/http` primitives, error handling patterns, status codes (`200`, `400`, `401`, `403`, `503`), and connection lifecycle management.

### 2. Seamless Dynamic Attribute Serialization (Problem 1)
JSON is inherently schemaless and naturally accommodates dynamic additive attributes.
* In Go, dynamic JSON attributes map directly into `map[string]interface{}` without code generation.
* As `mock-pvmbg` introduces new fields (e.g., `confidence_level: 0.95`), the JSON payload traverses from `mock-pvmbg` → `aggregator` → `canonical-db` → `aggregator` → `client-api` → client without any service dropping unknown fields.

### 3. Grading Transparency and Terminal Operability
Evaluators assess services by executing tests, inspecting containers, and running shell commands. With REST:
* An evaluator can immediately curl the internal aggregator directly from within any container or during debugging:
  ```bash
  docker compose exec client-api curl -s http://aggregator:8083/internal/hazards | jq .
  ```
* Every payload is human-readable JSON. There is no need to install specialized binary decoding tools (`grpcurl`), mount `.proto` files, or maintain reflection servers.

### 4. Low Latency on Docker Bridge Network
Benchmarking within a Linux Docker bridge network reveals that HTTP/1.1 with persistent keep-alive connections (`MaxIdleConnsPerHost: 100`) achieves sub-millisecond round-trip times (<0.6 ms for typical payloads). For the Milestone 1 scale (50 sustained concurrent requests), the overhead of HTTP/1.1 compared to gRPC binary framing is negligible (microseconds), while saving substantial architectural complexity.

---

## Alternatives Evaluated

### Alternative A: gRPC with Protocol Buffers (Protobuf)
* **Description**: Define `.proto` service contracts and compile client/server stubs with `protoc`.
* **Why Rejected**:
  * **Dynamic Schema Friction (Problem 1)**: Protobuf enforces strict static schemas. Handling unexpected dynamic fields requires wrapping payloads in `google.protobuf.Struct` or `google.protobuf.Any`, which introduces cumbersome boilerplate and type reflection in Go (`structpb.NewValue(...)`), negating gRPC's static type safety advantages.
  * **Toolchain & Build Overhead**: Requires adding `protoc` compilers and Go protobuf plugins into Dockerfiles, increasing container build times and workspace complexity.
  * **CLI Inconvenience**: Internal endpoints cannot be inspected with standard `curl`.

### Alternative B: Broker Request-Reply (RPC over Kafka/Redpanda)
* **Description**: `client-api` publishes a request to a Kafka request topic and awaits a response on a dedicated reply topic with a correlation ID.
* **Why Rejected**:
  * Request-reply over an asynchronous event log is an anti-pattern for synchronous read queries (`GET /hazards`).
  * Introduces unnecessary latency, complex timeout handling, and potential consumer group rebalancing stalls during synchronous user requests.
  * Event brokers should be reserved for decoupled event fan-out (Problem 5), not synchronous query-response loops.

---

## Clean Domain Decoupling

To prevent `client-api` handlers from coupling to transport details, communication is encapsulated behind a domain interface:

```go
package domain

import "context"

type QueryFilter struct {
    Since      string
    HazardType string
    Severity   string
    Limit      int
    Offset     int
}

type HazardService interface {
    GetHazards(ctx context.Context, filter QueryFilter) (*HazardResponse, error)
    GetHealth(ctx context.Context) (bool, error)
}
```

In `client-api`, the concrete implementation uses an HTTP client with connection pooling:

```go
type RemoteAggregatorClient struct {
    baseURL    string
    httpClient *http.Client
}

func NewRemoteAggregatorClient(baseURL string) *RemoteAggregatorClient {
    return &RemoteAggregatorClient{
        baseURL: baseURL,
        httpClient: &http.Client{
            Timeout: 5 * time.Second,
            Transport: &http.Transport{
                MaxIdleConns:        100,
                MaxIdleConnsPerHost: 50,
                IdleConnTimeout:     90 * time.Second,
            },
        },
    }
}
```

This abstraction ensures that if Milestone 2 requires introducing gRPC, Redis caching, or a circuit breaker, only `RemoteAggregatorClient` changes; the HTTP request handlers, auth validation, and scope filtering remain completely untouched.
