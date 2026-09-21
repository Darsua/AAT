# ADR 001: Standardizing Microservices Implementation on Go

---

## Context
IF4031 (Arsitektur Aplikasi Terdistribusi) Milestone 1 requires implementing an event-driven disaster coordination platform for BNPB consisting of six discrete microservices:
1. `mock-bmkg`: External seismic and tsunami alert provider.
2. `mock-pvmbg`: External volcanic observation provider featuring dynamic schema evolution and simulated outages.
3. `aggregator`: Core ingestion engine, canonical schema harmonizer, transactional outbox relay, and internal query API.
4. `auth`: Token issuance, scope management, and 60-second TTL refresh token flow.
5. `client-api`: Public API gateway, scope enforcement, and correlation ID propagator.
6. `consumers`: Downstream event processors (e.g., disaster dashboard and notification dispatchers).

The assignment strictly mandates that services remain fully decoupled:
> *"Dilarang memanggil fungsi secara langsung maupun berbagi library business-logic lintas service."*

Furthermore, the entire architecture must run reliably inside Docker Compose on a single workstation during grading, withstand a sustained 50-connection load test with low latency, and remain clear and maintainable across a three-engineer team.

We evaluated three potential implementation stacks:
1. **Monoglot Go (All-Go)**: All six services written in Go 1.22.
2. **Polyglot (Go + Python/FastAPI)**: Core aggregator in Go; mocks, auth, or client facade in Python.
3. **Monoglot Python**: All services built with FastAPI / Uvicorn.

---

## Decision
We standardize **all six microservices on Go 1.22** using a standard-library-first approach complemented by minimal, battle-tested, zero-CGO libraries:
* **HTTP Transport & Routing**: Go standard library `net/http` (leveraging Go 1.22+ enhanced path routing: `GET /internal/hazards/{id}`).
* **PostgreSQL Driver & Pool**: `jackc/pgx/v5` (pure Go, high-performance binary protocol driver with integrated connection pooling).
* **Kafka/Redpanda Driver**: `segmentio/kafka-go` (pure Go implementation; zero CGO or `librdkafka` C dependencies).
* **JWT Signing & Verification**: `golang-jwt/jwt/v5` (industry standard for HMAC-SHA256 token lifecycle).
* **Workspace Tooling**: Go Multi-Module Workspaces (`go.work`) at the repository root managing isolated service modules.

```
AAT/
├── go.work                          # Root workspace linking all modules
├── services/
│   ├── mock-bmkg/go.mod             # Isolated module (Member A)
│   ├── mock-pvmbg/go.mod            # Isolated module (Member A)
│   ├── aggregator/go.mod            # Isolated module (Member B)
│   ├── auth/go.mod                  # Isolated module (Member C)
│   ├── client-api/go.mod            # Isolated module (Member C)
│   └── consumers/                   # Downstream event consumers (Member C)
```

---

## Rationale

### 1. Container Resource Efficiency & Footprint
Deploying eight containers (six Go microservices, PostgreSQL 16, and Redpanda) simultaneously on a developer machine or grading environment imposes strict RAM and CPU budgets.

| Metric | Monoglot Go 1.22 | Polyglot (Go + Python) | Monoglot Python (FastAPI) |
| :--- | :--- | :--- | :--- |
| **Compiled Binary Size** | ~12–18 MB per service | Mixed | N/A (requires Python runtime) |
| **Docker Image Size** | **< 25 MB** (Alpine 3.20 base) | ~180–250 MB (Python) | ~180–250 MB per service |
| **Idle Memory per Service** | **~10–16 MB** | ~110–160 MB (Python) | ~110–160 MB |
| **Total Idle Memory (6 Services)** | **< 100 MB combined** | ~450–600 MB | **~800 MB – 1.1 GB** |
| **Cold Startup Time** | **< 50 ms** | 1.2–2.5 s (Python) | 1.2–2.5 s |

By compiling static binaries (`CGO_ENABLED=0 GOOS=linux`) into minimal Alpine runtime containers, the entire six-service compute layer idles at under 100 MB RAM total. This leaves virtually all system memory available for PostgreSQL's buffer cache and Redpanda's high-throughput ring buffers.

### 2. Native Concurrency Under 50 Sustained Connections
Problem 2 and Problem 4 evaluate system resilience under concurrent load (including a 50-connection sustained benchmark).
* Go implements native M:N user-space goroutine scheduling with 2 KB initial stack allocations. A pool of 50 or 500 concurrent connections produces virtually zero context-switch overhead or memory strain.
* In Python/FastAPI, concurrency depends on `asyncio` running on a single OS thread (constrained by the Global Interpreter Lock). Any un-awaited blocking operation (e.g., intensive JSON serialization, synchronous cryptographic signing, or DNS resolution) starves the event loop, causing p99 latency spikes.

### 3. Unified Developer Tooling and Zero Multi-Runtime Friction
* **Single Toolchain**: Every team member needs only Go 1.22 installed locally. There are no competing virtual environment tools (`uv`, `venv`, `poetry`), no Python version mismatches, and no C compiler toolchains needed.
* **Go Workspace (`go.work`)**: Allows developers to open the entire repository in their editor (Zed, VS Code, GoLand) with cross-service symbol navigation and shared linting, while preserving strict dependency isolation across individual `go.mod` files.
* **Identical Multi-Stage Dockerfile Pattern**: All services share the same deterministic multi-stage build structure (see each service's actual `Dockerfile`; `aggregator` builds from `./cmd/server/main.go`, the other four build from a flat `main.go` in the service root):
  ```dockerfile
  FROM golang:1.22-alpine AS builder
  WORKDIR /app
  COPY go.mod go.sum* ./
  RUN go mod download || true
  COPY . .
  RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/<service> <entrypoint>

  FROM alpine:3.20
  RUN apk add --no-cache ca-certificates tzdata
  COPY --from=builder /bin/<service> /app/<service>
  USER nobody:nobody
  ENTRYPOINT ["/app/<service>"]
  ```

---

## Alternatives Evaluated

### Alternative A: Full Python (FastAPI / Uvicorn)
* **Description**: Write all services in Python 3.12 using FastAPI, Pydantic v2, and Uvicorn.
* **Why Rejected**:
  * **Memory Bloat**: Baseline memory consumption for six FastAPI/Uvicorn containers exceeds 900 MB idle, approaching resource limits on evaluation virtual machines.
  * **Event Loop Bottlenecks**: Heavy polling loops, hashing operations in `auth`, and JSON deserialization in `aggregator` compete on the single-threaded asyncio loop.
  * **Deployment Complexity**: Container images balloon to 200 MB+ due to Python interpreter runtimes, wheel caching, and dependencies.

### Alternative B: Polyglot Architecture (Go Aggregator + Python Mocks/Clients)
* **Description**: Implement data-intensive services (`aggregator`) in Go, and lighter facade services (`mock-bmkg`, `mock-pvmbg`, `auth`, `client-api`) in Python.
* **Why Rejected**:
  * **Operational Friction**: Requires two separate language toolchains on every developer's machine and in CI/Docker build pipelines.
  * **Contract Drift**: Struct definitions must be maintained in two disparate type systems (Go structs vs. Pydantic models) without shared code generators.
  * **Violation of Lean Principle**: Introduces cognitive overhead across the team with zero tangible performance benefit over Go's concise `net/http` handlers.

---

## Trade-offs & Mitigations

### 1. Static Typing vs. Dynamic Additive Schema (Problem 1)
* **Challenge**: Problem 1 requires `mock-pvmbg` to dynamically introduce new fields (e.g., `confidence_level: 0.95`) at runtime without crashing downstream services or dropping unknown attributes. Go's strict static typing could reject unrecognized fields if using rigid structs.
* **Mitigation**: We separate the canonical payload into a **strongly-typed envelope** and a **dynamic attributes container**:
  ```go
  type HazardEvent struct {
      HazardID     string                 `json:"hazard_id"`
      Source       string                 `json:"source"`
      HazardType   string                 `json:"hazard_type"`
      Severity     string                 `json:"severity"`
      AreaName     string                 `json:"area_name"`
      OccurredAt   time.Time              `json:"occurred_at"`
      Attributes   map[string]interface{} `json:"attributes"` // Dynamic schemaless bag
  }
  ```
  Unknown additive fields from upstream providers unmarshal seamlessly into `Attributes` and persist directly into PostgreSQL's `JSONB` column, combining strict type safety on queryable indexes with dynamic flexibility for live schema evolution.

### 2. Verbosity Compared to Python Frameworks
* **Challenge**: Go lacks decorator-based routing (`@app.get("/...")`) and automatic request body binding found in FastAPI.
* **Mitigation**: Go 1.22's standard library `net/http` introduces method and path wildcard matching (`mux.HandleFunc("GET /internal/hazards/{id}", handler)`). Standard JSON helper functions (`json.NewDecoder(r.Body).Decode(&req)`) are explicit, readable, and free of hidden reflection overhead.
