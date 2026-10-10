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
Deploying ten containers (seven Go service containers covering the six logical microservices, with `consumers` running as separate `dashboard-consumer` and `notifier-consumer` containers; plus PostgreSQL 16, Redpanda, and the one-shot `redpanda-init` topic provisioner) simultaneously on a developer machine or grading environment imposes strict RAM and CPU budgets.

| Metric | Monoglot Go 1.22 | Polyglot (Go + Python) | Monoglot Python (FastAPI) |
| :--- | :--- | :--- | :--- |
| **Compiled Binary Size** | 5.1–11.2 MB per service (measured) | Mixed | N/A (requires Python runtime) |
| **Docker Image Size** | **24.4–32.6 MB** (Alpine 3.20 base; measured, 4 of 7 images are above 25 MB) | 178 MB base (`python:3.12-slim`), 224 MB with FastAPI + Uvicorn (measured) | ~224 MB or more per service |
| **Idle Memory per Service** | **1.8–16.6 MiB** (measured, 7 containers) | ~32 MiB for a hello-world FastAPI app (measured; real services with dependencies not measured) | ~32 MiB or more |
| **Total Idle Memory** | **55.8 MiB** for the 7 Go service containers (measured) | Not measured (6 hello-world instances would be about 190 MiB) | Not measured |
| **Cold Startup Time** | **233–515 ms** from `docker start` to `/health` 200 (measured; includes container start) | 929 ms for hello-world FastAPI measured the same way | 929 ms or more |

*Measured on 2026-10-10: Docker 29.8.2, 13.5 GiB host RAM, single sample, idle about 45 s to 2 min after the service was ready. Raw figures and method are in [`docs/adr-audit.md`](../adr-audit.md), section E. Python and other third-party figures come from minimal stand-ins (a hello-world FastAPI app; stock images with default settings), not from BNPB services.*

By compiling static binaries (`CGO_ENABLED=0 GOOS=linux`) into minimal Alpine runtime containers, the seven Go service containers idle at about 56 MiB RAM in total (measured), leaving most system memory for PostgreSQL and Redpanda. The footprint advantage over Python is real (roughly 4x on idle memory, 8 MiB average versus 32 MiB, and 7x to 9x on image size, for a hello-world comparison) but it is not decisive on its own: the host used here has 13.5 GiB of RAM.

### 2. Native Concurrency Under 50 Sustained Connections
Problem 2 and Problem 4 evaluate system resilience under concurrent load (including a 50-connection sustained benchmark).
* Go implements native M:N user-space goroutine scheduling with 2 KB initial stack allocations. A pool of 50 or 500 concurrent connections produces virtually zero context-switch overhead or memory strain.
* In Python/FastAPI, concurrency depends on `asyncio` running on a single OS thread (constrained by the Global Interpreter Lock). Any un-awaited blocking operation (e.g., intensive JSON serialization, synchronous cryptographic signing, or DNS resolution) starves the event loop, causing p99 latency spikes.

### 3. Unified Developer Tooling and Zero Multi-Runtime Friction
* **Single Toolchain**: Every team member needs only Go 1.22 installed locally. There are no competing virtual environment tools (`uv`, `venv`, `poetry`), no Python version mismatches, and no C compiler toolchains needed.
* **Go Workspace (`go.work`)**: Allows developers to open the entire repository in their editor (Zed, VS Code, GoLand) with cross-service symbol navigation and shared linting, while preserving strict dependency isolation across individual `go.mod` files.
* **Identical Multi-Stage Dockerfile Pattern**: All services share the same deterministic multi-stage build structure (see each service's actual `Dockerfile`; `aggregator` builds from `./cmd/server/main.go`, the other services build from a flat `main.go` in the service root):
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
  * **Memory Footprint**: A hello-world FastAPI/Uvicorn container idles at about 32 MiB (measured), so six instances would use roughly 190 MiB before application dependencies. This is several times the Go footprint but well within the limits of a developer or grading machine, so memory is a secondary argument.
  * **Event Loop Bottlenecks**: Heavy polling loops, hashing operations in `auth`, and JSON deserialization in `aggregator` compete on the single-threaded asyncio loop.
  * **Deployment Complexity**: Container images are about 224 MB for FastAPI + Uvicorn on `python:3.12-slim` (measured) versus 24–33 MB for the Go images, plus a Python runtime and dependency installation step in every build.

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
