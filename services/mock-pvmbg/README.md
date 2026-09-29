# Service: Mock PVMBG (Pusat Vulkanologi dan Mitigasi Bencana Geologi)

**Assigned Owner:** Member A (Aldoy)  
**Module Name:** `bnpb/mock-pvmbg`  
**Default Port:** `8082`  
**Contract:** [`contracts/pvmbg.yaml`](../../contracts/pvmbg.yaml)  

---

## 1. Overview & Architecture Role

The `mock-pvmbg` service simulates the Geological Agency / Center for Volcanology and Geological Hazard Mitigation (PVMBG) API feed. It provides volcanic observation reports, alert level changes (Normal, Waspada, Siaga, Awas), danger zones, and ash eruption notices.

Additionally, this service includes **Admin Chaos Toggles (P1 & P2)** to simulate upstream disruptions and contract changes, allowing Darrel's `aggregator` to be tested against real-world upstream failures.

```
+--------------------+     HTTP (GET)      +--------------------+
|     mock-pvmbg     | <------------------ |  aggregator (8083) |
|      (:8082)       |     Bearer Auth     +--------------------+
+--------------------+
          ^
          | Admin POST toggles (Chaos Testing)
+--------------------+
|  Tester / Script   |
+--------------------+
```

---

## 2. Service Specifications

### 2.1 Networking & Port
- **Port:** `8082` (configurable via `PORT=8082`).
- Base URL: `http://localhost:8082` or `http://mock-pvmbg:8082` in Docker network.

### 2.2 Authentication Requirement
- `GET /health` is the only public, unauthenticated endpoint.
- `GET /volcanic-reports`, `POST /admin/schema-version`, and `POST /admin/outage` **all** require the same standard HTTP `Authorization` header (per `contracts/pvmbg.yaml`'s `BearerAuth` security scheme — there is no separate "internal admin token"):
  ```http
  Authorization: Bearer <PVMBG_TOKEN>
  ```
- If token is missing, malformed, or invalid on any of the three endpoints above, respond with `401 Unauthorized`:
  ```json
  {"error": "Unauthorized", "message": "Missing or invalid Bearer token"}
  ```
- Default configured token: `PVMBG_TOKEN=pvmbg-bearer-token-2026` (matching `.env.example`).

### 2.3 Latency Injection Requirement
- Field observation data from remote mountain stations suffers from high latency and variable network conditions.
- Every response to `GET /volcanic-reports` must inject an artificial random delay between **500ms and 3000ms (3.0s)** to test upstream timeout handling and asynchronous polling.

---

## 3. Chaos Engineering Admin Toggles (P1 & P2)

To evaluate resilience in the aggregator without restarting services, Aldoy must maintain in-memory state flags that can be toggled at runtime via admin endpoints:

### 3.1 P1: Schema Evolution Toggle (`POST /admin/schema-version`)
Controls dynamic live schema evolution (adding dynamic attribute `confidence_level` [0.0 - 1.0] to volcanic observation reports).

- **Request Body:**
  ```json
  {
    "include_confidence_level": true
  }
  ```
- **Behavior:**
  - When `include_confidence_level == false`: return reports without `confidence_level`.
  - When `include_confidence_level == true`: return reports with dynamic `confidence_level` (deterministic range 0.80–0.99 per report).
- **Response `200 OK`:**
  ```json
  {
    "status": "updated",
    "include_confidence_level": true,
    "message": "Dynamic schema updated successfully"
  }
  ```

### 3.2 P2: Upstream Outage Toggle (`POST /admin/outage`)
Simulates upstream downtime (server crash, maintenance, satellite link drop).

- **Request Body:**
  ```json
  {
    "is_outage": true
  }
  ```
- **Behavior:**
  - When `is_outage == true`: `GET /volcanic-reports` immediately returns `503 Service Unavailable`:
    ```json
    {
      "error": "SERVICE_UNAVAILABLE",
      "message": "PVMBG service temporarily unreachable"
    }
    ```
  - When `is_outage == false`: service resumes normal operation with seed data.
- **Response `200 OK`:**
  ```json
  {
    "status": "updated",
    "is_outage": true,
    "message": "Outage mode updated successfully"
  }
  ```

---

## 4. Endpoints Contract

Refer to authoritative OpenAPI specification in [`contracts/pvmbg.yaml`](../../contracts/pvmbg.yaml).

### 4.1 `GET /health`
Liveness check.
- **Response `200 OK`:**
  ```json
  {
    "status": "UP",
    "service": "mock-pvmbg"
  }
  ```

### 4.2 `GET /volcanic-reports`
Returns active reports for monitored volcanoes.
- **Auth:** `Authorization: Bearer <PVMBG_TOKEN>`
- **Latency:** 500ms - 3000ms
- **Query Parameters (Optional):**
  - `since`: ISO 8601 UTC timestamp filter (e.g. `2026-09-05T15:30:00Z`).
- **Response `200 OK`:**
  ```json
  [
    {
      "report_id": "pvmbg-rpt-20260922-001",
      "volcano_id": "VLC-001",
      "alert_level": "Siaga",
      "eruption_count_24h": 12,
      "ash_column_height_m": 750.0,
      "reported_at": "2026-09-05T15:30:00Z",
      "confidence_level": 0.95
    }
  ]
  ```
*(Note: Coordinates and volcano names are omitted here because the central Aggregator enriches them from its static `volcano_references` catalog).*

---

## 5. Volcano Seed Data Reference (`VLC-001` .. `VLC-006`)

Aldoy should generate volcanic activity reports that reference the authoritative volcano codes matching the central database (`volcano_references` in `services/aggregator/migrations/001_init.sql`):

| Code | Volcano Name | Coordinates (Enriched by Aggregator) | Default Alert Level |
|---|---|---|---|
| `VLC-001` | Gunung Merapi | -7.5407, 110.4456 | Level III (Siaga) |
| `VLC-002` | Gunung Anak Krakatau | -6.1020, 105.4230 | Level III (Siaga) |
| `VLC-003` | Gunung Semeru | -8.1077, 112.9224 | Level III (Siaga) |
| `VLC-004` | Gunung Marapi | -0.3811, 100.4739 | Level III (Siaga) |
| `VLC-005` | Gunung Sinabung | 3.1694, 98.3925 | Level II (Waspada) |
| `VLC-006` | Gunung Ruang | 2.3014, 125.3697 | Level IV (Awas) |

> [!IMPORTANT]
> Per the official M1 spec, this mock must generate **at least 1 new volcanic report every 10 seconds** (rate may be increased for demo convenience), in addition to the ≥20-record historical seed set. This is a hard, graded requirement — the Aggregator's polling logic needs a steady stream of genuinely new records to demonstrate against.

---

## 6. Development & Testing Runbook

### Run Locally
```bash
# From services/mock-pvmbg
export PORT=8082
export PVMBG_TOKEN=pvmbg-bearer-token-2026
go run main.go
```

### Build & Run with Docker
```bash
# From workspace root
docker build -t bnpb/mock-pvmbg:latest services/mock-pvmbg/
docker run -p 8082:8082 -e PVMBG_TOKEN=pvmbg-bearer-token-2026 bnpb/mock-pvmbg:latest
```

### Verification Commands
```bash
# 1. Health check
curl -i http://localhost:8082/health

# 2. Get volcanic reports (Bearer auth)
curl -i -H "Authorization: Bearer pvmbg-bearer-token-2026" http://localhost:8082/volcanic-reports

# 3. Trigger P2 Upstream Outage
curl -i -X POST http://localhost:8082/admin/outage \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer pvmbg-bearer-token-2026" \
  -d '{"is_outage": true}'

# 4. Verify 503 response under outage
curl -i -H "Authorization: Bearer pvmbg-bearer-token-2026" http://localhost:8082/volcanic-reports

# 5. Restore normal operations
curl -i -X POST http://localhost:8082/admin/outage \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer pvmbg-bearer-token-2026" \
  -d '{"is_outage": false}'

# 6. Trigger P1 Schema Evolution Toggle (add confidence_level)
curl -i -X POST http://localhost:8082/admin/schema-version \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer pvmbg-bearer-token-2026" \
  -d '{"include_confidence_level": true}'
```

---

## 7. Implementation Checklist for Aldoy

- [x] Implement Bearer token extraction and validation from `Authorization` header against `PVMBG_TOKEN`.
- [x] Implement random latency generator between `500ms` and `3000ms`.
- [x] Store state for schema evolution (`include_confidence_level`) and outage mode (`is_outage`) protected by a `sync.RWMutex`.
- [x] Handle `POST /admin/schema-version` to toggle `include_confidence_level`.
- [x] Handle `POST /admin/outage` to toggle `is_outage`.
- [x] Ensure `GET /volcanic-reports` immediately yields HTTP 503 when outage mode is active.
