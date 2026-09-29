# Service: Mock BMKG (Badan Meteorologi, Klimatologi, dan Geofisika)

**Assigned Owner:** Member A (Aldoy)  
**Module Name:** `bnpb/mock-bmkg`  
**Default Port:** `8081`  
**Contract:** [`contracts/bmkg.yaml`](../../contracts/bmkg.yaml)  

---

## 1. Overview & Architecture Role

The `mock-bmkg` service acts as an upstream simulation of the Indonesian BMKG seismic and tsunami early warning API feeds. It provides earthquake event catalogs and tsunami warnings for the central BNPB `aggregator` service to ingest, normalize, and distribute.

```
+---------------+     HTTP (GET)      +--------------------+
|   mock-bmkg   | <------------------ |  aggregator (8083) |
|  (:8081)      |   X-BMKG-Key Auth   +--------------------+
+---------------+
```

---

## 2. Service Specifications

### 2.1 Networking & Port
- **Port:** `8081` (configurable via environment variable `PORT=8081`).
- Base URL: `http://localhost:8081` or `http://mock-bmkg:8081` inside Docker network.

### 2.2 Authentication Requirement
- All business endpoints (`/seismic-events`, `/tsunami-warnings`) must validate incoming requests against an API key passed in the custom header:
  ```http
  X-BMKG-Key: <BMKG_API_KEY>
  ```
- If missing or invalid, return `401 Unauthorized` with JSON:
  ```json
  {"error": "Unauthorized", "message": "Missing or invalid X-BMKG-Key header"}
  ```
- Configure the valid key via environment variable: `BMKG_API_KEY` (default: `bmkg-secret-key-2026`).

### 2.3 Latency Injection Requirement
- Real-world BMKG external feeds have network and computational delay.
- In every request to `/seismic-events` and `/tsunami-warnings`, inject an artificial delay of **50ms to 150ms** (jittered randomly using `math/rand` or `crypto/rand`) before responding.

---

## 3. Endpoints Contract

Refer to authoritative OpenAPI specification in [`contracts/bmkg.yaml`](../../contracts/bmkg.yaml).

### 3.1 `GET /health`
Liveness and readiness probe for container orchestrator / docker-compose.
- **Auth:** None (Public)
- **Response `200 OK`:**
  ```json
  {
    "status": "UP",
    "service": "mock-bmkg"
  }
  ```

### 3.2 `GET /seismic-events`
Returns list of seismic earthquake events in Indonesia.
- **Auth:** Header `X-BMKG-Key` required.
- **Query Parameters (Optional):**
  - `since`: ISO 8601 UTC timestamp filter (e.g. `2026-09-05T15:30:00Z`).
- **Latency:** 50ms - 150ms delay.
- **Response `200 OK`:**
  ```json
  [
    {
      "event_id": "bmkg-seis-20260922-001",
      "magnitude": 5.6,
      "depth_km": 15.0,
      "epicenter_lat": -7.5407,
      "epicenter_lon": 110.4456,
      "region_name": "124 km BaratDaya KAB-BANTUL-DIY",
      "occurred_at": "2026-09-05T15:30:00Z",
      "potential_tsunami": false
    }
  ]
  ```

### 3.3 `GET /tsunami-warnings`
Returns active or recent tsunami alerts triggered by major undersea seismic events.
- **Auth:** Header `X-BMKG-Key` required.
- **Query Parameters (Optional):**
  - `since`: ISO 8601 UTC timestamp filter (e.g. `2026-09-05T15:30:00Z`).
- **Latency:** 50ms - 150ms delay.
- **Response `200 OK`:**
  ```json
  [
    {
      "warning_id": "tsu-bmkg-20260922-01",
      "related_event_id": "bmkg-seis-20260922-001",
      "threat_level": "Waspada",
      "affected_zones": [
        "Pesisir Kulonprogo",
        "Pesisir Kebumen"
      ],
      "estimated_arrival": "2026-09-05T15:55:00Z"
    }
  ]
  ```

---

## 4. Seed Data Requirements

Aldoy should define his own initial seed dataset or generator containing ≥20 historical events matching `contracts/bmkg.yaml`.
Aldoy's implementation tasks:
1. Define and load seed events on application startup into memory.
2. Provide dynamic event generation or continuous time advancement so events can appear newly generated when tested by the aggregator.
3. Realistic coordinates across the Indonesian archipelago (Sumatra, Java, Sulawesi, Maluku, NTT, Papua).

> [!IMPORTANT]
> Per the official M1 spec, this mock must generate **at least 1 new event every 10 seconds** (rate may be increased for demo convenience). This is a hard, graded requirement — the Aggregator's polling and dedup logic needs a steady stream of genuinely new records to demonstrate against, not just a static seed set.

---

## 5. Development Runbook

### Run Locally (Go)
```bash
# From services/mock-bmkg
export PORT=8081
export BMKG_API_KEY=bmkg-secret-key-2026
go run main.go
```

### Build & Run with Docker
```bash
# From workspace root
docker build -t bnpb/mock-bmkg:latest services/mock-bmkg/
docker run -p 8081:8081 -e BMKG_API_KEY=bmkg-secret-key-2026 bnpb/mock-bmkg:latest
```

### Verification Commands
```bash
# 1. Health check
curl -i http://localhost:8081/health

# 2. Check seismic events (with auth)
curl -i -H "X-BMKG-Key: bmkg-secret-key-2026" http://localhost:8081/seismic-events

# 3. Check tsunami warnings (with auth)
curl -i -H "X-BMKG-Key: bmkg-secret-key-2026" http://localhost:8081/tsunami-warnings
```

---

## 6. Implementation Checklist for Aldoy

- [ ] Parse and validate incoming `X-BMKG-Key` against `BMKG_API_KEY`.
- [ ] Implement random latency generator between `50ms` and `150ms` before responding.
- [ ] Load seed dataset from `seed.json` on startup.
- [ ] Implement query filtering on `GET /seismic-events` (`limit`, `min_magnitude`).
- [ ] Implement `GET /tsunami-warnings` filtering by active alert status.
