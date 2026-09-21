# Service: Client API Gateway (Facade)

**Assigned Owner:** Member C (Jenka)  
**Module Name:** `bnpb/client-api`  
**Default Port:** `8080`  
**Dependencies:** `github.com/golang-jwt/jwt/v5`  
**Contract:** [`contracts/client_api.yaml`](../../contracts/client_api.yaml)

---

## 1. Overview & Architecture Role

The `client-api` service acts as the **Public Edge Gateway** for all external consumers (web apps, mobile apps, media outlets, and emergency field responder devices). It isolates the internal `aggregator` engine from the public internet.

```
                     +---------------------------------------+
                     |         External Consumers            |
                     |  (Media / Field Teams / Public Portal)|
                     +---------------------------------------+
                                         |
                                         | HTTP GET /api/v1/hazards
                                         | Authorization: Bearer <JWT>
                                         | [X-Correlation-ID: <uuid>]
                                         v
                     +---------------------------------------+
                     |           client-api (:8080)          |
                     |  1. Injects/Preserves Correlation ID  |
                     |  2. Validates JWT Signature & Claims  |
                     |  3. Enforces Scope Permissions        |
                     |  4. Proxies Request to Aggregator     |
                     +---------------------------------------+
                                         |
                                         | HTTP GET /internal/hazards
                                         | X-Correlation-ID: <uuid>
                                         v
                     +---------------------------------------+
                     |           aggregator (:8083)          |
                     +---------------------------------------+
```

---

## 2. Gateway Responsibilities & Specifications

### 2.1 Networking & Port
- **Port:** `8080` (configurable via `PORT=8080`).
- Public URL: `http://localhost:8080` or exposed via ingress/reverse-proxy.

### 2.2 JWT Verification
- Every request to `/api/v1/*` must include a valid Bearer token in the `Authorization` header:
  ```http
  Authorization: Bearer <token>
  ```
- Validate token authenticity and expiration against `JWT_SECRET` (HMAC-SHA256).
- If absent, invalid, or expired, return `401 Unauthorized`.

### 2.3 Scope-Based Access Control & Filtering
Tokens issued by `auth` contain a `scope` claim with one of two literal values: `hazards:read:summary` (Media) or `hazards:read:all` (Field/Internal) — see `contracts/auth_api.yaml` and `services/auth/README.md` §2.5. `client-api` only ever checks these two literal strings; it has no knowledge of `media`/`field`/`internal` as role names.

1. **`hazards:read:summary` Scope (Media):**
   - Designed for news agencies and public information dashboards.
   - Summarized view only (`fields=summary` by default).
   - **403 Forbidden Rule:** If a `media` client requests raw telemetry or sets query param `fields=all`, `client-api` must reject the request with **`403 Forbidden`**:
     ```json
     {
       "error": "FORBIDDEN",
       "message": "Scope 'hazards:read:all' required to access raw fields or fields=all"
     }
     ```

2. **`hazards:read:all` Scope (Field):**
   - Permitted access to operational coordinates, evacuation zones, severity levels, and field responder dispatches (`fields=all`).

3. **`hazards:read:all` Scope (Internal):**
   - Full unrestricted access to all canonical domain models and operational metadata (`fields=all`). Same scope value as Field — `role` in the JWT (not `scope`) distinguishes them for audit/logging purposes only; access level is identical.

### 2.4 Distributed Tracing: `X-Correlation-ID` Propagation
- Inspect incoming request headers for `X-Correlation-ID`.
- If missing or empty: generate a fresh UUIDv4 (or high-entropy pseudo-random identifier).
- Always inject `X-Correlation-ID: <id>` into:
  1. Downstream HTTP calls forwarded to `aggregator:8083`.
  2. The HTTP response headers returned back to the client (`w.Header().Set("X-Correlation-ID", id)`).
  3. All structured logs output by `client-api`.

---

## 3. Downstream Proxying

- Target Aggregator Endpoint: `http://aggregator:8083/internal/hazards` (configurable via `AGGREGATOR_URL`).
- Query parameters (`type`, `severity`, `since`, `limit`, `offset`) should be safely forwarded to the aggregator.
- Use `net/http` client with reasonable timeouts (e.g., 5 seconds).

---

## 4. Endpoints Contract

Refer to authoritative OpenAPI specification in [`contracts/client_api.yaml`](file:///home/darrel/Projects/AAT/contracts/client_api.yaml).

### 4.1 `GET /health`
Liveness probe.
- **Response `200 OK`:**
  ```json
  {
    "status": "UP",
    "service": "client-api"
  }
  ```

### 4.2 `GET /api/v1/hazards`
Public hazard event retrieval endpoint.
- **Headers Required:**
  - `Authorization: Bearer <JWT>`
- **Headers Optional:**
  - `X-Correlation-ID: <uuid>`
- **Query Parameters (Optional):**
  - `fields`: `summary` (default) or `all` (requires non-media scope; media returns 403)
  - `type`: `SEISMIC`, `VOLCANIC`
  - `severity`: `NORMAL`, `WASPADA`, `SIAGA`, `AWAS`
  - `since`: ISO 8601 UTC timestamp
  - `limit`: default `50`
  - `offset`: default `0`
- **Response `200 OK` (Summary Projection):**
  ```json
  {
    "data": [
      {
        "hazard_id": "haz-bmkg-20260922-001",
        "source": "BMKG",
        "hazard_type": "SEISMIC",
        "severity": "WASPADA",
        "area_name": "124 km BaratDaya KAB-BANTUL-DIY",
        "occurred_at": "2026-09-05T15:30:00Z",
        "ingested_at": "2026-09-05T15:35:00Z"
      }
    ],
    "meta": {
      "total": 1,
      "limit": 50,
      "offset": 0,
      "degraded": false,
      "stale_since": null
    }
  }
  ```

---

## 5. Development Runbook

### Run Locally
```bash
# In services/client-api
export PORT=8080
export JWT_SECRET="super-secret-jwt-key-2026"
export AGGREGATOR_URL="http://localhost:8083"
go run main.go
```

### Build & Run with Docker
```bash
docker build -t bnpb/client-api:latest services/client-api/
docker run -p 8080:8080 \
  -e JWT_SECRET="super-secret-jwt-key-2026" \
  -e AGGREGATOR_URL="http://aggregator:8083" \
  bnpb/client-api:latest
```

### Verification Commands
```bash
# 1. Health check
curl -i http://localhost:8080/health

# 2. Query hazards with Correlation ID and Bearer token
curl -i http://localhost:8080/api/v1/hazards?fields=summary \
  -H "Authorization: Bearer <JWT_TOKEN>" \
  -H "X-Correlation-ID: test-trace-12345"
```

---

## 6. Implementation Checklist for Jenka

- [ ] Middleware: Read `X-Correlation-ID` or generate new UUID; set on context and response header.
- [ ] Middleware: Parse and validate JWT from `Authorization: Bearer <token>` using `jwt.Parse` and `JWT_SECRET`.
- [ ] Middleware / Guard: Check the JWT `scope` claim. Return `403 Forbidden` if a token with `scope: hazards:read:summary` (Media) requests `fields=all` or attempts to access raw telemetry.
- [ ] Implement reverse proxy / HTTP client forwarder to `AGGREGATOR_URL + "/internal/hazards"`.
- [ ] Forward `X-Correlation-ID` to downstream `aggregator`.
- [ ] Handle aggregator network errors gracefully (return `502 Bad Gateway` or `504 Gateway Timeout`).
