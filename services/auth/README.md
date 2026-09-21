# Service: Authentication & Token Authority (Auth)

**Assigned Owner:** Member C (Jenka)  
**Module Name:** `bnpb/auth`  
**Default Port:** `8084`  
**Dependencies:** `github.com/golang-jwt/jwt/v5`

---

## 1. Overview & Architecture Role

The `auth` service is the central security authority for the BNPB platform. It issues short-lived JSON Web Tokens (JWTs) and manages refresh token rotation. Downstream services such as `client-api` use the shared secret or public key to verify incoming client credentials without coupling to user databases.

```
+---------------+      POST /auth/token       +---------------+
| Client / App  | --------------------------> |  auth (:8084) |
| (Credentials) | <-------------------------- |  Issues JWT   |
+---------------+     Access Token (60s TTL)  +---------------+
       |              & Refresh Token
       |
       |  GET /api/v1/hazards (Authorization: Bearer <JWT>)
       v
+--------------------+
| client-api (:8080) | (Verifies JWT signature & checks scopes)
+--------------------+
```

---

## 2. Security & Token Specifications

### 2.1 Networking & Port
- **Port:** `8084` (configurable via `PORT=8084`).
- Base URL: `http://localhost:8084` or `http://auth:8084` within Docker network.

### 2.2 JWT Signing Key & Algorithm
- **Algorithm:** HMAC-SHA256 (`HS256`).
- **Secret Key:** Provided via environment variable `JWT_SECRET` (default: `super-secret-jwt-key-2026`).

### 2.3 Access Token Lifetime (60s TTL Requirement)
- **TTL:** Exactly **60 seconds** (`exp = time.Now().Add(60 * time.Second).Unix()`).
- Short TTL minimizes the blast radius of compromised tokens and ensures near real-time revocation enforcement when permissions change.

### 2.4 Token Claims Structure
Tokens must include the following custom claims. **`scope` is the field `client-api` actually checks** (per `contracts/client_api.yaml`) — it must be one of the two literal OAuth-style scope strings below, never the bare role name. `role` is carried separately for audit/logging purposes only.
```json
{
  "iss": "bnpb-auth-service",
  "sub": "user_id_or_client_id",
  "iat": 1758500000,
  "exp": 1758500060,
  "role": "field",
  "scope": "hazards:read:all"
}
```

### 2.5 Scopes Definition
| Role (`role` claim / `requested_scope` input) | JWT `scope` claim value (checked by `client-api`) | Intended Audience | Access Level |
|---|---|---|---|
| `media` | `hazards:read:summary` | Public press & news organizations | Summarized alerts only; forbidden (`403`) from accessing raw telemetry or internal notes |
| `field` | `hazards:read:all` | First responders (TNI, POLRI, Basarnas, PMI) | Operational dispatches, rescue coordinates, responder telemetry |
| `internal` | `hazards:read:all` | BNPB Operations Center (Pusdalops) | Unrestricted read access to all hazard event fields |

> [!IMPORTANT]
> `auth` is responsible for mapping the requested role (`media`/`field`/`internal`) to the correct `scope` claim value above when issuing a token. `client-api` only ever checks for the literal strings `hazards:read:summary` and `hazards:read:all` — it has no knowledge of `media`/`field`/`internal` as concepts.

---

## 3. Refresh Token Flow & Rotation

To maintain seamless client sessions despite the 60-second access token TTL, implement strict **Refresh Token Rotation**:

1. **Issuance:** On successful initial login (`POST /auth/token`), return:
   - `access_token` (expires in 60s)
   - `refresh_token` (opaque secure random token, TTL: 24 hours)
2. **Rotation on Refresh:** When client calls `POST /auth/refresh` with a valid refresh token:
   - Validate that the refresh token exists and has not been revoked or used previously.
   - **Invalidate (burn)** the presented refresh token immediately.
   - Issue a **new access token** (60s TTL) and a **brand-new rotated refresh token**.
3. **Replay Detection:** If an already-burned refresh token is submitted again, immediately invalidate all refresh tokens associated with that user session (compromise mitigation).

---

## 4. Endpoints Contract

### 4.1 `GET /health`
Liveness check.
- **Response `200 OK`:**
  ```json
  {
    "status": "UP",
    "service": "auth"
  }
  ```

### 4.2 `POST /auth/token`
Authenticate client credentials (e.g. `client_id` + `client_secret` or `username` + `password`). `requested_scope` accepts the role name (`media`/`field`/`internal`); the server maps it to the actual `hazards:read:*` scope string per the table in §2.5 and embeds that in both the JWT `scope` claim and the response body below.
- **Request Body:**
  ```json
  {
    "client_id": "field-unit-jakarta-01",
    "client_secret": "secret123",
    "requested_scope": "field"
  }
  ```
- **Response `200 OK`:**
  ```json
  {
    "token_type": "Bearer",
    "access_token": "eyJhbGciOiJIUzI1Ni...",
    "expires_in": 60,
    "refresh_token": "rt_8fbc2e7a...",
    "scope": "hazards:read:all"
  }
  ```

### 4.3 `POST /auth/refresh`
Rotate refresh token and issue new 60s access token.
- **Request Body:**
  ```json
  {
    "refresh_token": "rt_8fbc2e7a..."
  }
  ```
- **Response `200 OK`:**
  ```json
  {
    "token_type": "Bearer",
    "access_token": "eyJhbGciOiJIUzI1Ni...",
    "expires_in": 60,
    "refresh_token": "rt_new_9a12c4e..."
  }
  ```
- **Response `401 Unauthorized` (expired or reused):**
  ```json
  {
    "error": "invalid_grant",
    "message": "Refresh token expired, invalid, or already rotated"
  }
  ```

---

## 5. Development Runbook

### Run Locally
```bash
# In services/auth
export PORT=8084
export JWT_SECRET="super-secret-jwt-key-2026"
go run main.go
```

### Build & Run with Docker
```bash
docker build -t bnpb/auth:latest services/auth/
docker run -p 8084:8084 -e JWT_SECRET="super-secret-jwt-key-2026" bnpb/auth:latest
```

### Verification Commands
```bash
# 1. Health check
curl -i http://localhost:8084/health

# 2. Issue token stub
curl -i -X POST http://localhost:8084/auth/token \
  -H "Content-Type: application/json" \
  -d '{"client_id":"media-cnn-indonesia","client_secret":"secret","requested_scope":"media"}'

# 3. Refresh token stub
curl -i -X POST http://localhost:8084/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"sample_token"}'
```

---

## 6. Implementation Checklist for Jenka

- [ ] Configure `jwt.SigningMethodHS256` using `JWT_SECRET`.
- [ ] Implement user/client credential validation table or in-memory store for `media`, `field`, and `internal` identities.
- [ ] Implement role → scope mapping (`media`→`hazards:read:summary`, `field`/`internal`→`hazards:read:all`) per §2.5, and embed the resulting string in the JWT `scope` claim — **not** the bare role name. This is what `client-api` actually validates.
- [ ] Implement JWT token generation with standard and custom claims (`scope`, `role`), ensuring `exp` is 60 seconds.
- [ ] Implement in-memory or Redis-backed store for active refresh tokens.
- [ ] Implement refresh token rotation logic in `POST /auth/refresh` with reuse detection.
