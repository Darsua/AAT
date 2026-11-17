# Client API

`bnpb/client-api` is the public hazard-read and field-report gateway. It verifies
JWTs, enforces scopes, projects read records fetched through Aggregator HTTP,
and durably stages field-report submissions to Redpanda. It never connects to
canonical PostgreSQL.

The frozen contract includes planned M2 audit behavior. Read queries and field
report submissions accept `SEISMIC`, `VOLCANIC`, `WILDFIRE`, and `COASTAL`
hazard types; audit endpoints remain outside this service.

## Routes and contract

Base URL: `http://client-api:8080` inside Compose.
Full shapes: frozen [Client API](../../contracts/client_api.yaml).

| Route | Current behavior |
| --- | --- |
| `GET /health` | Unauthenticated process liveness. |
| `GET /api/v1/hazards` | Bearer JWT required; projected `{data, meta}` list. |
| `GET /api/v1/hazards/{id}` | Bearer JWT required; single projected object or `404`. |
| `POST /api/v1/field-reports` | Bearer JWT (`hazards:read:all`) required; validates and stages durably before `202 Accepted`. |
| `POST /field-reports` | Alias for `POST /api/v1/field-reports`. |

List parameters: `fields=summary|all` (default `summary`), `type` (`SEISMIC`,
`VOLCANIC`, `WILDFIRE`, `COASTAL`), `severity` (`NORMAL`, `WASPADA`, `SIAGA`,
`AWAS`), `since` (UTC RFC3339), `limit` (default `50`, range `1`–`500`), and
`offset` (default `0`). Get-by-ID supports the same `fields` projection and
scope checks.

## Field report submission and staging (#27)

- **Authorization**: Both `POST /api/v1/field-reports` and `POST /field-reports`
  require a valid JWT with scope `hazards:read:all` (Field Team / Internal).
  Media clients (`hazards:read:summary`) are rejected with `403 Forbidden`
  before touching staging.
- **Durable staging owner & ack boundary**: `client-api` never connects to
  canonical PostgreSQL. It publishes a `StagedFieldReport` JSON envelope (keyed
  by `report_id`) to Redpanda topic `REDPANDA_TOPIC_FIELD_REPORTS` (default
  `bnpb.field-reports`) using `RequiredAcks: RequireAll`, `BatchSize: 1`, and
  a bounded `2s` fast-ack write timeout. `202 Accepted` is returned only after
  broker durability confirmation; broker unavailability or write failure
  returns `503 Service Unavailable`.
- **Idempotency & no-key policy**:
  1. **With `Idempotency-Key` header**: Scoped to `(jwt.sub, Idempotency-Key)`
     in a 24h-TTL, 10,000-entry bounded in-memory store with in-flight
     coalescing. If `report_id` is omitted, `client-api` derives a deterministic
     `rep-idem-<sha256>` identifier. Reusing the key with the same payload
     returns the original `202` without re-staging; reusing it with a different
     payload returns `409 Conflict`. Failed staging clears the entry so retries
     can succeed.
  2. **Without `Idempotency-Key`, with body `report_id`**: Scoped to
     `(jwt.sub, report_id)` with the same retry/conflict behavior.
  3. **No-key behavior (neither `Idempotency-Key` nor `report_id` provided)**:
     Generates a unique `rep-fld-<random>` per request and treats each call as
     a distinct submission. Clients requiring retry deduplication must send
     `Idempotency-Key` or `report_id`.
- **Downstream boundary & limitations**:
  - The gateway's in-memory idempotency cache resets on process restart and is
    not shared across multiple replicas; however, deterministic
    `rep-idem-<sha256>` / client `report_id` keys preserve message identity on
    `bnpb.field-reports` for downstream deduplication.
  - Local Compose runs a single Redpanda broker (`--replicas 1`), which
    survives `client-api` crashes but not broker disk loss; multi-broker
    clusters require replication factor >= 2 and `min.insync.replicas` >= 2.
  - Downstream consumption from `bnpb.field-reports` into canonical PostgreSQL
    (offset commit after transactional DB+outbox write, `ON CONFLICT`
    deduplication, and canonical `source` enum mapping) belongs to
    [Aggregator](../aggregator/README.md).

## Authorization and upstream behavior

- Missing, invalid, or expired JWTs return `401`.
- `hazards:read:summary` permits summaries; requesting `fields=all` returns `403`.
  `hazards:read:all` permits full records. The gateway checks scope strings,
  not the `media`/`field`/`internal` role names issued by [Auth](../auth/README.md).
- `X-Correlation-ID` is preserved or generated, returned to callers, and
  forwarded to Aggregator.
- List retrieval forwards filters to `GET /internal/hazards` and preserves
  degradation metadata. The HTTP timeout is five seconds per upstream request.
- **Get-by-ID scans Aggregator list pages of 500**, not a direct index or an
  internal get-by-ID endpoint. Large stores require multiple requests;
  concurrent writes can shift offset pagination. A failed page fails the lookup.
- JWT verification is local; refresh-session revocation does not invalidate
  an already-issued access token before expiry.

## Configuration

These are **process defaults**, not Compose credentials or host mappings.

| Variable | Default / requirement |
| --- | --- |
| `PORT` | `8080` |
| `JWT_SECRET` | Required; must match Auth. |
| `AGGREGATOR_URL` | `http://aggregator:8083` |
| `REDPANDA_BROKERS` | `redpanda:9092` |
| `REDPANDA_TOPIC_FIELD_REPORTS` | `bnpb.field-reports` |

Supply the signing secret through the environment and override Compose's
development-only fallback. A host process needs a reachable Aggregator URL;
do not assume Compose's internal Aggregator is host-accessible.

## Run, test, and code

See the [development guide](../../docs/development.md) for Compose startup,
JWT acquisition, scope checks, and verification. With `JWT_SECRET` exported
and `AGGREGATOR_URL` set appropriately, run `go run .` from this module.
The listener binds all interfaces; keep the development stack on a trusted
network or behind a firewall.

- [Entrypoint, routes, JWT middleware, projections, and upstream calls](main.go)
- [Field report staging and idempotency](field_reports.go)
- [Canonical gateway diagram](../../docs/diagrams/clientapi-gateway.mmd)
