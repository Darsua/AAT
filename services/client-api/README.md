# Client API

`bnpb/client-api` is the M1 public hazard-read gateway. It verifies JWTs,
enforces read scopes, and projects records fetched through Aggregator HTTP.
It never connects to canonical PostgreSQL.

The frozen contract includes planned M2 types and write/audit behavior.
The current runtime accepts only `SEISMIC` and `VOLCANIC` and exposes no
field-report write or audit endpoints.

## Routes and contract

Base URL: `http://client-api:8080` inside Compose.
Full shapes: frozen [Client API](../../contracts/client_api.yaml).

| Route | Current behavior |
| --- | --- |
| `GET /health` | Unauthenticated process liveness. |
| `GET /api/v1/hazards` | Bearer JWT required; projected `{data, meta}` list. |
| `GET /api/v1/hazards/{id}` | Bearer JWT required; single projected object or `404`. |

List parameters: `fields=summary|all` (default `summary`), `type`, `severity`,
`since` (UTC RFC3339), `limit` (default `50`, range `1`–`500`), and `offset`
(default `0`). Severities: `NORMAL`, `WASPADA`, `SIAGA`, `AWAS`.
Get-by-ID supports the same `fields` projection and scope checks.

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
- [Canonical gateway diagram](../../docs/diagrams/clientapi-gateway.mmd)
