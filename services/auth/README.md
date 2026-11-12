# Auth

`bnpb/auth` issues HS256 access JWTs for environment-configured clients and
rotates refresh tokens in process-local memory. The gateway verifies JWTs
locally using the same signing secret; there is no username/password login
or public-key mode.

This is M1 read authorization. Role claims do not implement planned M2
field-report writes or an Audit API. Shared refresh-state design remains
unresolved; see [ADR 005](../../docs/adr/005-stateless-architecture-and-m2-readiness.md).

## Routes and contract

Base URL: `http://auth:8084` inside Compose.
Full request/response shapes: frozen [Auth API](../../contracts/auth_api.yaml).

| Route | Current behavior |
| --- | --- |
| `GET /health` | Process liveness. |
| `POST /auth/token` | Authenticate `client_id` + `client_secret`; issue access and refresh tokens. |
| `POST /auth/refresh` | Consume a refresh token and return a rotated token pair. |

`requested_scope` must match the client's configured role; a different role
returns `403`. The role is mapped to the JWT `scope` checked by the gateway:

| Role | JWT scope |
| --- | --- |
| `media` | `hazards:read:summary` |
| `field`, `internal` | `hazards:read:all` |

## Configuration

These are **process defaults**, not Compose credential fallbacks.

| Variable | Default / requirement |
| --- | --- |
| `PORT` | `8084` |
| `JWT_SECRET` | Required; must match `client-api`. |
| `JWT_TTL_SECONDS` | `60`; positive integer. |
| `AUTH_MEDIA_CLIENT_SECRET` | Required; no default. |
| `AUTH_FIELD_CLIENT_SECRET` | Required; no default. |
| `AUTH_INTERNAL_CLIENT_SECRET` | Required; no default. |
| `AUTH_MEDIA_CLIENT_ID` | `test-media-client` |
| `AUTH_FIELD_CLIENT_ID` | `test-field-client` |
| `AUTH_INTERNAL_CLIENT_ID` | `test-internal-client` |

Supply secrets through the environment. Override Compose's development-only
fallbacks and keep the development stack off untrusted networks.

## Token-state limits

- Refresh tokens expire after 24 hours and are consumed on rotation.
- Reusing a retained, already-used token revokes refresh tokens in that session.
  Expired records are cleaned up; replay history is not permanent.
- Refresh and revocation state is neither durable nor shared across replicas.
  Restart loses outstanding refresh sessions.
- **Issued access JWTs remain valid until expiry** despite refresh revocation,
  credential changes, or an Auth restart, if the verification key is unchanged.
  There is no access-token revocation lookup or separate revoke route.

## Run, test, and code

See the [development guide](../../docs/development.md) for credential setup,
Compose startup, token examples, and verification. With the required secrets
exported, run `go run .` from this module. The listener binds all interfaces;
use a trusted network/firewall.

- [Entrypoint, routes, token logic, and configuration](main.go)
- [Canonical token-flow diagram](../../docs/diagrams/auth-flow.mmd)
