# Mock PVMBG

`bnpb/mock-pvmbg` simulates M1 volcanic reports and provides schema/outage
toggles for ingestion testing. It is not a live agency integration or a
danger-zone/dispatch API; planned M2 capabilities are not extra runtime routes.

## Routes and contract

Base URL: `http://mock-pvmbg:8082` inside Compose.
Full request/response shapes: frozen [PVMBG API](../../contracts/pvmbg.yaml).

| Route | Authentication | Current behavior |
| --- | --- | --- |
| `GET /health` | None | `200` normally; `503` / `DOWN` during simulated outage. |
| `GET /volcanic-reports` | Bearer token | Reports; optional `since` (UTC RFC3339). |
| `POST /admin/schema-version` | Same Bearer token | Toggle `include_confidence_level`. |
| `POST /admin/outage` | Same Bearer token | Toggle `is_outage`. |

The contract also defines M2 incompatible-schema controls; those modes are **not implemented**
by the current mock and remain tracked in [#26](https://github.com/Darsua/AAT/issues/26).
The route table above describes the runnable M1 behavior, not those planned controls.

All protected routes use `PVMBG_TOKEN`; there is no separate admin credential.
Missing, malformed, or invalid tokens return `401`.

## Configuration

These are **process defaults**, not Compose credential fallbacks or host ports.

| Variable | Default / requirement |
| --- | --- |
| `PORT` | `8082` |
| `PVMBG_TOKEN` | Required; no default. Startup fails if empty. |
| `PVMBG_DELAY_MIN_MS` | `500` |
| `PVMBG_DELAY_MAX_MS` | `3000`; must not be below the minimum. |

Supply the token through the environment and use the same value for Aggregator.
Override Compose's development-only fallback. Set both delay bounds to `3000`
for a fixed three-second report delay.

## Dataset and toggle behavior

- Startup loads embedded `seed.json`, validating at least 20 reports; a generator
  adds a report every 10 seconds. Generated data and toggles are process-local,
  reset on restart, and are not shared across replicas.
- Reports identify volcanoes but omit names/coordinates. Aggregator enriches
  them from its static catalog, using matching `VLC-001` through `VLC-006` IDs.
- Schema mode adds `confidence_level` values from `0.80` to `0.99`, based on
  position in the filtered response, not a stable per-report measurement.
  Aggregator captures unknown additive PVMBG fields into `attributes`; breaking
  field removal/type changes are outside this policy.
- Outage mode makes authenticated report requests immediately return `503`
  instead of waiting for injected latency. Health also returns `503`, marking
  the Compose container unhealthy; authenticated admin toggles remain available
  so the outage can be cleared.
- Clearing outage resumes serving the current in-memory dataset.

## Run, test, and code

Use the [development guide](../../docs/development.md) for Compose startup,
verification, and schema/outage demonstrations. With `PVMBG_TOKEN` exported,
run `go run .` from this module.

The process listens on all interfaces, and Compose's default mock port mapping
is not loopback-only. Keep it on a trusted network/firewall or restrict the
host binding for standalone development.

- [Entrypoint, routes, toggles, authentication, and generator](main.go)
- [Embedded historical data](seed.json)
- [Canonical context diagram](../../docs/diagrams/mock-pvmbg-context.mmd)
