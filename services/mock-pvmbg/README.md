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
| `GET /metrics` | None | Prometheus metrics registry. |
| `GET /volcanic-reports` | Bearer token | Reports; optional `since` (UTC RFC3339). |
| `POST /admin/schema-version` | Same Bearer token | Set `mode` and/or toggle `include_confidence_level`. |
| `POST /admin/outage` | Same Bearer token | Toggle `is_outage`. |

`mode` selects how every report in `/volcanic-reports` is serialized (M2 P3,
[#26](https://github.com/Darsua/AAT/issues/26)):

| `mode` | Served reports |
| --- | --- |
| `additive` (default) | Nominal M1 schema. |
| `rename_field` | `report_id` replaced by `event_id`, same value. |
| `type_change` | `eruption_count_24h` sent as a decimal string, e.g. `"12"`. |
| `missing_required_field` | `volcano_id` omitted. |

The three incompatible modes are test fixtures for quarantine and replay, not
accepted canonical data. A request without `mode` selects `additive`, so the M1
`{"include_confidence_level": true}` request behaves as before; a request
without `include_confidence_level` keeps its current value, so the flag
combines with any mode. An unknown mode, a non-boolean flag, or a body with
neither field returns `400` `INVALID_REQUEST` and changes nothing.

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
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Empty; optional OTLP/HTTP JSON trace endpoint (e.g. `http://tempo:4318`). |

Supply the token through `.env` or the process environment and use the same
value for Aggregator. Set both delay bounds to `3000`
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
- Schema modes rewrite only the response; stored reports are unchanged, so
  switching back to `additive` serves the same reports normally.
- Outage mode takes priority over every schema mode. Outage mode makes authenticated report requests immediately return `503`
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
