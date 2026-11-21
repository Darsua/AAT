# Mock BMKG

`bnpb/mock-bmkg` simulates BMKG seismic events and tsunami warnings for M1
Aggregator ingestion. It is not a live agency integration. Planned M2 sources
in broader contracts do not add runtime routes to this mock.

## Routes and contract

Base URL: `http://mock-bmkg:8081` inside Compose.
Full request/response shapes: frozen [BMKG API](../../contracts/bmkg.yaml).

| Route | Authentication | Current behavior |
| --- | --- | --- |
| `GET /health` | None | Process health. |
| `GET /metrics` | None | Prometheus metrics registry. |
| `GET /seismic-events` | `X-BMKG-Key` | Seismic event list. |
| `GET /tsunami-warnings` | `X-BMKG-Key` | Tsunami warning list. |

Both feed routes accept optional `since` timestamps in UTC RFC3339 and inject
50–150 ms random latency. Missing or invalid keys return `401`.
There are no admin chaos-toggle routes on this service.

## Configuration

These are **process defaults**, not Compose credential fallbacks or host ports.

| Variable | Default / requirement |
| --- | --- |
| `PORT` | `8081` |
| `BMKG_API_KEY` | Required; no default. Startup fails if empty. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Empty; optional OTLP/HTTP JSON trace endpoint (e.g. `http://tempo:4318`). |

Supply the key through `.env` or the process environment; Compose passes the
matching `BMKG_API_KEY` value to Aggregator as `BMKG_KEY`.

## Dataset and process state

- Startup loads embedded `seed.json` and validates at least 20 seismic records.
- A background generator adds a seismic event every 10 seconds.
- Generated records are in memory and lost on restart; embedded historical
  seeds are reloaded. There is no durable/shared dataset across replicas.
- Tsunami warnings correlate to seismic records by `related_event_id`.
- Aggregator's BMKG adapter uses typed DTOs and discards unknown JSON fields.
  Additive unknown-field capture is implemented for PVMBG only, not this feed.

## Run, test, and code

Use the [development guide](../../docs/development.md) for credential setup,
Compose startup, feed queries, and verification. With `BMKG_API_KEY` exported,
run `go run .` from this module.

The HTTP process listens on all interfaces, and Compose's default mock port
mapping is not loopback-only. Use a trusted network/firewall or restrict the
host binding for standalone development.

- [Entrypoint, routes, authentication, and generator](main.go)
- [Embedded historical data](seed.json)
- [Canonical context diagram](../../docs/diagrams/mock-bmkg-context.mmd)
