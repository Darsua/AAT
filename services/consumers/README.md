# Hazard consumers

Three independent Go modules consume Redpanda topic `bnpb.hazard-events`:
`bnpb/consumer-dashboard`, `bnpb/consumer-notifier`, and `bnpb/consumer-summarizer`
(plus optional `bnpb.field-reports` in Kubernetes). Separate consumer groups
provide independent offsets and catch-up while broker data is retained.

The dashboard logs process-local counters; the notifier logs alert decisions; the
summarizer maintains a bounded sliding window (`MAX_RECENT_EVENTS=200`) and
asynchronously generates labeled situational briefs (`generated: true`) via an
OpenAI-compatible endpoint with deterministic extractive fallback
([ADR 008](../../docs/adr/008-non-blocking-ai-situation-summarizer.md)).

The dashboard serves a browser-accessible multi-hazard analytics UI and JSON
API backed by update-aware Kafka projection + periodic canonical reconciliation
against [Aggregator](../aggregator/README.md) HTTP (never direct PostgreSQL).
The notifier remains M1 log-only alert evaluation until #29 lands.

## Input and routes

Kafka key: `hazard_id`; value: raw canonical HazardEvent JSON, **no wrapper**.
Full payload shape: frozen [HazardEvent schema](../../contracts/hazard_event.json).
An optional `X-Correlation-ID` message header carries tracing context.

| Process | HTTP route | Default port |
| --- | --- | --- |
| Dashboard | `GET /`, `GET /dashboard` — web analytics UI | `8085` (published to host) |
| Dashboard | `GET /api/analytics`, `GET /dashboard/api/analytics` — JSON analytics (`?window=all\|1h\|6h\|24h\|7d\|30d` or `?since=...`) | `8085` |
| Dashboard | `GET /health` — process health | `8085` |
| Notifier | `GET /health` — process health | `8086` |
| Summarizer | `GET /health`, `GET /summaries`, `GET /summaries/latest`, `GET /metrics` | `8087` |

Neither process exposes a business HTTP API. Compose does not publish these
health ports to the host; health alone does not establish end-to-end delivery.

## Processing behavior and limits

- **Dashboard update-aware projection & reconciliation (#24)**:
  - Stores one canonical record per `hazard_id`; newer `ingested_at` updates (or
    changed severity/type/area/attributes) adjust counts in place without
    double-counting, while older or identical payloads are skipped.
  - Periodically reconciles via `GET {AGGREGATOR_URL}/internal/hazards` (pages
    of 500): on a complete non-degraded snapshot, removes retracted hazards; on
    partial-shard headers (`X-Storage-Partial: true`, `X-Unavailable-Units`),
    `meta.degraded=true`, or Aggregator failure, keeps the last-known view and
    flags `status.partial_data=true` so the UI shows a prominent partial-data
    outage banner instead of presenting incomplete totals as complete.
  - Correlated tsunami warnings/advisories (`SEISMIC` events with
    `attributes.potential_tsunami=true` or `attributes.tsunami_warning`) are
    surfaced in `correlated_tsunami_count` as a subset of `SEISMIC` without
    double-counting `total_hazards` (the frozen `hazard_type` enum has no
    separate `TSUNAMI` value).
  - Restart rebuilds state immediately from the Aggregator snapshot plus ongoing
    Kafka consumption.
- **Notifier (M1)**:
  - Logs `ALERT` for seismic magnitude >= `6.0`, nested tsunami warning
    `SIAGA`/`AWAS`, or volcanic severity `AWAS`. Other first-seen hazards log
    `no alert`.
  - Still uses an **unbounded, process-local set keyed only by `hazard_id`**
    until #29 updates it.
- Valid messages are committed after processing; malformed messages are logged,
  skipped, and committed.

## Integration for Dashboard (#24) & Observability (#35)

- **Web Analytics Dashboard (`#24`):** Optionally query `GET http://summarizer-consumer:8087/summaries/latest` with a short client timeout (`<= 500ms`) so dashboard rendering never blocks if the summarizer is down or restarting. When `latest` is present, display `latest.text`, `latest.generator_mode` (`"llm"` or `"extractive-fallback"`), `latest.degraded` / `latest.degraded_reason`, `active_correlations`, and the `advisory_disclaimer` (`generated: true` label).
- **Prometheus (`#35`):** Scrape `summarizer-consumer:8087` at `GET /metrics` for `bnpb_summarizer_events_consumed_total`, `bnpb_summarizer_summaries_generated_total{mode="llm"|"extractive-fallback"}`, `bnpb_summarizer_coalesced_total`, `bnpb_summarizer_quota_exhausted_total`, `bnpb_summarizer_circuit_open`, `bnpb_summarizer_last_latency_ms`, and `bnpb_summarizer_window_events`.

## Configuration

These are **process defaults**; Compose supplies container broker addresses.

| Variable | Default / requirement |
| --- | --- |
| `REDPANDA_BROKERS` | `localhost:19092`; Compose uses `redpanda:9092`. |
| `TOPIC` | `bnpb.hazard-events`; must match Aggregator's topic. |
| `CONSUMER_GROUP` | `bnpb-dashboard-group` / `bnpb-notifier-group` / `bnpb-summarizer-group`. |
| `PORT` | `8085` / `8086` / `8087`, respectively. |
| `AI_API_URL`, `AI_MODEL`, `AI_API_KEY` | Summarizer-only OpenAI-compatible inference settings; falls back to extractive mode when disabled, throttled, or unreachable. |
| `AGGREGATOR_URL` | `http://aggregator:8083` (dashboard periodic canonical sync). |
| `DASHBOARD_SYNC_INTERVAL_SECS` | `5` (dashboard canonical reconciliation interval). |

Compose maps `KAFKA_TOPIC_HAZARDS` to `TOPIC` and the per-service
`CONSUMER_GROUP_DASHBOARD` / `CONSUMER_GROUP_NOTIFIER` settings to `CONSUMER_GROUP`.
Keep different group IDs for independent fan-out.

## Run, test, and code

Use the [development guide](../../docs/development.md) for Compose startup,
verification, and consumer downtime/catch-up checks. For a host process with
reachable brokers, run `go run .` from `dashboard/` or `notifier/` in separate
terminals. Health listeners bind all interfaces.

- [Dashboard entrypoint and processing](dashboard/main.go)
- [Dashboard analytics, sync, and web UI](dashboard/analytics.go)
- [Notifier entrypoint and alert rules](notifier/main.go)
- [Summarizer entrypoint and async engine](summarizer/main.go)
- [Canonical fan-out diagram](../../docs/diagrams/consumers-fanout.mmd)
