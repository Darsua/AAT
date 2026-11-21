# Hazard consumers

Three independent Go modules consume Redpanda topic `bnpb.hazard-events`:
`bnpb/consumer-dashboard`, `bnpb/consumer-notifier`, and `bnpb/consumer-summarizer`
(which also consumes `bnpb.field-reports` when `FIELD_REPORTS_TOPIC` is set).
Separate consumer groups provide independent offsets and catch-up while broker
data is retained.

- **Dashboard (`bnpb/consumer-dashboard`, `:8085`)** serves a browser-accessible
  multi-hazard analytics UI and JSON API backed by update-aware Kafka projection
  plus periodic canonical delta/checksum reconciliation against
  [Aggregator](../aggregator/README.md) HTTP (never direct PostgreSQL).
- **Notifier (`bnpb/consumer-notifier`, `:8086`)** evaluates seismic, tsunami,
  volcanic, and coastal alert rules with bounded payload-fingerprint deduplication,
  persists JSONL delivery receipts (`NOTIFIER_RECEIPT_LOG`), optionally forwards
  webhook alerts (`FIELD_TEAM_RECEIVER_URL`), and serves `GET /notifications`.
- **Summarizer (`bnpb/consumer-summarizer`, `:8087`)** maintains a bounded sliding
  window (`MAX_RECENT_EVENTS=200`) and asynchronously generates labeled
  situational briefs (`generated: true`) via an OpenAI-compatible endpoint with
  deterministic extractive fallback
  ([ADR 008](../../docs/adr/008-non-blocking-ai-situation-summarizer.md)).

## Input and routes

Kafka key: `hazard_id`; value: raw canonical HazardEvent JSON, **no wrapper**.
Full payload shape: frozen [HazardEvent schema](../../contracts/hazard_event.json).
An optional `X-Correlation-ID` message header carries tracing context.

| Process | HTTP route | Default port |
| --- | --- | --- |
| Dashboard | `GET /`, `GET /dashboard` — web analytics UI | `8085` (published to host) |
| Dashboard | `GET /api/analytics`, `GET /dashboard/api/analytics` — JSON analytics (`?window=all\|1h\|6h\|24h\|7d\|30d` or `?since=...`) | `8085` |
| Dashboard | `GET /health`, `GET /metrics` — process health and Prometheus metrics | `8085` |
| Notifier | `GET /notifications` — JSON snapshot of delivered alerts (`total`, `deliveries`) | `8086` |
| Notifier | `GET /health`, `GET /metrics` — process health and Prometheus metrics | `8086` |
| Summarizer | `GET /health`, `GET /summaries`, `GET /summaries/latest`, `GET /metrics` | `8087` (`127.0.0.1:8087` in Compose) |

## Processing behavior and limits

- **Dashboard update-aware projection & reconciliation (`#24`)**:
  - Stores one canonical record per `hazard_id`; newer `ingested_at` updates (or
    changed severity/type/area/attributes, including attribute-only
    `{"retracted": true}` messages) adjust counts in place without
    double-counting, while older or identical payloads are skipped.
  - When `AUDIT_TOKEN` is configured, reconciles every
    `DASHBOARD_SYNC_INTERVAL_SECS` (`5` s) via `GET /internal/audit/events?since=...`
    delta pages and `GET /internal/audit/checksum`, triggering a full rebuild
    only on checksum drift or every `DASHBOARD_FULL_SYNC_INTERVAL_SECS` (`300` s);
    falls back to `GET /internal/hazards` (pages of 500) if Audit is unconfigured
    or unavailable. On partial-shard responses (`partial: true` or
    `X-Storage-Partial: true`), `meta.degraded=true`, or Aggregator failure,
    keeps the last-known view and flags `status.partial_data=true` with
    `status.partial_reason` so the UI shows a prominent partial-data banner.
  - Correlated tsunami warnings/advisories (`SEISMIC` events with
    `attributes.potential_tsunami=true` or `attributes.tsunami_warning`) are
    surfaced in `correlated_tsunami_count` as a subset of `SEISMIC` without
    double-counting `total_hazards` (the frozen `hazard_type` enum has no
    separate `TSUNAMI` value).
  - Restart rebuilds state immediately from the Aggregator snapshot plus ongoing
    Kafka consumption.
- **Notifier (`#29`)**:
  - Evaluates `ALERT` for seismic magnitude >= `6.0`, nested tsunami warning
    `SIAGA`/`AWAS`, volcanic severity `AWAS`, or coastal `attributes.critical == true`
    / severity `AWAS`.
  - Uses a bounded FIFO SHA-256 payload-fingerprint map (`50,000` entries) so
    revised severity/attribute updates for the same `hazard_id` re-trigger alerts
    while identical redeliveries are suppressed.
  - Persists alert receipts to `NOTIFIER_RECEIPT_LOG` (reloaded on startup),
    optionally POSTs JSON payloads to `FIELD_TEAM_RECEIVER_URL` (2 attempts with
    W3C `traceparent` + `X-Correlation-ID`), retains a bounded `10,000`-entry
    in-memory delivery slice for `GET /notifications`, and records dedup /
    commits Kafka offsets strictly after delivery succeeds.
- Valid messages are committed after processing; malformed messages are logged,
  skipped, and committed. All three consumers exit non-zero (`1`) on unrecoverable
  Kafka reader/commit errors so container orchestrators restart them.

## Integration for Dashboard (#24) & Observability (#35)

- **Situational Summaries (`#36`):** Clients or operators can query
  `GET http://summarizer-consumer:8087/summaries/latest` (`https://aat.darsu.dev/summaries/latest`
  via Ingress) with a short timeout (`<= 500ms`). Responses include `latest.text`,
  `latest.generator_mode` (`"llm"` or `"extractive-fallback"`), `latest.degraded` /
  `latest.degraded_reason`, `active_correlations`, and the `advisory_disclaimer`
  (`generated: true` label). (`dashboard-consumer` does not call `summarizer-consumer`
  server-side.)
- **Prometheus (`#35`):** Scrape `dashboard-consumer:8085/metrics`,
  `notifier-consumer:8086/metrics`, and `summarizer-consumer:8087/metrics`
  (`bnpb_summarizer_events_consumed_total`,
  `bnpb_summarizer_summaries_generated_total{mode="llm"|"extractive-fallback"}`,
  `bnpb_summarizer_coalesced_total`, `bnpb_summarizer_quota_exhausted_total`,
  `bnpb_summarizer_circuit_open`, `bnpb_summarizer_last_latency_ms`, and
  `bnpb_summarizer_window_events`).

## Configuration

These are **process defaults**; Compose and Kubernetes supply container broker
addresses and tuned overrides.

| Variable | Default / requirement |
| --- | --- |
| `REDPANDA_BROKERS` | `localhost:19092`; Compose and K8s use `redpanda:9092`. |
| `TOPIC` | `bnpb.hazard-events`; must match Aggregator's topic. |
| `FIELD_REPORTS_TOPIC` | Empty (Summarizer optional secondary topic; `bnpb.field-reports` in Compose/K8s). |
| `CONSUMER_GROUP` | `bnpb-dashboard-group` / `bnpb-notifier-group` / `bnpb-summarizer-group`. |
| `PORT` | `8085` / `8086` / `8087`, respectively. |
| `AGGREGATOR_URL`, `AUDIT_TOKEN` | `http://aggregator:8083`, empty (Dashboard Audit delta/checksum sync). |
| `DASHBOARD_SYNC_INTERVAL_SECS`, `DASHBOARD_FULL_SYNC_INTERVAL_SECS` | `5`, `300` (Dashboard delta and full-reconciliation intervals). |
| `NOTIFIER_RECEIPT_LOG`, `FIELD_TEAM_RECEIVER_URL` | Empty (`/tmp/notifier-receipts.jsonl` in Compose/K8s), empty (optional webhook receiver URL). |
| `AI_API_URL`, `AI_MODEL`, `AI_API_KEY` | Summarizer OpenAI-compatible inference settings (`qwen3.5:9b`); falls back to extractive mode when empty, throttled, or unreachable. |
| `AI_DISABLE_THINKING` | `true` (sends `chat_template_kwargs.enable_thinking=false`). |
| `AI_TIMEOUT_MS`, `AI_MAX_TOKENS`, `AI_MAX_CALLS_PER_MIN` | Process defaults `20000` (`30000` in Compose/K8s), `160` (`120` in Compose/K8s), `12`. |
| `SUMMARY_MIN_INTERVAL_MS`, `AI_CB_COOLDOWN_SECS` | Process defaults `2000` (`15000` in Compose/K8s), `30` (`45` in Compose/K8s). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Empty; optional OTLP/HTTP trace endpoint for Dashboard and Notifier (`http://tempo:4318`). |

Compose maps `KAFKA_TOPIC_HAZARDS` to `TOPIC` and the per-service
`CONSUMER_GROUP_*` variables to `CONSUMER_GROUP`. Keep different group IDs for
independent fan-out.

## Run, test, and code

Use the [development guide](../../docs/development.md) for Compose startup,
verification, and consumer downtime/catch-up checks. For a host process with
reachable brokers, run `go run .` from `dashboard/`, `notifier/`, or
`summarizer/` in separate terminals. HTTP listeners bind all interfaces.

- [Dashboard entrypoint and processing](dashboard/main.go)
- [Dashboard analytics, sync, and web UI](dashboard/analytics.go)
- [Notifier entrypoint and alert rules](notifier/main.go)
- [Summarizer entrypoint and async engine](summarizer/main.go)
- [Canonical fan-out diagram](../../docs/diagrams/consumers-fanout.mmd)
