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

## Input and routes

Kafka key: `hazard_id`; value: raw canonical HazardEvent JSON, **no wrapper**.
Full payload shape: frozen [HazardEvent schema](../../contracts/hazard_event.json);
the consumers implement the M1 subset, not all planned types in that schema.
An optional `X-Correlation-ID` message header carries tracing context.

| Process | HTTP route | Default internal port |
| --- | --- | --- |
| Dashboard | `GET /health` — process health | `8085` |
| Notifier | `GET /health` — process health | `8086` |
| Summarizer | `GET /health`, `GET /summaries`, `GET /summaries/latest`, `GET /metrics` | `8087` |

Neither process exposes a business HTTP API. Compose does not publish these
health ports to the host; health alone does not establish end-to-end delivery.

## Processing behavior and limits

- Dashboard counts first-seen IDs by hazard type and severity.
- Notifier logs `ALERT` for seismic magnitude ≥ `6.0`, nested tsunami warning
  `SIAGA`/`AWAS`, or volcanic severity `AWAS`. Other first-seen hazards log
  `no alert`; volcanic `SIAGA` and tsunami `WASPADA` alone do not trigger alerts.
- Both use **unbounded, process-local sets keyed only by `hazard_id`**.
  Later same-ID payloads are skipped and committed even when attributes or
  severity change. An initial non-alert can suppress a later escalation.
- Restart clears seen-ID sets and counters. Committed messages are not replayed
  merely to rebuild state; rebalances/replicas do not transfer or share it.
- Valid messages are committed after processing; a crash before commit can
  repeat processing after restart. This is not exactly-once side effects.
- Malformed messages are logged, skipped, and explicitly committed. There is
  no DLQ/replay workflow yet ([#26](https://github.com/Darsua/AAT/issues/26)).
- The producer can coalesce pending revisions. Offset catch-up neither proves
  delivery of every revision nor reconstructs a complete dashboard.

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

Compose maps `KAFKA_TOPIC_HAZARDS` to `TOPIC` and the per-service
`CONSUMER_GROUP_DASHBOARD` / `CONSUMER_GROUP_NOTIFIER` settings to `CONSUMER_GROUP`.
Keep different group IDs for independent fan-out.

## Run, test, and code

Use the [development guide](../../docs/development.md) for Compose startup,
verification, and consumer downtime/catch-up checks. For a host process with
reachable brokers, run `go run .` from `dashboard/` or `notifier/` in separate
terminals. Health listeners bind all interfaces.

- [Dashboard entrypoint and processing](dashboard/main.go)
- [Notifier entrypoint and alert rules](notifier/main.go)
- [Summarizer entrypoint and async engine](summarizer/main.go)
- [Canonical fan-out diagram](../../docs/diagrams/consumers-fanout.mmd)
