# Hazard consumers

Two independent M1 Go modules consume Redpanda topic `bnpb.hazard-events`:
`bnpb/consumer-dashboard` and `bnpb/consumer-notifier`. Separate consumer groups
provide independent offsets and catch-up while broker data is retained.

The dashboard logs process-local counters; the notifier logs alert decisions.
There is no browser dashboard or SMS/webhook/push delivery. Durable analytics
and notification delivery are not implemented M2 capabilities.

## Input and routes

Kafka key: `hazard_id`; value: raw canonical HazardEvent JSON, **no wrapper**.
Full payload shape: frozen [HazardEvent schema](../../contracts/hazard_event.json);
the consumers implement the M1 subset, not all planned types in that schema.
An optional `X-Correlation-ID` message header carries tracing context.

| Process | HTTP route | Default internal port |
| --- | --- | --- |
| Dashboard | `GET /health` — process health | `8085` |
| Notifier | `GET /health` — process health | `8086` |

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

## Configuration

These are **process defaults**; Compose supplies container broker addresses.

| Variable | Default / requirement |
| --- | --- |
| `REDPANDA_BROKERS` | `localhost:19092`; Compose uses `redpanda:9092`. |
| `TOPIC` | `bnpb.hazard-events`; must match Aggregator's topic. |
| `CONSUMER_GROUP` | `bnpb-dashboard-group` / `bnpb-notifier-group`. |
| `PORT` | `8085` / `8086`, respectively. |

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
- [Canonical fan-out diagram](../../docs/diagrams/consumers-fanout.mmd)
