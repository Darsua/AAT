# Aggregator

`bnpb/aggregator` polls the BMKG and PVMBG mocks, normalizes M1 hazards,
and publishes canonical payloads through a PostgreSQL transactional outbox.
It exclusively owns `canonical-db`; other services read through its HTTP API.

The runtime supports `SEISMIC` and `VOLCANIC` only. Broader types, field-report
writes, and audit behavior in frozen contracts are planned M2 capabilities,
not implemented endpoints or an immutable event history.

## Routes and contracts

Internal base URL: `http://aggregator:8083`.

| Route | Current behavior |
| --- | --- |
| `GET /health` | Checks database and broker dependencies. |
| `GET /internal/hazards` | Filtered, paginated `{data, meta}` list with degradation metadata. |

List filters: `type`, `severity`, `since` (UTC RFC3339), `limit`, `offset`.
There is no internal get-by-ID route. Full shapes are in the frozen
[internal API](../../contracts/aggregator_internal.yaml) and
[HazardEvent schema](../../contracts/hazard_event.json).

## Configuration

These are **process defaults**, not Compose credentials or host mappings.

| Variable | Default / requirement |
| --- | --- |
| `PORT` | `8083` |
| `DATABASE_URL` | Required; no default. |
| `BMKG_KEY`, `PVMBG_TOKEN` | Empty; supply matching mock credentials for ingestion. |
| `BMKG_URL` | `http://mock-bmkg:8081` |
| `PVMBG_URL` | `http://mock-pvmbg:8082` |
| `REDPANDA_BROKERS` | `redpanda:9092` |
| `TOPIC` | `bnpb.hazard-events` |
| `POLL_INTERVAL_SECS` | `3` |

Compose maps `BMKG_API_KEY` to `BMKG_KEY` and `KAFKA_TOPIC_HAZARDS` to `TOPIC`.
Override development credentials through the environment.

## Ingestion and delivery limits

- BMKG joins tsunami warnings to seismic events. PVMBG names and coordinates
  come from the static volcano catalog.
- **Only PVMBG** captures unknown additive fields into canonical `attributes`.
  Typed BMKG DTOs discard unknown fields; breaking field changes are unsupported.
- Pollers use backoff/jitter and process-local source-staleness tracking,
  not a circuit breaker. Stored hazards remain readable during upstream outages.
- Each `(source, source_ref_id)` has current hazard state and one outbox row.
  Pending revisions can coalesce into the latest payload: this is not event
  sourcing and does not guarantee delivery of every intermediate revision.
- Pending valid payloads are retried with at-least-once semantics; duplicates
  are possible. Single-node Compose does not establish node-loss durability.
- Kafka key: `hazard_id`; value: raw canonical HazardEvent JSON, without a
  wrapper; optional header: `X-Correlation-ID`. Hash partition affinity does
  not guarantee event-time/revision ordering across concurrent relays/retries.

## Run, test, and code

Use the [development guide](../../docs/development.md) for Compose startup,
verification, and outage/schema demos. For a host process, provision a reachable
PostgreSQL instance and override container-only upstream/broker addresses;
from this module run `go run ./cmd/server`. Compose does not publish PostgreSQL,
and the HTTP listener binds all interfaces.

- [Bootstrap and routes](cmd/server/main.go)
- [Adapters](internal/adapter/) and [normalization](internal/normalizer/normalizer.go)
- [Repository](internal/repository/hazard_repo.go) and [migrations](migrations/)
- [Pollers](internal/worker/poller.go) and [outbox relay](internal/worker/outbox_relay.go)
- [Canonical flow diagram](../../docs/diagrams/aggregator-flow.mmd)
