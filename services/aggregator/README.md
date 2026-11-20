# Aggregator

`bnpb/aggregator` polls upstream hazard feeds (BMKG and PVMBG), normalizes
canonical events, replicates them across sharded PostgreSQL units, serves
internal hazard and Appendix A Audit APIs, and publishes outbox records to
Redpanda. It exclusively owns canonical database shards; other services read
through its HTTP API.

For sharded consistent hashing, cross-node anti-entropy repair, and HA leader
leasing/fencing design decisions, see [ADR 006](../../docs/adr/006-sharded-storage-consistent-hashing-and-audit-api.md)
and [ADR 007](../../docs/adr/007-aggregator-ha-leader-leasing-and-fencing.md).

## Routes and contracts

Internal base URL: `http://aggregator:8083`.

| Route | Auth | Behavior |
| :--- | :--- | :--- |
| `GET /health` | None | Checks reachable shards and broker dependencies; returns `503` while draining. |
| `GET /internal/hazards` | None (internal net) | Filtered, paginated `{data, meta}` list with degradation and partial-shard metadata. |
| `GET /internal/audit/events` | `Bearer AUDIT_TOKEN` | Scatter-gather events ordered `(ingested_at ASC, hazard_id ASC)` with keyset cursor. |
| `GET /internal/audit/checksum` | `Bearer AUDIT_TOKEN` | SHA-256 over raw-byte sorted `source_ref_id` values joined by `\n` (all or per-source). |
| `GET /internal/audit/location` | `Bearer AUDIT_TOKEN` | Reports `{node, unit, role}` shard/replica placement for a `hazard_id` (`404` vs `503`). |
| `GET /internal/audit/unprocessed` | `Bearer AUDIT_TOKEN` | Paginated dead-letter / retrying entries from `dlq_messages`. |

List filters on `/internal/hazards`: `type`, `severity`, `since` (UTC RFC3339),
`limit`, `offset`. Full API shapes are defined in the frozen
[internal API](../../contracts/aggregator_internal.yaml),
[Audit API](../../contracts/audit_api.yaml), and
[HazardEvent schema](../../contracts/hazard_event.json).

## Configuration

These are **process defaults**, not Compose credentials or host mappings.

| Variable | Default / requirement |
| :--- | :--- |
| `PORT` | `8083` |
| `DATABASE_URL` | Fallback single-shard DSN when `SHARD_CONFIG` / `SHARD_TOPOLOGY_JSON` are empty |
| `SHARD_CONFIG`, `SHARD_TOPOLOGY_JSON` | Empty; configure multi-shard primary/replica units and worker nodes (`${VAR}` env expansion supported) |
| `NODE_NAME`, `STORAGE_UNIT_ID` | `local`, `shard-1` (fallback single-shard unit identity) |
| `AUDIT_TOKEN` | Empty (fail-closed `401` if unset on `/internal/audit/*`) |
| `AUDIT_ALL_SOURCES_LABEL` | `ALL` |
| `INSTANCE_ID`, `LEASE_TTL_SECS`, `LEASE_RENEW_INTERVAL_SECS` | Auto (`HOSTNAME`/`NODE_NAME-pid`), `15`, `5` |
| `DRAIN_DELAY_MS` | `0` (`3000` in K8s and `docker-compose.sharded.yml`) |
| `BMKG_KEY`, `PVMBG_TOKEN` | Empty; supply matching mock credentials for ingestion |
| `BMKG_URL` | `http://mock-bmkg:8081` |
| `PVMBG_URL` | `http://mock-pvmbg:8082` |
| `REDPANDA_BROKERS` | `redpanda:9092` |
| `TOPIC` | `bnpb.hazard-events` |
| `REDPANDA_TOPIC_FIELD_REPORTS` | `bnpb.field-reports` (staged field-report topic consumed into canonical storage) |
| `CONSUMER_GROUP_FIELD_REPORTS` | `bnpb-aggregator-field-reports` |
| `POLL_INTERVAL_SECS` | `3` |
| `SATELLITE_ENABLED` | `false` |
| `SATELLITE_URL` | `https://satellite.naufarrel.dev` |
| `SATELLITE_GROUP_TOKEN` | Required when enabled; secret, no default. |
| `SATELLITE_LATENESS_MINUTES` | `360` (event-time watermark) |
| `SATELLITE_STATE_RETENTION_HOURS` | `48`; fires this far behind the newest acquisition are closed and dropped from memory (minimum: lateness + 360 min) |
| `SATELLITE_PROVINCE_CENTROIDS_FILE` | Empty. JSON array of `{"name","latitude","longitude"}` reference-province centroids for `area_name`. |
| `PVMBG_FIELD_ALIASES` | Empty (no renamed fields accepted); `from=to` pairs, comma-separated. |
| `PVMBG_COERCE_NUMERIC_STRINGS` | `false` |
| `DLQ_RETRY_BUDGET` | `5` polls before a retryable rejection is parked. |

Compose maps `BMKG_API_KEY` to `BMKG_KEY` and `KAFKA_TOPIC_HAZARDS` to `TOPIC`.
Override development credentials through the environment.

## Ingestion and delivery limits

- BMKG joins tsunami warnings to seismic events. PVMBG names and coordinates
  come from the static volcano catalog.
- **Only PVMBG** captures unknown additive fields into canonical `attributes`.
  Typed BMKG DTOs discard unknown fields.
- PVMBG reports are validated one by one ([#26](https://github.com/Darsua/AAT/issues/26)).
  A missing or empty required field, a type change, invalid UTF-8, or malformed
  JSON rejects only that report; its raw bytes go to `dlq_messages` as `parked`
  before the cursor or checkpoint moves, and valid reports in the same response
  continue. A response that is not a JSON array is `retrying` (retryable) up to
  `DLQ_RETRY_BUDGET` polls, then `parked`; a response over 8 MiB is parked with
  its first 8 MiB only.
- `PVMBG_FIELD_ALIASES` (e.g. `event_id=report_id`) and
  `PVMBG_COERCE_NUMERIC_STRINGS=true` are the explicit system changes that accept
  the `rename_field` and `type_change` mock modes. The lease holder re-validates
  parked messages every 30 s, oldest first, applies those that now decode, and
  marks them `recovered` so they leave `GET /internal/audit/unprocessed`. A
  replayed report older than the stored hazard for the same `report_id` is not
  written. Missing `volcano_id` has no policy and stays parked.
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
- The satellite wildfire stream ([#28](https://github.com/Darsua/AAT/issues/28))
  runs only with `SATELLITE_ENABLED=true` and `SATELLITE_GROUP_TOKEN` set. Every
  replica may enable it: a replica streams only while it holds the
  `source:SATELIT` lease, and hazard and checkpoint writes are fenced, so a
  replica that loses the lease stops and a standby resumes from the checkpoint.
  Each feed event updates connected fires; changed fires are upserted as
  `SATELIT` / `WILDFIRE` hazards, then the engine snapshot and last offset are
  saved to `satellite_checkpoints` on the coordinator units (every 5 s and on
  shutdown). After a crash or failover the stream restores that checkpoint and
  replays the feed from its offset; hazard writes are idempotent upserts, so
  replay cannot skip events but can re-emit Kafka events. Merged or shrunken
  fires stay stored with severity `NORMAL` and
  `attributes.lifecycle_status = "retired"` (plus `retired_reason`,
  `merged_into`, `severity_before_retire`), because HazardEvent has no lifecycle
  field. Unparseable events are logged with their offset and counted, not
  retried. Progress, lag, and p95 persist latency are logged as
  `satellite_progress` every 10 s and exported on `/metrics` (`satellite_lag_events`,
  `satellite_last_applied_offset`, `satellite_latest_offset`,
  `satellite_event_persist_duration_seconds`, fire and disposition counts,
  `satellite_checkpoint_bytes`, and `satellite_streaming` = 1 on the lease
  holder), shown in the "Satellite wildfire stream (P4)" Grafana row.
- Before each checkpoint, fires whose newest detection is more than
  `SATELLITE_STATE_RETENTION_HOURS` behind the newest acquisition are closed:
  no new detection can join them, so their detections are dropped from memory
  and later checkpoints while the stored hazard stays as it is. Updates or
  retracts that arrive for a closed fire are counted
  (`satellite_unknown_references`) rather than applied, so the retention must
  exceed how late the feed revises old detections.
- Staged field reports from [Client API](../client-api/README.md) arrive on
  `REDPANDA_TOPIC_FIELD_REPORTS` (`bnpb.field-reports`) and are consumed under
  group `CONSUMER_GROUP_FIELD_REPORTS` (`bnpb-aggregator-field-reports`). To
  preserve the frozen `HazardEvent.source` enum without contract changes,
  `hazard_type` maps deterministically to `source` (`SEISMIC->BMKG`,
  `VOLCANIC->PVMBG`, `WILDFIRE->SATELIT`, `COASTAL->SENSOR_PANTAI`) with
  `hazard_id = "haz-field-" + report_id`, `source_ref_id = report_id`, and
  `attributes.submission_channel = "FIELD_REPORT"`. Offsets are committed only
  after transactional hazard+outbox persistence; matching `payload_sha256`
  redeliveries are deduplicated via `(source, source_ref_id)`, and malformed
  messages are parked in `dlq_messages` (`Retryable: false`) before committing.
- Compose keeps the Aggregator on the internal-only network, so it cannot reach
  the external feed there; use the Kubernetes deployment. The payload format was
  checked against the live feed on 2026-11-17: offsets start at 0, detections
  arrive as `{"offset","detection":{...}}` with a numeric `acq_time`, and updates
  as `{"offset","key","changes":{...}}` patches. Confidence `l` is excluded from
  fires but kept, so a later update can raise it. Retracts are
  `{"offset","reason","key"}`. Heartbeats with an `id` occupy an offset and are
  stepped over. Coordinates near zero can arrive in exponent form (`1.9e-4`)
  while keys spell them `0.00019`, so exponent text is rewritten to plain
  decimal without rounding. The feed allows **one stream connection per group
  token**: a second connection (another replica, a laptop, `curl`) closes the
  first with `too_many_connections`, so only the lease holder may stream.
  `area_name` is the nearest centroid from
  `SATELLITE_PROVINCE_CENTROIDS_FILE`, and falls back to a coordinate label
  while that reference table is not configured.

## Run, test, and code

Use the [development guide](../../docs/development.md) for Compose startup,
verification, and outage/schema demos. For a host process, provision a reachable
PostgreSQL instance and override container-only upstream/broker addresses;
from this module run `go run ./cmd/server`. Compose does not publish PostgreSQL,
and the HTTP listener binds all interfaces.

- [Bootstrap and routes](cmd/server/main.go) and [Catalog skew CLI](cmd/catalog-loader/main.go) (`make catalog-bench`)
- [Adapters](internal/adapter/) and [normalization](internal/normalizer/normalizer.go)
- [Sharding engine](internal/repository/sharding.go), [fencing & leases](internal/repository/fencing.go), [repository](internal/repository/hazard_repo.go), and [migrations](migrations/)
- [Audit API handler](internal/api/audit_handler.go) and [health/drain handler](internal/api/health.go)
- [Pollers](internal/worker/poller.go), [field-report consumer](internal/worker/field_reports.go), and [outbox relay](internal/worker/outbox_relay.go)
- [Satellite wildfire clustering and feed client](internal/satellite/)
- [Canonical flow diagram](../../docs/diagrams/aggregator-flow.mmd)
