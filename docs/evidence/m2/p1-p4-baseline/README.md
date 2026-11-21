# P1, P4, Audit, Satellite, Analytics, Summarizer & Observability Baseline (M2)

**Recorded date: 2026-11-20 (15:00–15:14 UTC).** Non-destructive live baseline verification on the 3-node M2 k3s cluster (`aqua`, `ruby`, `akane`; namespace `sistem-informasi-kebencanaan-bnpb`). Notice this baseline is distinct from chaos fault injection drills (`#31`).

## 1. Cluster Topology, Governance & Tester Agent ([`cluster-baseline.log`](cluster-baseline.log))

- **Nodes:** 3 distinct Debian Trixie nodes on `v1.37.2+k3s1`: `aqua` (`control-plane`, kernel `6.12.117+deb13-cloud-amd64`), `ruby` (worker, kernel `6.12.117+deb13-cloud-amd64`), and `akane` (worker, kernel `6.12.115+deb13.1-cloud-amd64`).
- **Workloads:** `store-aqua-0`, `store-ruby-0`, `store-akane-0`, `redpanda-0`, `aggregator` (3/3), `client-api` (2/2), `auth` (1/1), `dashboard-consumer` (2/2), `notifier-consumer` (2/2), `summarizer-consumer` (1/1), `mock-bmkg` (1/1), `mock-pvmbg` (1/1), and `sensor-pantai` (1/1).
- **Governance:** `LimitRange/table-b-resource-limits`, `NetworkPolicy` set (`allow-ingress-controllers`, `allow-mock-admin-policy`, `isolate-broker-policy`, `isolate-storage-strict`), and `HorizontalPodAutoscaler` (`aggregator` 3–5 at 44%/70% CPU; `client-api` 2–6 at 1%/70% CPU).
- **Tester Agent:** All 3 DaemonSet pods running in `tester-agent`; `/app list` reports `clock-skew`, `kill-leader`, `kill-store`, `partition`, `schema`, `wipe-node`.

## 2. Canonical Storage NetworkPolicy Isolation ([`networkpolicy-isolation.log`](networkpolicy-isolation.log))

Verified `NetworkPolicy/isolate-storage-strict` (`role=store` TCP `5432` ingress restricted to `role=aggregator`, `role=store`, and `tester-agent` namespace):

| Caller pod | `store-aqua:5432` | `store-ruby:5432` | `store-akane:5432` |
| --- | --- | --- | --- |
| `aggregator-547cc65b86-lnwrg` | Allowed (`exit 0`) | Allowed (`exit 0`) | Allowed (`exit 0`) |
| `client-api-5c76cd958-l9ff9` | Blocked (`exit 1`) | Blocked (`exit 1`) | Blocked (`exit 1`) |
| `dashboard-consumer-555c7f744c-fhgrq` | Blocked (`exit 1`) | Blocked (`exit 1`) | Blocked (`exit 1`) |

## 3. Audit API & Sharded PostgreSQL Distribution ([`audit-verification.json`](audit-verification.json), [`shard-distribution.log`](shard-distribution.log))

Ran [`scripts/verify_audit_and_catalog.py`](../../../../scripts/verify_audit_and_catalog.py) against `https://audit-aat.darsu.dev` (`--page-limit 1000 --sample-locations 30`):

- **Passed structural checks:**
  - `events_pagination_and_ordering`: `passed: true` (`51,792` events across `52` pages, strict `(ingested_at, hazard_id)` ordering, `partial: false`, `unavailable_units: []`).
  - `checksum_BMKG`: `passed: true` (`1,959 == 1,959`, exact SHA-256 `60c3db0e09426273e0cbc20906c1a4937990c57465e590af037d295460d9c462`).
  - `checksum_PVMBG`: `passed: true` (`1,959 == 1,959`, exact SHA-256 `0b07c8e9289784ff7a17cef535992d4aef728d0aed19f072546c9692a0494c49`).
  - `location_sharding_and_replication`: `passed: true` (`30` sampled events spread across `aqua`, `ruby`, `akane` and all 6 units `shard-1`, `shard-1-r1`, `shard-2`, `shard-2-r1`, `shard-3`, `shard-3-r1`; `no_node_holds_all_records: true`).
  - `unprocessed_dlq_endpoint`: `passed: true` (`0` parked messages).
- **Live-ingestion drift note:** `verify_audit_and_catalog.py` reported `passed: false` solely because active streaming ingested additional `SATELIT` (+1: `33,563` vs `33,562`) and `SENSOR_PANTAI` (+45: `14,357` vs `14,312`) events during the ~50 s 52-page `/internal/audit/events` enumeration before `get_checksum()` ran (`ALL`: `51,800` vs `51,792`). Direct SQL queries in [`shard-distribution.log`](shard-distribution.log) confirm balanced per-shard primary row counts across all 3 nodes (`bnpb_shard_1`: 639 BMKG / 629 PVMBG / 10,655 SATELIT / 7,910 SENSOR_PANTAI; `bnpb_shard_2`: 632 / 656 / 10,744 / 7,914; `bnpb_shard_3`: 750 / 735 / 12,242 / 9,367).

## 4. Satellite Wildfire Stream & Coastal Poller ([`satellite-coastal.log`](satellite-coastal.log))

- Aggregator `/internal/hazards` metadata reported `degraded: false, stale_since: null` across `ALL` (`50,719`), `SEISMIC` (`1,953`), `VOLCANIC` (`1,953`), `WILDFIRE` (`33,560`), and `COASTAL` (`13,300`).
- Leader `aggregator-547cc65b86-nf9ld` reached `lag_events: 0` (`last_applied_offset: 2127952`, `latest_offset: 2127952`, `p95_persist_ms: 0.172982`, `active_fires: 394`, `singletons: 212643`) and exclusively polled `sensor-pantai` (`200 OK`, 1–5 ms).

## 5. Analytics Dashboard & GPU LLM Summarizer ([`analytics-summarizer.log`](analytics-summarizer.log))

- `https://aat.darsu.dev/dashboard/api/analytics?window=all` reported `63,447` hazards (`partial_data: false`, `degraded: false`, `correlated_tsunami_count: 289`, `source_ref_checksum: 399cba104b82456711ac825ea39dc9dac1121337379a330d1aa0cd630c5ef680`); `window=24h` reported `37,703` hazards (`partial_data: false`, `degraded: false`, `source_ref_checksum: 0ad4f5d626826ac0f134d7896e768443ff53d82234c8eac0e9f2741e975f8eb7`; includes upstream satellite events timestamped `2027-05-30`–`2027-06-01`). In the archived log, `by_area` and `timeseries` arrays were truncated to compact samples.
- `https://aat.darsu.dev/summaries/latest` returned `model: "qwen3.5:9b"`, `circuit_state: "closed"`, `generator_mode: "llm"`, `degraded: false`, `llm_success_total: 83`, `fallback_total: 1` (`validation_failures_total: 1`), `last_latency_ms: 7344`, and valid named province/region clusters. Internal health checks on `dashboard-consumer:8085/health` returned `{"status":"UP"}`, while `summarizer-consumer:8087/health` returned `{"circuit_state":"closed","service":"summarizer-consumer","status":"ok"}`.

## 6. Unified Observability & Trace Correlation ([`observability-trace.log`](observability-trace.log))

- Scraped `/metrics` from all 8 Go application services (`aggregator`, `auth`, `client-api`, `dashboard-consumer`, `notifier-consumer`, `summarizer-consumer`, `mock-bmkg`, `mock-pvmbg`), plus `/healthz` (`200 OK`) on external simulator `sensor-pantai:8080`.
- **Synchronous HTTP trace (`X-Correlation-ID: m2-cert-sync-001`):** `client-api` propagated `correlation_id="m2-cert-sync-001"` and `trace_id="3f2324413c0b6dbf0f7f503fcd4d301a"` to `aggregator` (`GET /internal/hazards`, `200`, `13 ms`).
- **Asynchronous field-report trace (`X-Correlation-ID: m2-cert-trace-001`):** `client-api` (`POST /api/v1/field-reports` -> `202`, `rep-idem-c315be801d25171b`) -> `aggregator` (`persisted field report hazard`, `haz-field-rep-idem-c315be801d25171b`, replicated on `aqua`/`shard-1` primary and `ruby`/`shard-1-r1` replica) -> `dashboard-consumer` and `notifier-consumer` (both logging `correlation_id="m2-cert-trace-001"` and outbox `trace_id="4c1288a9d4522d8bbd5c727c1bbd082d"`).
