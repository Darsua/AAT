# Problem 4: Flexible storage and restart recovery — historical M1 evidence

**Recorded date: 2026-10-08.** This summarizes the retained M1 run, not current validation or uninterrupted/node-loss availability. Nothing was rerun for this documentation cleanup.

## Run and observations

[`storage_restart.log`](storage_restart.log) records [`test_storage_restart.sh`](../../../scripts/test_storage_restart.sh):

- SQL inspection found **1,633 hazards**, including **771** with `confidence_level` and **45** with baseline volcanic attributes but no `confidence_level`. The sample includes BMKG and PVMBG records with different JSONB keys in the same `hazards` table. Additive attributes required **no DDL change**; ordinary SQL writes still acquire database locks.
- After `docker compose restart aggregator` returned, sampled `/health` checks on client-api, mock-bmkg, and mock-pvmbg succeeded. The script then polled Aggregator health and waited three seconds before printing its reconnection/resumption message.
- After `docker compose restart canonical-db`, `pg_isready` reported PostgreSQL accepting connections. A subsequent authenticated gateway query returned HTTP 200 and a hazard record without application-container restarts.

## Method limitations

The script checks HTTP health **after** the Aggregator restart command completes, not continuously during downtime. It therefore does **not** prove uninterrupted gateway query availability or absence of degradation. The Aggregator wait loop does not assert successful readiness on exhaustion, and its resumption message is not an independent check of ingestion or outbox progress. The post-database-restart query demonstrates recovery at that sample, not a measured recovery time. Containers were restarted, not rebuilt; no node-loss or multi-replica test is shown.

The storage decision and alternatives belong in [ADR 003](../../adr/003-canonical-storage-postgres-jsonb.md), not a second comparison table here. MongoDB supports `$lookup` joins and multi-document transactions on a replica set (one member is sufficient); this run did not compare database engines.
