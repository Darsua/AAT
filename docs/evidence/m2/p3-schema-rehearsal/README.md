# Problem 3: PVMBG schema changes, local rehearsal

**Date: 2026-11-17 (12:09–12:13 UTC).** A local rehearsal on one developer machine, not the Tester Agent run and not cluster evidence. It used an unmerged combination of [#45](https://github.com/Darsua/AAT/pull/45) (mock schema modes) and [#47](https://github.com/Darsua/AAT/pull/47) (aggregator DLQ and replay) at `a0c30a8` (#47) merged locally with `1d4ad63` (#45), built as plain binaries.

## Setup

- One `aggregator`, `mock-bmkg`, and `mock-pvmbg` process, with a fresh local PostgreSQL 18 database as a single storage unit. No broker was running, so Kafka publishing was not exercised.
- `mock-pvmbg` delay 50–150 ms; it generates a report every 10 s. Aggregator poll interval 3 s.
- Modes were switched with `POST /admin/schema-version`. Each snapshot records the PVMBG hazard count, `dlq_messages` rows, and `GET /internal/audit/unprocessed?source=PVMBG`.

## Results

[`strict-policy.log`](strict-policy.log), default policy (no aliases, no coercion):

| Step | PVMBG hazards | Parked | Reason |
| --- | --- | --- | --- |
| Baseline, `additive` | 22 | 0 | – |
| 25 s `rename_field` | 22 | 4 | `missing required field 'report_id'` |
| 25 s `type_change` | 22 | 11 | `field 'eruption_count_24h' changed from integer to string` |
| 25 s `missing_required_field` | 22 | 20 | `missing required field 'volcano_id'` |
| 15 s back to `additive` | 31 | 20 | – |

No incompatible record reached `hazards`. Each rejected record was parked once (`attempts` = 1) and listed by the Audit API. After returning to `additive`, the reports generated during the incompatible window were ingested (22 → 31), because the cursor did not advance past rejected records.

[`replay-policy.log`](replay-policy.log) and [`replay-cycle.log`](replay-cycle.log), aggregator restarted with `PVMBG_FIELD_ALIASES=event_id=report_id` and `PVMBG_COERCE_NUMERIC_STRINGS=true`:

- The first replay cycle scanned 20 parked messages and marked 11 `recovered` (4 `rename_field` + 7 `type_change`). The 9 `missing_required_field` messages stayed parked, as no policy accepts them; the Audit API then listed only those 9.
- With the policy on, 25 s of live `rename_field` traffic was ingested directly (38 → 40 hazards) with no new parked rows.
- At the end the database held 40 PVMBG hazards (`pvmbg_hazards: 40` in `replay-policy.log`), so replay created no duplicates.

## Limitations

Single process and single storage unit, so lease failover, replica sync, and the `recovered` guard across units were not exercised here (they are covered by unit tests in #47). No broker, so outbox publication was not checked. Hazard counts include reports the generator added during the run. This needs repeating with the Tester Agent on the M2 deployment before it is used as assessed evidence.
