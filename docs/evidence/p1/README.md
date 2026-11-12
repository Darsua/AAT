# Problem 1: Dynamic schema evolution — historical M1 evidence

**Recorded date: 2026-10-07.** These retained M1 observations are not current validation or M2 guarantees. Nothing was rerun for this documentation cleanup.

## Run and observations

- [`demo_p1.log`](demo_p1.log), from [`test_schema_change.sh`](../../../scripts/test_schema_change.sh): toggled PVMBG `include_confidence_level` from false to true via `POST /admin/schema-version`, without service restart/redeployment. Baseline reports omitted the field; subsequent reports included it.
- The gateway query `GET /api/v1/hazards?type=VOLCANIC&fields=all&limit=5` returned `attributes.confidence_level` (sample value `0.8200000000000001`), with `meta.degraded=false`. The earlier recorded example has `occurred_at=2026-10-07T14:47:07.951865Z` and `ingested_at=2026-10-07T14:47:10.865165Z`; see the raw log for full payloads.
- [`mock_contract_tests.log`](mock_contract_tests.log): historical `go test -count=1 -v ./services/mock-bmkg/... ./services/mock-pvmbg/...` passed the four seed/endpoint, generation/filter, and auth/schema/outage-control tests (each module reported `0.003s`).

## Scope and limitations

Unknown-field preservation here is **PVMBG-specific**: its decoder captures additive fields and normalization forwards them into canonical `attributes`/JSONB. BMKG uses typed DTOs that discard unknown fields; this is not an all-source forwarding policy. The demonstrated change is additive `confidence_level`, not proof of arbitrary nested-field or breaking-schema compatibility. Renamed keys, removed required fields, and changed primitive types remain outside this evidence.

The run shows the sampled API response after a live toggle, not continuous availability measurement. Storage and language decisions are recorded in [ADR 003](../../adr/003-canonical-storage-postgres-jsonb.md) and [ADR 001](../../adr/001-service-language-all-go.md).
