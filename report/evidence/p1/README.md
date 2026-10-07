# Problem 1 Demonstration & Evidence: Dynamic Schema Evolution & Unknown Field Policy

**Date:** 07 October 2026  
**Component Owner:** Member A (Aldoy)  
**Target Problem:** Problem 1 (Arsitektur & Interoperabilitas Data)

---

## 1. Overview & Acceptance Criteria
This document records the empirical evidence for **Problem 1 (P1)**:
1. **P1-01 (Baseline PVMBG Schema):** System boots with baseline PVMBG schema without `confidence_level`, producing valid canonical `HazardEvent` records.
2. **P1-02 (Live Schema Evolution):** Schema change is triggered via `POST /admin/schema-version` without restarting or redeploying any service (`mock-pvmbg`, `aggregator`, `client-api`). Upstream immediately emits `confidence_level`.
3. **P1-03 (Unknown Field Policy & Dynamic Ingestion):** Aggregator ingests the dynamic field and maps unmapped attributes into the canonical `attributes` (PostgreSQL `JSONB`) column. Client API serves records with `confidence_level` in `attributes` with zero downtime, crash, or manual schema migration.

---

## 2. Test Execution Commands & Results

### Command 1: Live Schema Evolution Demo
```bash
./scripts/test_schema_change.sh
```

#### Step 0: Upstream Health Check
- Endpoint: `GET http://localhost:8082/health`
- Result: `[OK] mock-pvmbg is healthy.`

#### Step 1 & 2: Baseline Schema Verification (include_confidence_level = false)
- Trigger: `POST /admin/schema-version` with payload `{"include_confidence_level": false}`
- Upstream Response:
```json
{
  "include_confidence_level": false
}
```
- Sample Baseline Report (`GET /volcanic-reports`):
```json
{
  "report_id": "pvmbg-rpt-20260901-001",
  "volcano_id": "VLC-001",
  "alert_level": "Normal",
  "eruption_count_24h": 0,
  "ash_column_height_m": 0,
  "reported_at": "2026-09-01T04:15:00Z"
}
```
- Verification: `[OK] Verified: baseline reports do NOT contain confidence_level.`

#### Step 3 & 4: Live Schema Evolution (include_confidence_level = true)
- Trigger: `POST /admin/schema-version` with payload `{"include_confidence_level": true}`
- Upstream Response:
```json
{
  "include_confidence_level": true
}
```
- Sample Evolved Report (`GET /volcanic-reports`):
```json
{
  "report_id": "pvmbg-rpt-live-20261007-144647-000001",
  "volcano_id": "VLC-001",
  "alert_level": "Siaga",
  "eruption_count_24h": 1,
  "ash_column_height_m": 250,
  "reported_at": "2026-10-07T14:46:47.952251391Z",
  "confidence_level": 0.8
}
```
- Verification: `[OK] Verified: Upstream reports successfully include dynamic 'confidence_level'.`

#### Step 5: Canonical Ingestion & Client API Verification
- Queried: `GET http://localhost:8080/api/v1/hazards?type=VOLCANIC&fields=all&limit=5`
- Response Payload:
```json
{
  "data": [
    {
      "hazard_id": "haz-pvmbg-pvmbg-rpt-live-20261007-144707-000003",
      "source": "PVMBG",
      "source_ref_id": "pvmbg-rpt-live-20261007-144707-000003",
      "hazard_type": "VOLCANIC",
      "severity": "SIAGA",
      "area_name": "Gunung Semeru",
      "latitude": -8.1077,
      "longitude": 112.9224,
      "attributes": {
        "ash_column_height_m": 450,
        "confidence_level": 0.8200000000000001,
        "eruption_count_24h": 3,
        "volcano_id": "VLC-003"
      },
      "occurred_at": "2026-10-07T14:47:07.951865Z",
      "ingested_at": "2026-10-07T14:47:10.865165Z"
    }
  ],
  "meta": {
    "total": 23,
    "limit": 5,
    "offset": 0,
    "degraded": false,
    "stale_since": null
  }
}
```
- Outcome:
```
[OK] Verified: Canonical hazards successfully contain dynamic 'confidence_level' in attributes!
[OK] Aggregator digested dynamic schema without schema rupture or crash.
```

---

### Command 2: Mock Services Contract & Unit Tests
```bash
go test -count=1 -v ./services/mock-bmkg/... ./services/mock-pvmbg/...
```

#### Test Output:
```
=== RUN   TestBMKGSeedAndEndpoints
--- PASS: TestBMKGSeedAndEndpoints (0.00s)
=== RUN   TestBMKGEventGeneration
--- PASS: TestBMKGEventGeneration (0.00s)
PASS
ok  	bnpb/mock-bmkg	0.003s
=== RUN   TestPVMBGAuthSchemaAndOutageControls
--- PASS: TestPVMBGAuthSchemaAndOutageControls (0.00s)
=== RUN   TestPVMBGReportFilterAndGeneration
--- PASS: TestPVMBGReportFilterAndGeneration (0.00s)
PASS
ok  	bnpb/mock-pvmbg	0.003s
```
All contract validation tests passed with zero failures.

---

## 3. Unknown Field Policy Declaration
- **Policy:** **Additive Forwarding to Attributes (Flexible JSONB)**
- **Behavior:** Unmapped or newly introduced upstream fields (e.g., `confidence_level`) are automatically preserved and forwarded to the canonical `attributes` map rather than being silently dropped or causing JSON unmarshaling errors.
- **Breaking vs Additive Changes:**
  - *Supported:* Additive scalar and object fields (e.g., new metadata, confidence scores).
  - *Not Supported (Conscious Limitation):* Breaking changes such as renaming primary keys (`report_id`), changing primitive types of existing contract fields, or removing mandatory fields.
