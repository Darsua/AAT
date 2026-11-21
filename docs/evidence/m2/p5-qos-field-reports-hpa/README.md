# Problem 5: Field-Report Ingestion, QoS Load Shedding & HPA Autoscaling (M2)

**Recorded date: 2026-11-20 (15:21–15:24 UTC).** Non-destructive verification of Problem 5 field-report ingestion (`#27`), priority QoS load shedding (`#29`), `GET /api/v1/hazards/{id}` O(1) shard point lookup, and bonus HPA scaling (`#37`) on the 3-node k3s cluster.

## 1. Field-Report RBAC, Durable Staging, Idempotency & O(1) Lookup ([`field-reports-e2e.log`](field-reports-e2e.log))

| Check | Outcome |
| --- | --- |
| Media JWT -> `POST /api/v1/field-reports` | `HTTP 403 Forbidden` (`hazards:read:all` scope required) |
| Field JWT + `Idempotency-Key: m2-cert-idem-201` | `HTTP 202 Accepted` (`report_id`: `rep-idem-ce1d6666ada4d230`) |
| Identical replay (`m2-cert-idem-201`) across replicas | `HTTP 202 Accepted` with identical `report_id`; `aggregator` logged `skipped duplicate field report` |
| Mutated payload (`severity: "AWAS"`) + `m2-cert-idem-201` | `HTTP 409 Conflict` (`idempotency key reused with a different request payload`) |
| Alias route `POST /field-reports` (`m2-cert-idem-202`) | `HTTP 202 Accepted` (`report_id`: `rep-idem-da8ef065e757863f`) |
| `GET /internal/audit/location?hazard_id=haz-field-rep-idem-ce1d6666ada4d230` | `ruby`/`akane` sharded placement (`shard-2` primary on `ruby`, `shard-2-r1` replica on `akane`) |
| `GET /api/v1/hazards/haz-field-rep-idem-ce1d6666ada4d230?fields=all` | `HTTP 200 OK` (single O(1) `?id=` lookup in `aggregator`) |
| `GET /api/v1/hazards/haz-field-nonexistent` | `HTTP 404 Not Found` |

## 2. 50-VU Concurrency Baseline, QoS `429` Shedding & HPA Scaling ([`qos-load-hpa.log`](qos-load-hpa.log))

Both phases hit `https://aat.darsu.dev` via persistent TLS keep-alive workers while `mock-pvmbg` enforced `PVMBG_DELAY_MIN_MS=500` and `PVMBG_DELAY_MAX_MS=3000`.

### Phase 1: 50-VU Baseline Concurrency (`25` Field `SEISMIC` + `25` Media `VOLCANIC`, `limit=50`, `24.02 s`)

| Group | Requests | RPS | HTTP Statuses | Latency (`p50` / `p95` / `p99` ms) | Error rate |
| --- | ---: | ---: | --- | --- | ---: |
| Field (`25` VUs) | `2,337` | `97.29` | `200`: `2,337` | `192.89` / `328.50` / `857.64` | `0.00%` |
| Media (`25` VUs) | `2,488` | `103.58` | `200`: `2,488` | `169.19` / `327.92` / `1264.88` | `0.00%` |
| **Total (`50` VUs)** | **`4,825`** | **`200.87`** | **`200`: `4,825`** | **`179.02` / `328.42` / `1182.92`** | **`0.00%`** |

### Phase 2: Saturation Burst (`160` Field + `80` Media workers, `limit=500`, `50.00 s`) & HPA Scale-Up

- **Priority QoS shedding (`media_shed_threshold=70`, `max_concurrent=100`):** Across the `3,799` Media requests during Phase 2 saturation, `client-api` immediately shed **`3,141` Media requests with `HTTP 429 Too Many Requests`** (`p50=99.33 ms`, alongside `656` `HTTP 500` and `2` `RemoteDisconnected` errors). Every pod `/health` `media_shed` counter reconciles identically to `3,141` (`rrtrg`: `1,566`, `v58k8`: `1,565`, `n7sv6`: `10`, `hr9nr`: `0`), and cumulative `field_accepted` across the 4 pods totals `3,780` (`2` baseline [`rrtrg: 1`, `v58k8: 1`] `+ 2,337` Phase 1 `+ 1,441` Phase 2 admitted; Phase 2 sent `1,446` Field requests yielding `5` `HTTP 200`, `1,436` `HTTP 500`, `1` `HTTP 503`, `4` `RemoteDisconnected`) as downstream `aggregator` (`500m` CPU LimitRange) hit its `5 s` query timeout under 240 simultaneous `limit=500` unfiltered scatter-gather requests.
- **HPA scale-up:**
  - `hpa/client-api` rose from `1%/70%` (`2` replicas) -> `83%/70%`, scaling **`2 -> 4` pods** (`rrtrg`, `v58k8`, `hr9nr`, `n7sv6`).
  - `hpa/aggregator` rose from `67%/70%` (`3` replicas) -> `90%/70%`, scaling **`3 -> 4` pods** (`fs8z4`, `vlbnp`, `jrzrd`, `9rmrp`).
  - Both HPAs stabilized at `4` pods during the `120 s`/`180 s` scale-down stabilization windows (`1%/70%` and `50%/70%` CPU at `15:24:41Z`); HPA **scale-down** back to `minReplicas` was not captured during this run.
