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
  - Both HPAs stabilized at `4` pods during the `120 s`/`180 s` scale-down stabilization windows (`1%/70%` and `50%/70%` CPU at `15:24:41Z`).

### Phase 3: Post-Hardening QoS Shedding & Full HPA Scale-Up -> Scale-Down Cooldown ([`hpa-scale-lifecycle.log`](hpa-scale-lifecycle.log))

Recorded on **2026-11-21 (11:47–11:50 UTC)** after rolling out the Wave 1 `client-api` QoS fix (releasing `inFlight` before writing `HTTP 429` / `503` responses):

- **Saturation & QoS Shedding (`120` Field + `80` Media workers, `45 s`):**
  - Across `14,417` Media requests, `client-api` immediately shed **`13,881` Media requests with `HTTP 429 Too Many Requests`** (`6,939` on `client-api-5bc9fb97d5-k5xxh` + `6,942` on `client-api-5bc9fb97d5-xz8j7`), while admitting all `1,084` Field requests through the gateway (`496 - 1 + 122 + 469 - 2 = 1,084` `field_accepted` delta, `0` Field `429` rejections).
  - Immediately after the burst, all 3 `client-api` pods reported `"in_flight": 0` on `GET /health`.
- **Full HPA Scale-Up & Cooldown Scale-Down Lifecycle:**
  - `hpa/client-api` spiked from `1%/70%` (`2` replicas) -> `115%/70%`, scaling **`2 -> 3` replicas** (`k5xxh`, `xz8j7`, `sbl9r`).
  - After the `120 s` scale-down stabilization window elapsed (`t+147 s`, `2026-11-21T11:50:55Z`), `hpa/client-api` cleanly scaled back down **`3 -> 2` replicas** (`Deployment pods: 2 current / 2 desired`, `ReadyForNewScale`, `1%/70%` CPU) while `hpa/aggregator` remained steady at its `minReplicas: 3` baseline (`34%/70%` CPU).

### Phase 4: 10,000 Concurrent Virtual Users (`9,500` Media + `500` Field VUs, `30.01 s`, [`qos-10k-vu-load.log`](qos-10k-vu-load.log))

Recorded on **2026-11-21 (11:57–11:58 UTC)** using [`scripts/load_test_10k_qos.go`](../../../../scripts/load_test_10k_qos.go) (`make load-test-10k`) from external gateway host `mainframe` (`ulimit -n 65536`, `peak_concurrent_vus=10,000`: `9,500` `media` VUs + `500` `field` VUs hitting Traefik ingress across `aqua`/`ruby`/`akane`):

- **Gateway Priority QoS Protection (`media_shed_threshold=70`, `max_concurrent=100` per pod):**
  - Across the 2 `client-api` pods (`k5xxh` on `ruby`, `xz8j7` on `akane`), cumulative `media_shed` increased by **`27,426`** (`k5xxh`: `6,939 -> 20,646` [`+13,707`], `xz8j7`: `6,942 -> 20,661` [`+13,719`]), while `field_accepted` increased by **`1,039`** (`k5xxh`: `496 -> 1,023` [`+527`], `xz8j7`: `469 -> 981` [`+512`]) and `media_accepted` increased by **`803`** (`k5xxh`: `216 -> 594` [`+378`], `xz8j7`: `241 -> 666` [`+425`]).
  - Despite `9,500` concurrent `media` VUs generating `365,574` request attempts (`12,183.44` attempts/s, with `20,512` completed `HTTP 429` responses, `437` `HTTP 200`, and `344,625` client-timeout/socket rejections at the 10k-socket ingress layer), the `500` critical `field` VUs received **`0` `HTTP 429` rejections** and completed **`551` `HTTP 200 OK` responses** (more than `200`s than all `9,500` `media` VUs combined, alongside `212` `HTTP 503` at `max_concurrent=100` and `18,498` transport timeouts).
  - Immediately after the 10,000-VU storm ended, both `client-api` pods reported `"in_flight": 0, "status": "UP"` with `0` pod restarts across the cluster.
