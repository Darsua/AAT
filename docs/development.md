# Local development and verification

This guide covers the runnable Docker Compose stacks (`docker-compose.yml` for single-node local development with M2 extensions, and `docker-compose.sharded.yml` for the 3-node sharded + 3-replica HA rehearsal). The `demo-p1` through `demo-p5` targets use M1 problem numbering, not M2 numbering. They do not validate Kubernetes multi-node isolation, satellite ingestion, or chaos failover.

For an overview, see the [root README](../README.md). For the documented cluster environment and M2 Kubernetes deployment, see the [Kubernetes runbook](../k8s/README.md). Planned work is tracked in [GitHub issues](https://github.com/Darsua/AAT/issues).

## Prerequisites

- Docker Engine and Docker Compose v2; the original local runbook targeted Engine 24+ and Compose 2.20+.
- Go 1.22+ for tests, vetting, and running services outside containers.
- GNU Make and Bash for Makefile targets and verification scripts.
- `curl` and `jq` for API examples and demonstration scripts.
- Optional: `k6` for local load tests and `tectonic` for the M1 report. The load-test Make target can use a Docker fallback when `k6` is unavailable.

## Start and stop

Run from the repository root:

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

If `.env` already exists, keep it instead of copying over it. `.env.example` and `docker-compose.yml` use empty secret placeholders (`${VAR:-}`), so populate local credentials in `.env` before running Compose or verification scripts. Several ports bind to all host interfaces by default, so use a trusted development host/network.

```bash
# Stop containers while preserving stored hazards and broker state.
docker compose down

# Destructive reset: delete the database and broker volumes too.
docker compose down -v
```

`make up`, `make build`, and `make logs` wrap Compose. **`make down` runs `down -v`**, not a data-preserving shutdown. `make logs` follows output until interrupted.

### Health and logs

With default host ports:

```bash
curl --fail http://localhost:8080/health
curl --fail http://localhost:8081/health
curl --fail http://localhost:8082/health
curl --fail http://localhost:8083/readyz
curl --fail http://localhost:8084/health
curl --fail http://localhost:8085/health
curl --fail http://localhost:8087/health
docker compose logs --tail=50 aggregator client-api
```

On Aggregator, `GET /readyz` checks draining state and canonical DB reachability (decoupled from single-broker outages), whereas `GET /health` checks draining, DB, and broker connectivity together; `GET /livez` reports process liveness. PVMBG also returns `503` during its simulated outage. Allow time for the first ingestion cycle before expecting hazard data.

PostgreSQL is not published to the host on the base stack. Loopback ports `127.0.0.1:8083` (`aggregator`) and `127.0.0.1:8087` (`summarizer-consumer`) are bound to host loopback via `bnpb-public` so local Audit/summarizer tools can query them cleanly. Running the Aggregator directly on the host requires a separately reachable database and appropriately configured upstream/broker addresses.

## Configuration

[`.env.example`](../.env.example) is the local configuration template. [Compose](../docker-compose.yml) determines which values reach each container. In Kubernetes, credentials belong in Secrets (via scoped `secretKeyRef`) and ordinary configuration in ConfigMaps.

| Configuration group | Variables |
| :--- | :--- |
| Host ports | `CLIENT_API_PORT`, `AUTH_PORT`, `MOCK_BMKG_PORT`, `MOCK_PVMBG_PORT`, `AGGREGATOR_PORT`, `DASHBOARD_PORT`, `SUMMARIZER_PORT`, `REDPANDA_PORT`, `STORE_AQUA_PORT`, `STORE_RUBY_PORT`, `STORE_AKANE_PORT`, `AGGREGATOR_2_PORT`, `AGGREGATOR_3_PORT`, `PROMETHEUS_PORT`, `TEMPO_PORT`, `GRAFANA_PORT` |
| PostgreSQL & Sharding | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `DATABASE_URL`, `NODE_NAME`, `STORAGE_UNIT_ID`, `SHARD_CONFIG`, `SHARD_TOPOLOGY_JSON` |
| Broker, Topics & Consumers | `REDPANDA_BROKERS`, `KAFKA_TOPIC_HAZARDS`, `REDPANDA_TOPIC_FIELD_REPORTS`, `KAFKA_TOPIC_PARTITIONS`, `CONSUMER_GROUP_FIELD_REPORTS`, `CONSUMER_GROUP_DASHBOARD`, `CONSUMER_GROUP_NOTIFIER`, `CONSUMER_GROUP_SUMMARIZER`, `NOTIFIER_RECEIPT_LOG`, `FIELD_TEAM_RECEIVER_URL`, `DASHBOARD_SYNC_INTERVAL_SECS`, `DASHBOARD_FULL_SYNC_INTERVAL_SECS` |
| Upstream Polling & Satellite | `BMKG_URL`, `BMKG_API_KEY`, `PVMBG_URL`, `PVMBG_TOKEN`, `SENSOR_PANTAI_URL`, `PANTAI_AUTH_TOKEN`, `POLL_INTERVAL_SECS`, `COASTAL_POLL_INTERVAL_SECS`, `COASTAL_TIMEOUT_MS`, `COASTAL_FRESHNESS_TTL_SECS`, `SATELLITE_ENABLED`, `SATELLITE_URL`, `SATELLITE_GROUP_TOKEN`, `SATELLITE_LATENESS_MINUTES`, `SATELLITE_STATE_RETENTION_HOURS`, `SATELLITE_PROVINCE_CENTROIDS_FILE` |
| PVMBG Latency, Schema & HA | `PVMBG_DELAY_MIN_MS`, `PVMBG_DELAY_MAX_MS`, `PVMBG_FIELD_ALIASES`, `PVMBG_COERCE_NUMERIC_STRINGS`, `DLQ_RETRY_BUDGET`, `INSTANCE_ID`, `LEASE_TTL_SECS`, `LEASE_RENEW_INTERVAL_SECS`, `DRAIN_DELAY_MS` |
| Auth, QoS & Audit | `AGGREGATOR_URL`, `AUDIT_TOKEN`, `AUDIT_ALL_SOURCES_LABEL`, `JWT_SECRET`, `JWT_TTL_SECONDS`, `QOS_MAX_CONCURRENT`, `QOS_MEDIA_SHED_THRESHOLD`, `AUTH_MEDIA_CLIENT_*`, `AUTH_FIELD_CLIENT_*`, `AUTH_INTERNAL_CLIENT_*` |
| AI Summarizer (`#36`) | `AI_API_URL`, `AI_MODEL`, `AI_API_KEY`, `AI_DISABLE_THINKING`, `AI_TIMEOUT_MS`, `AI_MAX_TOKENS`, `AI_MAX_CALLS_PER_MIN`, `SUMMARY_MIN_INTERVAL_MS`, `AI_CB_COOLDOWN_SECS` |
| Observability | `OTEL_EXPORTER_OTLP_ENDPOINT`, `GF_SECURITY_ADMIN_USER`, `GF_SECURITY_ADMIN_PASSWORD` |

Defaults include non-secret host ports (`8080`–`8085`, `8087`, `19092`), a 3-second polling interval, 3 hazard-topic partitions, and a 60-second access-token TTL. Secret values (`POSTGRES_PASSWORD`, `JWT_SECRET`, `AUDIT_TOKEN`, upstream tokens/secrets) have **no usable fallback defaults** and must be supplied in `.env` or the process environment.

`POSTGRES_*` settings initialize a fresh database volume; changing them does not rotate credentials in an existing volume. Keep `DATABASE_URL` consistent with the actual database credentials. Aggregator receives the BMKG credential as `BMKG_KEY`, mapped from Compose's `BMKG_API_KEY`. The hazard topic is passed to the Go services as `TOPIC`.

Container URLs such as `http://auth:8084` and `http://mock-pvmbg:8082` are not host-facing URLs. Host scripts need `localhost` addresses (or your overridden host ports).

## Query the API

The following examples use **Bash**, `curl`, and `jq`. Source only a trusted, locally reviewed `.env` file: sourcing it executes shell code. Values come from your environment rather than literal credentials in this guide.

```bash
set -a
source .env
set +a

# Host-facing URLs for these examples and the scripts below.
export AUTH_URL="http://localhost:${AUTH_PORT:-8084}"
export CLIENT_API_URL="http://localhost:${CLIENT_API_PORT:-8080}"
export BMKG_URL="http://localhost:${MOCK_BMKG_PORT:-8081}"
export PVMBG_URL="http://localhost:${MOCK_PVMBG_PORT:-8082}"
export BMKG_KEY="${BMKG_API_KEY}"
```

Authenticate using the field client's configured identity:

```bash
FIELD_SESSION=$(jq -n \
  --arg id "${AUTH_FIELD_CLIENT_ID:-test-field-client}" \
  --arg secret "${AUTH_FIELD_CLIENT_SECRET:?Set AUTH_FIELD_CLIENT_SECRET}" \
  '{client_id: $id, client_secret: $secret, requested_scope: "field"}' \
  | curl --fail-with-body --silent --show-error \
      -H 'Content-Type: application/json' --data-binary @- \
      "${AUTH_URL}/auth/token")
FIELD_TOKEN=$(printf '%s' "${FIELD_SESSION}" | jq -er '.access_token')

curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer ${FIELD_TOKEN}" \
  -H 'X-Correlation-ID: local-hazard-query' \
  "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=10" | jq .
```

List filters are `type`, `severity`, `since` (UTC RFC3339), `limit` (1–500), and `offset`. Accepted hazard types are `SEISMIC`, `VOLCANIC`, `WILDFIRE`, and `COASTAL`.

`fields=summary` is the default for all clients. Media tokens have `hazards:read:summary`; requesting `fields=all` with one returns 403. Field/internal tokens have `hazards:read:all`. `GET /api/v1/hazards/{id}` supports the same projection rules.

Tokens expire quickly. `POST /auth/refresh` rotates a refresh token; reuse invalidates its refresh session. Already-issued access tokens are checked locally by the gateway and remain usable until expiry. Refresh sessions are process-local and do not survive an Auth restart.

## Tests and static checks

Each service is an independent module registered in `go.work`. Use workspace-aware paths rather than assuming `go test ./...` from the root covers every module.

```bash
go vet ./services/aggregator/... ./services/auth/... ./services/client-api/... ./services/consumers/dashboard/... ./services/consumers/notifier/... ./services/consumers/summarizer/... ./services/mock-bmkg/... ./services/mock-pvmbg/...
make test
docker compose config --quiet
docker compose -f docker-compose.yml -f docker-compose.sharded.yml config --quiet
git status --short
```

`make test` includes all eight Go modules. CI also vets and builds all eight services, runs Go tests with race detection (`-race`), checks shell script syntax (`bash -n`), and runs repository checks (`python3 -B`) for links/diagrams, tracked-file hygiene, YAML/JSON syntax, empty Secrets, and contract-structure regressions. It does not perform full OpenAPI/JSON Schema semantic validation or prove Kubernetes deployment/recovery.

Run the repository checks locally with Python 3 (`-B` avoids writing `__pycache__` bytecode) and the existing CI parser dependency, `PyYAML==6.0.3`:

```bash
python3 -B -m unittest discover -s scripts -p 'test_*.py' -v
python3 -B scripts/check_repository.py docs
python3 -B scripts/check_repository.py configs
python3 -B scripts/check_repository.py hygiene
```

The hygiene guard checks tracked filenames and common token/private-key patterns; it is not comprehensive secret detection. Inspect changes for accidental credentials, `.env` additions, and arbitrary vendor tokens before sharing them. Secret templates must contain empty values; provision real credentials separately. These checks do not substitute for integration or failure scenarios.

## M1 demonstrations

Start with a healthy local stack. These scripts **change running state**: they may toggle schema/outage flags, restart services, inject broker events, or create temporary consumer groups. Use a disposable development stack, not a shared or operational deployment.

| Target | Checks | Recorded M1 evidence |
| :--- | :--- | :--- |
| `make demo-p1` | Additive PVMBG `confidence_level` ingestion | [Schema evolution](evidence/m1/p1/README.md) |
| `make demo-p2` | PVMBG outage, retained data, and degraded metadata | [Outage handling](evidence/m1/p2/README.md) |
| `make demo-p3` | Cross-credential rejection, scopes, expiry, and refresh rotation | [Authentication](evidence/m1/p3/README.md) (archived log skipped natural TTL wait) |
| `make demo-p4` | Mixed JSONB attributes and recovery after container restarts | [Storage and restarts](evidence/m1/p4/README.md) |
| `make demo-p5` | Consumer-group independence, offset recovery, and a third group | [Pub-sub](evidence/m1/p5/README.md) |
| `make test-observability` | Correlation IDs, HTTP latency/status logs, Kafka headers, and group lag | See [the script](../scripts/test_observability.sh) |

Use the host environment setup above (`source .env`) for all demonstration scripts; scripts fail fast when required tokens or client secrets are unset (no hardcoded credential defaults). `SKIP_TTL_WAIT=1` skips the natural-expiration wait for quick iteration; it does **not** prove expiry behavior. Inspect scripts before using a customized stack; `make demo-p4` targets base Compose service names rather than `up-sharded`.

The observability script checks HTTP and Kafka paths separately, with a directly injected test message. It does not demonstrate an end-to-end ingestion/outbox trace or the full M2 metrics/tracing requirements. Restart checks establish post-recovery behavior, not uninterrupted availability during a single Aggregator/database outage.

### M1 load test

The existing k6 test uses 25 seismic-query VUs and 25 volcanic-query VUs for 60 seconds, plus five direct PVMBG VUs when `PVMBG_TOKEN` is set. It renews access tokens and checks gateway p95 below 300 ms and error rate below 1%, excluding reported HTTP 429 rejections.

With the local credentials exported as above:

```bash
PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 docker compose up -d mock-pvmbg
make load-test
```

The Docker k6 fallback forwards `TARGET_URL`, `AUTH_URL`, `PVMBG_URL`, `PVMBG_TOKEN`, and internal client credentials over host networking. For custom gateway ports, set `TARGET_URL` and `AUTH_URL` before running `make load-test`. Restore the normal delay afterward:

```bash
PVMBG_DELAY_MIN_MS=500 PVMBG_DELAY_MAX_MS=3000 docker compose up -d mock-pvmbg
```

This is **not** the M2 sustained 10,000-VU/scaling test. Historical measurements are evidence of a particular run, not a guarantee for a new environment.

## M2 sharded, HA, and Kubernetes workflows

Beyond the M1 single-node Compose stack, local M2 tooling covers 3-node database sharding (`#23`), Internal Audit API verification (`#30`), HA Aggregator leasing/fencing (`#25`), and full Kubernetes manifest deployment (`#22`):

```bash
# Validate sharded Compose overlay and launch 3 Postgres stores + 3 HA aggregators
make up-sharded
make down-sharded

# Validate rendered Kubernetes workloads and one-command deployment script
make k8s-validate
make k8s-deploy

# Run historical catalog loading + regional spike skew comparison (JSON report)
make catalog-bench

# Run cross-language Go/Python SHA-256 checksum & Audit API verification
make audit-verify
```

To verify a live running Aggregator endpoint directly against an exported or external catalog JSON file:

```bash
python3 scripts/verify_audit_and_catalog.py \
  --url http://localhost:8083 \
  --token "${AUDIT_TOKEN:?Set AUDIT_TOKEN}" \
  --json
```

See [docker-compose.sharded.yml](../docker-compose.sharded.yml), [the Kubernetes runbook](../k8s/README.md), [ADR 006](adr/006-sharded-storage-consistent-hashing-and-audit-api.md), [ADR 007](adr/007-aggregator-ha-leader-leasing-and-fencing.md), and [ADR 008](adr/008-non-blocking-ai-situation-summarizer.md) for topology details and recorded benchmark measurements.

## Reports and architecture documentation

`make report` builds the **M1** report with `tectonic`, producing `docs/report/m1/main.pdf` and `docs/report/m1/IF4031_M1_AhhAldoyTolong.pdf`. Tectonic may download TeX dependencies. See the [report guide](report/README.md). M2 will also use LaTeX; its folders are reserved, with no manuscript or build target yet.

Architecture decisions are in [docs/adr](adr/). Their cited [historical measurements](evidence/m1/adr-audit/README.md) retain methodology and limitations, not an active audit backlog. Diagram sources live in [docs/diagrams](diagrams/); update canonical `.mmd` sources together with their embedded Markdown diagrams.
