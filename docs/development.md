# Local development and verification

This guide covers the runnable **Milestone 1 Docker Compose baseline**. The `demo-p1` through `demo-p5` targets use M1 problem numbering, not M2 numbering. They do not validate Kubernetes sharding, high availability, satellite ingestion, or coastal sensors.

For an overview, see the [root README](../README.md). For the documented cluster environment and M2 foundation manifests, see the [Kubernetes runbook](../k8s/README.md). Planned work is tracked in [GitHub issues](https://github.com/Darsua/AAT/issues).

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

If `.env` already exists, keep it instead of copying over it. Review the example configuration and use local-only credentials. Compose's credential fallbacks are for development, not production. Several ports bind to all host interfaces by default, so use a trusted development host/network.

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
curl --fail http://localhost:8084/health
docker compose exec -T client-api curl --fail http://aggregator:8083/health
docker compose logs --tail=50 aggregator client-api
```

The Aggregator health endpoint checks its database and broker dependencies. The other checks do not establish end-to-end readiness; PVMBG also returns 503 during its simulated outage. Allow time for the first ingestion cycle before expecting hazard data.

PostgreSQL is not published to the host. Use the internal container network for Aggregator diagnostics; do not assume that host port 8083 is reachable just because a loopback mapping appears in Compose. Running the Aggregator directly on the host requires a separately reachable database and appropriately configured upstream/broker addresses.

## Configuration

[`.env.example`](../.env.example) is the local configuration template. [Compose](../docker-compose.yml) determines which values reach each container. In Kubernetes, credentials belong in Secrets and ordinary configuration in ConfigMaps; Compose fallbacks are not a deployment secret strategy.

| Configuration group | Variables |
| :--- | :--- |
| Host ports | `CLIENT_API_PORT`, `AUTH_PORT`, `MOCK_BMKG_PORT`, `MOCK_PVMBG_PORT`, `AGGREGATOR_PORT`, `REDPANDA_PORT` |
| PostgreSQL | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `DATABASE_URL` |
| Broker and groups | `REDPANDA_BROKERS`, `KAFKA_TOPIC_HAZARDS`, `KAFKA_TOPIC_PARTITIONS`, `CONSUMER_GROUP_DASHBOARD`, `CONSUMER_GROUP_NOTIFIER` |
| Upstream polling | `BMKG_URL`, `BMKG_API_KEY`, `PVMBG_URL`, `PVMBG_TOKEN`, `POLL_INTERVAL_SECS` |
| PVMBG latency | `PVMBG_DELAY_MIN_MS`, `PVMBG_DELAY_MAX_MS` |
| Gateway and authentication | `AGGREGATOR_URL`, `JWT_SECRET`, `JWT_TTL_SECONDS`, `AUTH_MEDIA_CLIENT_ID`, `AUTH_MEDIA_CLIENT_SECRET`, `AUTH_FIELD_CLIENT_ID`, `AUTH_FIELD_CLIENT_SECRET`, `AUTH_INTERNAL_CLIENT_ID`, `AUTH_INTERNAL_CLIENT_SECRET` |

Defaults include host ports 8080–8084 and 19092, a three-second polling interval, three hazard-topic partitions, and a 60-second access-token TTL. These are **Compose development settings**, not universally available process defaults. For example, Auth and the gateway require `JWT_SECRET`, and the mocks require their upstream credentials when launched directly.

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

List filters are `type`, `severity`, `since` (UTC RFC3339), `limit` (1–500), and `offset`. The implemented baseline accepts `SEISMIC` and `VOLCANIC`; the expanded contracts also describe pending M2 types.

`fields=summary` is the default for all clients. Media tokens have `hazards:read:summary`; requesting `fields=all` with one returns 403. Field/internal tokens have `hazards:read:all`. `GET /api/v1/hazards/{id}` supports the same projection rules.

Tokens expire quickly. `POST /auth/refresh` rotates a refresh token; reuse invalidates its refresh session. Already-issued access tokens are checked locally by the gateway and remain usable until expiry. Refresh sessions are process-local and do not survive an Auth restart.

## Tests and static checks

Each service is an independent module registered in `go.work`. Use workspace-aware paths rather than assuming `go test ./...` from the root covers every module.

```bash
go vet ./services/aggregator/... ./services/auth/... ./services/client-api/... ./services/consumers/dashboard/... ./services/consumers/notifier/... ./services/mock-bmkg/... ./services/mock-pvmbg/...
make test
docker compose config --quiet
git status --short
```

`make test` includes both consumer modules. CI also vets and builds all seven services, runs Go tests with race detection, and checks documentation links/diagrams, tracked-file hygiene, YAML/JSON syntax, empty Secret templates, and targeted contract-structure regressions. It does not perform full OpenAPI/JSON Schema semantic validation or prove Kubernetes deployment/recovery.

Run the repository checks locally with Python 3 and the existing CI parser dependency, `PyYAML==6.0.3`:

```bash
python3 -m unittest discover -s scripts -p 'test_*.py' -v
python3 scripts/check_repository.py docs
python3 scripts/check_repository.py configs
python3 scripts/check_repository.py hygiene
```

The hygiene guard checks tracked filenames and common token/private-key patterns; it is not comprehensive secret detection. Inspect changes for accidental credentials, `.env` additions, and arbitrary vendor tokens before sharing them. Secret templates must contain empty values; provision real credentials separately. These checks do not substitute for integration or failure scenarios.

## M1 demonstrations

Start with a healthy local stack. These scripts **change running state**: they may toggle schema/outage flags, restart services, inject broker events, or create temporary consumer groups. Use a disposable development stack, not a shared or operational deployment.

| Target | Checks | Recorded M1 evidence |
| :--- | :--- | :--- |
| `make demo-p1` | Additive PVMBG `confidence_level` ingestion | [Schema evolution](evidence/m1/p1/README.md) |
| `make demo-p2` | PVMBG outage, retained data, and degraded metadata | [Outage handling](evidence/m1/p2/README.md) |
| `make demo-p3` | Cross-credential rejection, scopes, expiry, and refresh rotation | [Authentication](evidence/m1/p3/README.md) |
| `make demo-p4` | Mixed JSONB attributes and recovery after container restarts | [Storage and restarts](evidence/m1/p4/README.md) |
| `make demo-p5` | Consumer-group independence, offset recovery, and a third group | [Pub-sub](evidence/m1/p5/README.md) |
| `make test-observability` | Correlation IDs, HTTP latency/status logs, Kafka headers, and group lag | See [the script](../scripts/test_observability.sh) |

Use the host environment setup above for the authentication scripts. `SKIP_TTL_WAIT=1` skips the natural-expiration wait for quick iteration; it does **not** prove expiry behavior. The restart script uses fixed default ports and database identifiers, and some historical scripts retain development credential fallbacks. Inspect scripts before using a customized stack; not every setting is overridable.

The observability script checks HTTP and Kafka paths separately, with a directly injected test message. It does not demonstrate an end-to-end ingestion/outbox trace or the full M2 metrics/tracing requirements. Restart checks establish post-recovery behavior, not uninterrupted availability during a single Aggregator/database outage.

### M1 load test

The existing k6 test uses 25 seismic-query VUs and 25 volcanic-query VUs for 60 seconds, plus five direct PVMBG VUs when `PVMBG_TOKEN` is set. It renews access tokens and checks gateway p95 below 300 ms and error rate below 1%, excluding reported HTTP 429 rejections.

With the local credentials exported as above:

```bash
PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 docker compose up -d mock-pvmbg
make load-test
```

The Docker k6 fallback uses host networking and default host endpoints; it does not forward every URL override. For custom gateway ports, run local k6 with `TARGET_URL` set appropriately. Restore the normal delay afterward:

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
  --token "${AUDIT_TOKEN:-audit-secret-token-2026}" \
  --json
```

See [docker-compose.sharded.yml](../docker-compose.sharded.yml), [the Kubernetes runbook](../k8s/README.md), [ADR 006](adr/006-sharded-storage-consistent-hashing-and-audit-api.md), and [ADR 007](adr/007-aggregator-ha-leader-leasing-and-fencing.md) for topology details and recorded benchmark measurements.

## Reports and architecture documentation

`make report` builds the **M1** report with `tectonic`, producing `docs/report/m1/main.pdf` and `docs/report/m1/IF4031_M1_AhhAldoyTolong.pdf`. Tectonic may download TeX dependencies. See the [report guide](report/README.md). M2 will also use LaTeX; its folders are reserved, with no manuscript or build target yet.

Architecture decisions are in [docs/adr](adr/). Their cited [historical measurements](evidence/m1/adr-audit/README.md) retain methodology and limitations, not an active audit backlog. Diagram sources live in [docs/diagrams](diagrams/); update canonical `.mmd` sources together with their embedded Markdown diagrams.
