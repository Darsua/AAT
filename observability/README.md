# Unified Observability (#35)

This directory contains configuration, provisioning manifests, and dashboard definitions for the BNPB Early Warning Platform's unified observability stack comprising **Prometheus** (metrics), **Grafana Tempo** (distributed tracing), and **Grafana** (visualization).

---

## 1. Architecture & Telemetry Pillars

The observability implementation uses pure standard-library Go telemetry (`telemetry.go` across 7 of the 8 Go services, excluding `summarizer-consumer`) without heavy external agent dependencies:

1. **Metrics (Prometheus):**
   - Every service registers an in-memory stdlib metrics registry and exposes Prometheus-compatible metrics on `GET /metrics` (`summarizer-consumer` exposes `bnpb_summarizer_*` metrics on `GET /metrics`).
   - Prometheus is configured via [prometheus/prometheus.yml](prometheus/prometheus.yml) to scrape all services every 5s.

2. **Distributed Tracing (Grafana Tempo):**
   - Traces are propagated across HTTP and Kafka (`outbox_relay.go`, `dashboard`, `notifier`) via W3C `traceparent` headers.
   - Trace spans are exported asynchronously from those 7 services to Grafana Tempo via OTLP/HTTP JSON (`POST /v1/traces` on `:4318`). `summarizer-consumer` does not export OTLP traces.

3. **Structured Logging (`slog`):**
   - The 7 instrumented services emit JSON logging with standard fields (`time`, `level`, `msg`, `service`, `correlation_id`, `trace_id`, `span_id`), while `summarizer-consumer` logs plain `log.Printf` lines.

---

## 2. Monitored Services & Target Registry

| Service | Component | Scrape Target | Metrics Endpoint | Tracing Export |
|---|---|---|---|---|
| `client-api` | Public Ingress API | `client-api:8080` | `GET /metrics` | OTLP &rarr; Tempo |
| `auth` | OAuth2 Token Provider | `auth:8084` | `GET /metrics` | OTLP &rarr; Tempo |
| `aggregator` | Core Ingestion & Storage | `aggregator:8083` | `GET /metrics` | OTLP &rarr; Tempo |
| `dashboard-consumer` | Analytics Consumer | `dashboard-consumer:8085` | `GET /metrics` | OTLP &rarr; Tempo |
| `notifier-consumer` | Alert Engine Consumer | `notifier-consumer:8086` | `GET /metrics` | OTLP &rarr; Tempo |
| `summarizer-consumer` | AI Situation Summarizer | `summarizer-consumer:8087` | `GET /metrics` | Metrics only (no OTLP) |
| `mock-bmkg` | Upstream Seismic Provider | `mock-bmkg:8081` | `GET /metrics` | OTLP &rarr; Tempo |
| `mock-pvmbg` | Upstream Volcanic Provider | `mock-pvmbg:8082` | `GET /metrics` | OTLP &rarr; Tempo |
| `redpanda` | Event Broker | `redpanda:9644` | `GET /public_metrics` | Broker Metrics |

---

## 3. Provisioning & Grafana Dashboards

- **Datasources:** [grafana/provisioning/datasources/datasources.yml](grafana/provisioning/datasources/datasources.yml) provisions Prometheus (`http://prometheus:9090`, UID: `Prometheus`) and Tempo (`http://tempo:3200`, UID: `Tempo`).
- **Dashboards:** [grafana/provisioning/dashboards/dashboards.yml](grafana/provisioning/dashboards/dashboards.yml) auto-loads three interconnected operations dashboards into the `BNPB` folder:
  1. [grafana/dashboards/bnpb-unified-observability.json](grafana/dashboards/bnpb-unified-observability.json) (`uid: bnpb-unified-obs`): **Platform Overview & API Gateway** (service replica health badges, HTTP RPS/latency/errors, OAuth2 token issuance/failures, Priority QoS 429 load shedding, and per-replica HA request balancing).
  2. [grafana/dashboards/bnpb-event-pipeline.json](grafana/dashboards/bnpb-event-pipeline.json) (`uid: bnpb-event-pipeline`): **Event Pipeline, Redpanda & Consumers** (transactional outbox relay, consumer throughput/p95 latency/deduplication skips, Redpanda partition lag and topic throughput, broker memory/disk metrics, and AI Situation Summarizer LLM vs fallback generations, latency, and circuit status).
  3. [grafana/dashboards/bnpb-satellite-ingestion.json](grafana/dashboards/bnpb-satellite-ingestion.json) (`uid: bnpb-satellite-ingestion`): **Satellite Wildfire & Upstream Ingestion** (active streaming leader election, live feed lag, offsets, p95 ingress-to-persist latency, wildfire lifecycle dispositions, fenced checkpoint payload size, and BMKG/PVMBG polling).

---

## 4. Kubernetes Observability Wiring (`k8s/base/observability.yaml`)

Prometheus, Tempo, and Grafana run in both Docker Compose ([`docker-compose.yml`](../docker-compose.yml)) and the 3-node k3s cluster ([`k8s/base/observability.yaml`](../k8s/base/observability.yaml)):

1. **Prometheus Deployment (`prometheus:9090`, `role: other`, `128Mi`/`256Mi`):**
   - Deployed in `sistem-informasi-kebencanaan-bnpb` with `bnpb-observability-config` mounted at `/etc/prometheus/prometheus.yml`.
   - Uses DNS A-record service discovery (`dns_sd_configs`) across headless Services (`aggregator-headless`, `client-api-headless`, `dashboard-consumer-headless`, `notifier-consumer-headless`) so every replica pod is scraped individually without requiring cluster-wide RBAC, alongside static scrape targets for single-replica services (`auth:8084`, `summarizer-consumer:8087`, `mock-bmkg:8081`, `mock-pvmbg:8082`, `redpanda:9644`).
2. **Tempo Deployment (`tempo:3200` / `:4318`, `role: other`, `128Mi`/`256Mi`):**
   - Runs `grafana/tempo:2.6.0` receiving OTLP/HTTP spans from the 7 instrumented services via `OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4318` in [`k8s/base/configmaps.yaml`](../k8s/base/configmaps.yaml).
3. **Grafana Deployment (`grafana:3000`, `role: other`, `128Mi`/`256Mi`):**
   - Mounts `bnpb-observability-config` (`datasources.yml`, `dashboards.yml`) and `bnpb-grafana-dashboards` (all 3 JSON dashboards), exposed at `/grafana` on `aat.darsu.dev` via [`k8s/base/ingress.yaml`](../k8s/base/ingress.yaml) with anonymous access disabled (`GF_AUTH_ANONYMOUS_ENABLED=false`) and admin credentials sourced from `Secret/bnpb-credentials` (`GF_SECURITY_ADMIN_PASSWORD`).
4. **Network Policies ([`k8s/base/network-policies.yaml`](../k8s/base/network-policies.yaml)):**
   - Ingress rules permit `role: other` across application ports `8080–8087`, `3000`, `3200`, `9090`, `4318`, and `redpanda:9644`.
