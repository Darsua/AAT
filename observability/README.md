# Unified Observability (#35)

This directory contains configuration, provisioning manifests, and dashboard definitions for the BNPB Early Warning Platform's unified observability stack comprising **Prometheus** (metrics), **Grafana Tempo** (distributed tracing), and **Grafana** (visualization).

---

## 1. Architecture & Telemetry Pillars

The observability implementation uses pure standard-library Go telemetry (`telemetry.go` across all services) without heavy external agent dependencies:

1. **Metrics (Prometheus):**
   - Every service registers an in-memory stdlib metrics registry and exposes Prometheus-compatible metrics on `GET /metrics`.
   - Prometheus is configured via [prometheus/prometheus.yml](prometheus/prometheus.yml) to scrape all services every 5s.

2. **Distributed Tracing (Grafana Tempo):**
   - Traces are propagated across process boundaries via W3C `traceparent` headers over HTTP and Kafka record headers (`outbox_relay.go` -> consumers).
   - Trace spans are exported asynchronously to Grafana Tempo via OTLP/HTTP JSON (`POST /v1/traces` on `:4318`).

3. **Structured Logging (`slog`):**
   - High-throughput JSON logging with standard fields (`time`, `level`, `msg`, `service`, `correlation_id`, `trace_id`, `span_id`).

---

## 2. Monitored Services & Target Registry

| Service | Component | Scrape Target | Metrics Endpoint | Tracing Export |
|---|---|---|---|---|
| `client-api` | Public Ingress API | `client-api:8080` | `GET /metrics` | OTLP &rarr; Tempo |
| `auth` | OAuth2 Token Provider | `auth:8084` | `GET /metrics` | OTLP &rarr; Tempo |
| `aggregator` | Core Ingestion & Storage | `aggregator:8083` | `GET /metrics` | OTLP &rarr; Tempo |
| `dashboard-consumer` | Analytics Consumer | `dashboard-consumer:8085` | `GET /metrics` | OTLP &rarr; Tempo |
| `notifier-consumer` | Alert Engine Consumer | `notifier-consumer:8086` | `GET /metrics` | OTLP &rarr; Tempo |
| `summarizer-consumer` | AI Situation Summarizer | `summarizer-consumer:8087` | `GET /metrics` | OTLP &rarr; Tempo |
| `mock-bmkg` | Upstream Seismic Provider | `mock-bmkg:8081` | `GET /metrics` | OTLP &rarr; Tempo |
| `mock-pvmbg` | Upstream Volcanic Provider | `mock-pvmbg:8082` | `GET /metrics` | OTLP &rarr; Tempo |
| `redpanda` | Event Broker | `redpanda:9644` | `GET /public_metrics` | Broker Metrics |

---

## 3. Provisioning & Grafana Dashboard

- **Datasources:** [grafana/provisioning/datasources/datasources.yml](grafana/provisioning/datasources/datasources.yml) provisions Prometheus (`http://prometheus:9090`, UID: `prometheus`) and Tempo (`http://tempo:3200`, UID: `tempo`) with Trace-to-Metrics / Trace-to-Logs correlation.
- **Dashboards:** [grafana/provisioning/dashboards/dashboards.yml](grafana/provisioning/dashboards/dashboards.yml) auto-loads the pre-built unified operations dashboard [grafana/dashboards/bnpb-unified-observability.json](grafana/dashboards/bnpb-unified-observability.json).

---

## 4. Kubernetes Observability Wiring Plan (3-Node k3s Cluster)

To cleanly respect workstream boundaries (`Member C` ownership of `k8s/base/`), the Kubernetes manifests and NetworkPolicy rules for observability will follow in a dedicated follow-up pull request:

1. **Prometheus Deployment:**
   - Deploy Prometheus in the `bnpb` namespace with a ConfigMap mounted at `/etc/prometheus/prometheus.yml`.
   - Pod discovery configured via Kubernetes service discovery (`kubernetes_sd_configs: [role: pod]`) or static headless service endpoints across the cluster nodes.
2. **Tempo Deployment:**
   - Single-pod StatefulSet / Deployment running `grafana/tempo:2.6.0` with ephemeral or hostPath storage for trace ingestion.
3. **Grafana Deployment:**
   - ConfigMap volume projections for `datasources.yml` and `bnpb-unified-observability.json`.
4. **Network Policies (`k8s/base/network-policies.yaml`):**
   - Add ingress rules permitting traffic from the `prometheus` pod selector to ports `8080`–`8087` on all application pods.
   - Add egress rules permitting application pods to send OTLP/HTTP payloads to `tempo:4318`.
