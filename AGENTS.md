# Repository rules

This is the BNPB Disaster Coordination Platform for IF4031. The runnable application is the **M1 baseline**; M2 work is in progress. Contracts and Kubernetes base resources do not imply that M2 features are implemented.

## Service boundaries and contracts

- Each microservice is an independent Go module. Do not create shared `pkg/`, `shared/`, or `common/` application libraries, or import implementation packages across services. Duplicated transport structs are intentional.
- The Aggregator exclusively owns canonical PostgreSQL storage, including future shards. Other application services must use its HTTP API or consume broker events, never connect to canonical storage. Enforce ownership through platform networking as well as code. A service needing its own durable state must declare its separate ownership and access policy.
- **All files in `contracts/` are frozen**, including Audit API and coastal-sensor specifications. Do not change fields, endpoints, or types without explicit approval. Record discrepancies for resolution; do not silently adjust implementation requirements to fit existing code.
- Preserve the Kafka contract: key = `hazard_id`; value = canonical HazardEvent JSON, without a wrapper; propagate `X-Correlation-ID`. Changing partition count or delivery behavior requires an explicit ordering/recovery assessment.

## Ownership and change scope

GitHub issues are the work tracker. An issue's approved scope defines cross-service changes; default ownership is not permission for unrelated edits.

| Member | Default ownership | M2 workstreams |
| :--- | :--- | :--- |
| Aldoy | Agency mocks | Kubernetes foundation, DLQ/schema handling, satellite ingestion, autoscaling |
| Darrel | Aggregator, canonical storage and migrations | Sharding/recovery, HA coordination, Audit API, optional AI summarizer |
| Jenka | Auth, public API, consumers | Analytics, field-report submission, coastal integration, unified observability |

Several M2 workstreams touch the Aggregator. Coordinate overlapping edits with its owner and identify the files/interfaces in the issue or PR; do not interpret the M1 service split as a ban on explicitly assigned M2 work. Documentation and orchestration are cross-cutting. Do not create branches, commits, releases, or tags without being asked.

## Implementation conventions

- Go 1.22 workspace (`go.work`); standard library first. Existing approved libraries: `pgx/v5`, `segmentio/kafka-go`, and `golang-jwt/jwt/v5`. Additional dependencies require justification and approval; illustrative ADR examples do not authorize them. Run `go work sync` when dependencies change.
- Keep additive PVMBG fields in dynamic `attributes`/JSONB. Breaking schema changes require explicit validation/failure handling; do not silently discard rejected payloads or claim all upstream adapters preserve unknown fields.
- Use UTC RFC3339 timestamps. Preserve correlation IDs across HTTP and Kafka; correlation IDs alone are not distributed traces.
- Application Dockerfiles use multi-stage `golang:1.22-alpine` / `alpine:3.20` builds and run as `nobody:nobody`. This does not authorize modifying assistant-provided sensor/tester images.
- Read credentials from environment/Secrets; never hardcode or print tokens. Keep `.env` and real kubeconfigs out of version control. Document new configuration in `.env.example`, applicable Compose wiring, and Kubernetes ConfigMap/Secret templates. Use empty/required placeholders for secrets, not usable credential defaults.

## M2 deployment and safety

- Application workloads belong to `sistem-informasi-kebencanaan-bnpb`, with the required `role` labels. Follow the [Kubernetes runbook](k8s/README.md) for component memory allocations and the pinned Tester Agent.
- Demo nodes must have separate kernels. Store data must use node-local disks, not external/network storage that bypasses wipe tests. In a two-node setup, destructive node scenarios target only the non-control-plane worker.
- Never block, replace, stop, or constrain the Tester Agent contrary to the course contract. Treat chaos tests as destructive; do not run them, change a live cluster, or restore snapshots during an assessed recovery window without the appropriate explicit instruction.
- A complete, one-command M2 deployment is still pending. Do not present `kubectl apply -k k8s/base` as deployment of the application.

## Documentation discipline

- Root README: visitor-facing overview and honest implementation status.
- `docs/development.md`: local setup and verification; service READMEs: service-specific essentials; `k8s/README.md`: cluster operations.
- Contracts own API details; ADRs own decisions/trade-offs; issues own pending work and acceptance criteria. Link instead of copying schemas, ownership matrices, or task lists across documents.
- Evidence is dated and milestone-specific, not current certification. Do not turn M1 logs into M2 proof or fabricate measurements. Keep report files unchanged until the team agrees on the M2 authoring workflow.
- No new roadmap, tracker, or session-audit Markdown unless explicitly requested. Keep canonical diagram sources consistent with any embedded blocks; historical rendered/report figures are separate artifacts.

## Verification

Run the applicable checks, report failures/skips honestly, and inspect changes for credentials:

```bash
go vet ./services/aggregator/... ./services/auth/... ./services/client-api/... ./services/consumers/dashboard/... ./services/consumers/notifier/... ./services/mock-bmkg/... ./services/mock-pvmbg/...
make test
docker compose config --quiet
git diff --check
git status --short
```

Also validate changed documentation links/diagram synchronization. These checks do not prove M2 deployment, load capacity, or failure recovery; those require the relevant integration evidence. Do not run destructive demo scripts merely to validate documentation changes.
