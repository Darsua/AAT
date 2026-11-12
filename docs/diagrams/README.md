# Diagram sources

The `.mmd` files are canonical [Mermaid](https://mermaid.js.org/) sources.
They describe the M1 runtime or explicitly labeled provisional M2 requirements;
a diagram does not establish that planned behavior is implemented.

## Architecture and ADR embeddings

These Markdown documents embed the corresponding source verbatim:

| Source | Embedded in |
| --- | --- |
| [architecture.en.mmd](architecture.en.mmd) | [Root README](../../README.md): current architecture block matches exactly. |
| [adr002-fanout.mmd](adr002-fanout.mmd) | [ADR 002](../adr/002-message-broker-redpanda.md) |
| [adr003-outbox.mmd](adr003-outbox.mmd) | [ADR 003](../adr/003-canonical-storage-postgres-jsonb.md) |
| [adr004-sequence.mmd](adr004-sequence.mmd) | [ADR 004](../adr/004-internal-communication-rest.md) |
| [adr005-scaleout.mmd](adr005-scaleout.mmd), [adr005-breaker.mmd](adr005-breaker.mmd) | [ADR 005](../adr/005-stateless-architecture-and-m2-readiness.md): pending scale-out requirements and an optional breaker concept, not selected implementations. |

## Service guide references

Service guides **link to these sources**, rather than embedding diagram blocks:

| Source | Service guide |
| --- | --- |
| [aggregator-flow.mmd](aggregator-flow.mmd) | [Aggregator](../../services/aggregator/README.md) |
| [auth-flow.mmd](auth-flow.mmd) | [Auth](../../services/auth/README.md) |
| [clientapi-gateway.mmd](clientapi-gateway.mmd) | [Client API](../../services/client-api/README.md) |
| [consumers-fanout.mmd](consumers-fanout.mmd) | [Consumers](../../services/consumers/README.md) |
| [mock-bmkg-context.mmd](mock-bmkg-context.mmd) | [Mock BMKG](../../services/mock-bmkg/README.md) |
| [mock-pvmbg-context.mmd](mock-pvmbg-context.mmd) | [Mock PVMBG](../../services/mock-pvmbg/README.md) |

## Variants and historical artifacts

[architecture.id.mmd](architecture.id.mmd) is the separate Indonesian reference.
[architecture.report.mmd](architecture.report.mmd) is the report variant for
`docs/report/figures/arsitektur.pdf` (Gambar 2.1); neither is an automatic mirror
of the English source.

Files in [rendered/](rendered/) and report figures remain historical,
unregenerated artifacts and may differ from current sources. Historical
measurements are retained in [ADR audit evidence](../evidence/adr-audit/README.md).

## Maintenance

- Edit the canonical source; for root/ADR embeddings, update the matching block
  identically. Service guides need only retain the correct source link.
- Check local links and compare mapped source/block pairs. Text equality checks
  synchronization, not Mermaid rendering or runtime correctness.
- Keep routes, process defaults, delivery limits, and planned/runtime labels
  accurate. Do not present unresolved M2 choices as implemented guarantees.
- Preview with a compatible Mermaid renderer. Some historical sources retain
  colour classes; rely on explicit labels rather than colour alone.
- Regenerate rendered/report artifacts only as a separately scoped task.
