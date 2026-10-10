# Diagrams

All architecture and flow diagrams are written in [Mermaid](https://mermaid.js.org/). The committed `.mmd` files are the canonical sources and evidence for the diagrams. Matching Mermaid blocks are embedded in the relevant Markdown files so GitHub can render them directly.

Rendering and synchronization helper scripts are intentionally not committed. Contributors may preview the `.mmd` files with any compatible Mermaid renderer, but must keep each embedded Markdown block identical to its canonical source.

| Source | Used in |
| :--- | :--- |
| `architecture.en.mmd` | `README.md` |
| `architecture.id.mmd` | Indonesian full variant (reference) |
| `architecture.report.mmd` | Report figure (`docs/report/figures/arsitektur.pdf`, Gambar 2.1) |
| `adr002-fanout.mmd` | ADR 002 |
| `adr003-outbox.mmd` | ADR 003 |
| `adr004-sequence.mmd` | ADR 004 |
| `adr005-scaleout.mmd`, `adr005-breaker.mmd` | ADR 005 (both marked plan only where applicable) |
| `aggregator-flow.mmd` | `services/aggregator/README.md` |
| `auth-flow.mmd` | `services/auth/README.md` |
| `clientapi-gateway.mmd` | `services/client-api/README.md` |
| `consumers-fanout.mmd` | `services/consumers/README.md` |
| `mock-bmkg-context.mmd`, `mock-pvmbg-context.mmd` | `services/mock-bmkg/README.md`, `services/mock-pvmbg/README.md` |

## Conventions

- Colour classes (`svc`, `store`, `broker`, `ext`, `cons`, `actor`, `plan`) are declared at the bottom of each flowchart. `plan` (dashed grey) means not implemented.
- Labels carry real service names, ports, routes and consumer group names from `docker-compose.yml` and the code. Update a diagram whenever those change.
- Text is English except the Indonesian architecture variant and report figure.

## Editing

1. Edit the `.mmd` file.
2. Paste the same text into the matching Markdown block.
3. Compare the source and embedded block before committing.
4. If the diagram is used by the report, refresh the corresponding figure with a compatible Mermaid renderer and run `make report`.
