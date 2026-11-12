# Reports

Reports use LaTeX and are separated by milestone:

- [m1/](m1/README.md): existing M1 manuscript, figures, and the named submission PDF. Run `make report` from the repository root to rebuild it with Tectonic.
- [m2/](m2/): reserved folders for the M2 manuscript (`chapters/`, `appendix/`, and `figures/`). No M2 report content or build target has been added yet.

Tectonic's generated `main.pdf` is ignored by Git; only the named submission PDF is retained. Rebuilding M1 replaces that submission PDF.

Keep milestone evidence in [docs/evidence](../evidence/), not duplicated in report folders. M1 figures and measurements remain historical; they are not M2 evidence.
