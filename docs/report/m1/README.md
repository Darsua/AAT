# Milestone 1 LaTeX report

This directory preserves the modular M1 report for the BNPB Disaster Coordination Platform. The manuscript is in Indonesian; this guide and the repository's other Markdown documentation use English. Files are split by chapter and problem to reduce editing conflicts.

## Structure

```text
docs/report/m1/
├── main.tex                       # Entry point
├── preamble.tex                   # Page layout, packages, tables, and code listings
├── references.bib                 # BibTeX references
├── chapters/
│   ├── 00_cover.tex               # Cover and team details
│   ├── 00_frontmatter.tex         # Contents, lists of tables/figures, and contributions
│   ├── 01_pendahuluan.tex         # Introduction, objectives, and assumptions
│   ├── 02_arsitektur.tex          # Architecture, service boundaries, and technology
│   ├── 03_solusi_p1.tex           # PVMBG schema evolution
│   ├── 03_solusi_p2.tex           # Concurrency, degradation, and load testing
│   ├── 03_solusi_p3.tex           # Authentication and authorization
│   ├── 03_solusi_p4.tex           # Storage and container isolation
│   ├── 03_solusi_p5.tex           # Pub-sub and consumer recovery
│   └── 04_petunjuk.tex            # Running the system and verification
├── appendix/
│   ├── ai_declaration.tex         # AI-use declaration
│   └── diagrams.tex               # Supplementary diagrams
└── figures/                       # Report-specific visual assets
```

These are **M1** problem numbers, not the M2 problem structure. Evidence is archived separately in [docs/evidence/m1](../../evidence/m1/).

## Build

Install [Tectonic](https://tectonic-typesetting.github.io/), then run from the repository root:

```bash
make report
```

Tectonic may download TeX packages. The build produces:

- `docs/report/m1/main.pdf`: local Tectonic output, ignored by Git.
- `docs/report/m1/IF4031_M1_AhhAldoyTolong.pdf`: the identical, submission-named copy retained as the historical M1 artifact.

Rebuilding replaces the submission-named PDF. Do not replace the historical artifact unintentionally.

## Editing conventions

- Preserve each problem's existing sections: solution design and mechanisms, decision rationale and trade-offs, known limitations, evidence, and answers to the specification's report questions.
- Escape `_` as `\_` outside code listings and `&` as `\&` outside tables.
- Use `\begin{lstlisting}[language=...] ... \end{lstlisting}` for code. The preamble supports `bash`, `Go`, `SQL`, and `json`.
- Preserve historical measurements and their limitations. M1 evidence does not establish M2 implementation or recovery guarantees.
