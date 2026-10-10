#!/usr/bin/env python3
"""Verify that every Mermaid block embedded in Markdown equals its source in docs/diagrams/*.mmd.

Usage: python3 docs/diagrams/check_sync.py   (run from the repository root)
Exit code 1 when a block has drifted from its .mmd source.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "docs" / "diagrams"

# Markdown file -> ordered list of .mmd sources, one per ```mermaid block
MAP = {
    "README.md": ["architecture.en"],
    "docs/adr/002-message-broker-redpanda.md": ["adr002-fanout"],
    "docs/adr/003-canonical-storage-postgres-jsonb.md": ["adr003-outbox"],
    "docs/adr/004-internal-communication-rest.md": ["adr004-sequence"],
    "docs/adr/005-stateless-architecture-and-m2-readiness.md": ["adr005-scaleout", "adr005-breaker"],
    "services/aggregator/README.md": ["aggregator-flow"],
    "services/auth/README.md": ["auth-flow"],
    "services/client-api/README.md": ["clientapi-gateway"],
    "services/consumers/README.md": ["consumers-fanout"],
    "services/mock-bmkg/README.md": ["mock-bmkg-context"],
    "services/mock-pvmbg/README.md": ["mock-pvmbg-context"],
}

BLOCK = re.compile(r"^([ \t]*)```mermaid\n(.*?)\n\1```", re.S | re.M)


def norm(text: str, indent: str) -> str:
    lines = [l[len(indent):] if l.startswith(indent) else l for l in text.split("\n")]
    return "\n".join(l.rstrip() for l in lines).strip()


bad = 0
for md, names in MAP.items():
    text = (ROOT / md).read_text(encoding="utf-8")
    blocks = list(BLOCK.finditer(text))
    if len(blocks) != len(names):
        print(f"FAIL {md}: {len(blocks)} mermaid blocks, expected {len(names)}")
        bad += 1
        continue
    for m, name in zip(blocks, names):
        want = (SRC / f"{name}.mmd").read_text(encoding="utf-8").strip()
        if norm(m.group(2), m.group(1)) != want.strip():
            print(f"FAIL {md}: block for {name}.mmd differs from source")
            bad += 1
        else:
            print(f"ok   {md} <- {name}.mmd")
sys.exit(1 if bad else 0)
