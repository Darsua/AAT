#!/usr/bin/env python3
"""Lightweight repository checks, not schema validation or secret certification."""

import argparse
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote, urlsplit


FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
LINK = re.compile(r"\[[^\]\n]*\]\(\s*(<[^>]+>|[^\s)]+)(?:\s+[^)]*)?\)")
REFERENCE = re.compile(r"^ {0,3}\[[^\]]+\]:\s*(<[^>]+>|\S+)", re.MULTILINE)
SECRET_PATTERN = re.compile(
    r"\bgh[pousr]_[A-Za-z0-9]{36,}\b|\bgithub_pat_[A-Za-z0-9_]{22,}\b"
    r"|-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----"
)


def repository_paths(root, *, tracked_only=False):
    command = ["git", "--no-pager", "ls-files", "--cached"]
    if not tracked_only:
        command += ["--others", "--exclude-standard"]
    result = subprocess.run(command + ["-z"], cwd=root, check=True, capture_output=True)
    return sorted({Path(name.decode("utf-8")) for name in result.stdout.split(b"\0") if name})


def check_configs(root, paths):
    import yaml  # Only config syntax checks require the existing PyYAML dependency.

    errors = []
    for path in paths:
        if path.suffix.lower() not in {".yaml", ".yml", ".json"}:
            continue
        try:
            text = (root / path).read_text(encoding="utf-8")
            if path.suffix.lower() == ".json":
                def reject_constant(value):
                    raise ValueError("Non-JSON numeric constant")
                json.loads(text, parse_constant=reject_constant)
                continue
            documents = list(yaml.safe_load_all(text))
        except (OSError, UnicodeError, ValueError, yaml.YAMLError):
            # Parser exceptions may contain source lines with credentials.
            errors.append(f"{path}: invalid or unreadable {path.suffix[1:].upper()} syntax")
            continue
        if not path.name.endswith((".template.yaml", ".template.yml")):
            continue
        for number, document in enumerate(documents, 1):
            if not isinstance(document, dict) or document.get("kind") != "Secret":
                continue
            for field in ("data", "stringData"):
                if field not in document:
                    continue
                values = document[field]
                if not isinstance(values, dict) or any(value != "" for value in values.values()):
                    errors.append(f"{path}: document {number} Secret {field} must contain only empty strings")
    return errors


def markdown_parts(text):
    """Return prose and fenced Mermaid bodies; ignore links inside fenced examples."""
    prose, diagrams, body = [], [], []
    opening = None
    language = ""
    for line in text.splitlines():
        fence = FENCE.match(line)
        if opening is None:
            if fence:
                opening, language = fence.groups()
                body = []
            else:
                prose.append(line)
        elif (fence and fence[1][0] == opening[0]
              and len(fence[1]) >= len(opening) and not fence[2].strip()):
            if language.strip() == "mermaid":
                diagrams.append("\n".join(body))
            opening = None
        else:
            body.append(line)
    return "\n".join(prose), diagrams, opening is not None


def anchors(text):
    prose, _, _ = markdown_parts(text)
    found, counts = set(), {}
    for line in prose.splitlines():
        heading = re.match(r"^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        if not heading:
            continue
        title = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", heading[1])
        slug = re.sub(r"[^\w\- ]", "", title.lower()).replace(" ", "-")
        count = counts.get(slug, 0)
        counts[slug] = count + 1
        found.add(f"{slug}-{count}" if count else slug)
    found.update(re.findall(r'<(?:a|[a-z][a-z0-9]*)\b[^>]*\b(?:id|name)=[\"\']([^\"\']+)', prose))
    return found


def normalize_mermaid(text):
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def check_docs(root, paths):
    errors = []
    canonical = {
        normalize_mermaid((root / path).read_text(encoding="utf-8"))
        for path in paths if path.parent == Path("docs/diagrams") and path.suffix == ".mmd"
    }
    for path in paths:
        if path.suffix.lower() != ".md":
            continue
        text = (root / path).read_text(encoding="utf-8")
        prose, diagrams, unclosed = markdown_parts(text)
        if unclosed:
            errors.append(f"{path}: unclosed Markdown fence")
        for diagram in diagrams:
            if normalize_mermaid(diagram) not in canonical:
                errors.append(f"{path}: embedded Mermaid does not match a canonical docs/diagrams/*.mmd source")
        targets = [match[1] for match in LINK.finditer(prose)]
        targets += [match[1] for match in REFERENCE.finditer(prose)]
        for raw in targets:
            link = urlsplit(raw.strip("<>"))
            if link.scheme or link.netloc:
                continue
            destination = unquote(link.path)
            target = ((root / destination.lstrip("/")) if destination.startswith("/")
                      else (root / path.parent / destination)) if destination else root / path
            if not target.exists():
                errors.append(f"{path}: missing relative link target {destination}")
            elif link.fragment and target.suffix.lower() == ".md":
                if unquote(link.fragment) not in anchors(target.read_text(encoding="utf-8")):
                    errors.append(f"{path}: missing Markdown anchor in {destination or path.name}: #{link.fragment}")
    return errors


def check_hygiene(root, paths):
    errors = []
    for path in paths:
        name = path.name.lower()
        if name == ".env.example":
            continue  # Example contents are outside this heuristic credential scan.
        if name == ".env" or name.startswith(".env."):
            errors.append(f"{path}: real environment file must not be tracked")
            continue
        example = any(part in name for part in ("example", "template"))
        if ("kubeconfig" in name or path.as_posix().endswith(".kube/config")) and not example:
            errors.append(f"{path}: real kubeconfig must not be tracked")
            continue
        data = (root / path).read_bytes()
        if b"\0" in data:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeError:
            continue
        if SECRET_PATTERN.search(text):
            errors.append(f"{path}: common token or private-key pattern detected (value withheld)")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("docs", "configs", "hygiene"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    paths = repository_paths(root, tracked_only=args.mode == "hygiene")
    # Ignore local deletions; the environment example is allowlisted by filename.
    paths = [path for path in paths if (root / path).is_file() and path.name != ".env.example"]
    checks = {"docs": check_docs, "configs": check_configs, "hygiene": check_hygiene}
    errors = checks[args.mode](root, paths)
    for error in errors:
        print(error)
    print(f"{args.mode}: {len(errors)} error(s)")
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
