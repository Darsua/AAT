#!/usr/bin/env python3
"""Independent SHA-256 checksum, event enumeration, and placement verifier for the Appendix A Audit API."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from typing import Any, Iterable
import urllib.error
import urllib.parse
import urllib.request


CANONICAL_SOURCES = ("BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI")


class AuditAPIError(RuntimeError):
    """Raised when an Audit API request fails or returns a malformed response."""


def compute_independent_checksum(
    source_ref_ids: Iterable[str],
    *,
    deduplicate: bool = True,
) -> tuple[int, str]:
    """Compute Appendix A SHA-256 digest over raw-byte sorted source_ref_id values.

    Values are sorted in ascending UTF-8 raw byte order, joined with LF ('\\n')
    without any trailing newline, and returned as (count, 64-char lowercase hex).
    """
    items = set(source_ref_ids) if deduplicate else list(source_ref_ids)
    ordered = sorted(items, key=lambda ref: ref.encode("utf-8"))
    payload = "\n".join(ordered).encode("utf-8")
    return len(ordered), hashlib.sha256(payload).hexdigest()


def parse_utc_timestamp(raw: str) -> datetime:
    """Parse an ISO-8601 / RFC3339 timestamp string into UTC datetime."""
    normalized = raw.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_catalog_events(path: Path) -> list[dict[str, Any]]:
    """Load canonical events from a JSON array or {events|items} wrapper."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        events = raw
    elif isinstance(raw, dict):
        events = raw.get("events") if raw.get("events") is not None else raw.get("items")
    else:
        raise ValueError(f"Unsupported catalog JSON format in {path}")
    if not isinstance(events, list):
        raise ValueError(f"Catalog in {path} does not contain an event list")
    return [ev for ev in events if isinstance(ev, dict)]


class AuditAPIClient:
    """Minimal authenticated HTTP client for /internal/audit/* endpoints."""

    def __init__(self, base_url: str, token: str, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _get_json(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        query = f"?{urllib.parse.urlencode(params)}" if params else ""
        url = f"{self.base_url}{path}{query}"
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise AuditAPIError(f"GET {path}{query} returned HTTP {exc.code}: {detail}") from exc
        except (OSError, ValueError) as exc:
            raise AuditAPIError(f"GET {path}{query} failed: {exc}") from exc

    def iter_all_events(
        self,
        *,
        source: str | None = None,
        since: str | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], int, bool, list[str]]:
        """Paginate GET /internal/audit/events to exhaustion and enforce ordering invariants."""
        events: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        seen_cursors: set[str] = set()
        unavailable: set[str] = set()
        partial_any = False
        cursor: str | None = None
        pages = 0
        prev_key: tuple[datetime, str] | None = None

        while True:
            params: dict[str, str] = {"limit": str(limit)}
            if source:
                params["source"] = source
            if since:
                params["since"] = since
            if cursor:
                params["cursor"] = cursor

            page = self._get_json("/internal/audit/events", params)
            pages += 1
            if bool(page.get("partial", False)):
                partial_any = True
            for unit in page.get("unavailable_units") or []:
                unavailable.add(str(unit))

            items = page.get("items") or []
            for ev in items:
                hazard_id = str(ev["hazard_id"])
                if hazard_id in seen_ids:
                    raise AuditAPIError(f"Duplicate hazard_id across pagination boundaries: {hazard_id}")
                seen_ids.add(hazard_id)

                sort_key = (parse_utc_timestamp(str(ev["ingested_at"])), hazard_id)
                if prev_key is not None and sort_key < prev_key:
                    raise AuditAPIError(
                        f"Events out of canonical order: {sort_key} preceded by {prev_key}"
                    )
                prev_key = sort_key
                events.append(ev)

            next_cursor = page.get("next_cursor")
            if not next_cursor:
                break
            next_cursor = str(next_cursor)
            if next_cursor in seen_cursors:
                raise AuditAPIError(f"Pagination cursor cycle detected at {next_cursor!r}")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return events, pages, partial_any, sorted(unavailable)

    def get_checksum(self, source: str | None = None) -> dict[str, Any]:
        params = {"source": source} if source else None
        return self._get_json("/internal/audit/checksum", params)

    def get_location(self, hazard_id: str) -> dict[str, Any]:
        return self._get_json("/internal/audit/location", {"hazard_id": hazard_id})

    def list_unprocessed(self, *, source: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            params: dict[str, str] = {"limit": str(limit)}
            if source:
                params["source"] = source
            if cursor:
                params["cursor"] = cursor
            page = self._get_json("/internal/audit/unprocessed", params)
            items.extend(page.get("items") or [])
            next_cursor = page.get("next_cursor")
            if not next_cursor:
                break
            next_cursor = str(next_cursor)
            if next_cursor in seen_cursors:
                raise AuditAPIError(f"Unprocessed cursor cycle detected at {next_cursor!r}")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return items


def _sample_events_evenly(events: list[dict[str, Any]], sample_limit: int) -> list[dict[str, Any]]:
    if sample_limit <= 0 or sample_limit >= len(events):
        return list(events)
    if sample_limit == 1:
        return [events[0]]
    step = (len(events) - 1) / float(sample_limit - 1)
    indices = sorted({min(len(events) - 1, int(round(i * step))) for i in range(sample_limit)})
    return [events[idx] for idx in indices]


def verify_live_audit_api(
    base_url: str,
    token: str,
    *,
    catalog_events: list[dict[str, Any]] | None = None,
    sample_locations: int = 30,
    page_limit: int = 100,
    all_sources_label: str = "ALL",
) -> dict[str, Any]:
    """Reconcile /internal/audit/{events,checksum,location,unprocessed} against independent checks."""
    client = AuditAPIClient(base_url, token)
    errors: list[str] = []
    checks: list[dict[str, Any]] = []

    events, pages, partial_events, unavailable_units = client.iter_all_events(limit=page_limit)
    checks.append({
        "name": "events_pagination_and_ordering",
        "passed": not partial_events,
        "count": len(events),
        "pages": pages,
        "partial": partial_events,
        "unavailable_units": unavailable_units,
    })
    if partial_events:
        errors.append(f"/internal/audit/events reported partial=true (unavailable={unavailable_units})")

    checksums_report: dict[str, Any] = {}
    scopes: list[str | None] = [None, *CANONICAL_SOURCES]

    for scope in scopes:
        label = all_sources_label if scope is None else scope
        scoped_events = events if scope is None else [ev for ev in events if ev.get("source") == scope]
        ind_count, ind_digest = compute_independent_checksum(
            (str(ev["source_ref_id"]) for ev in scoped_events),
            deduplicate=True,
        )

        api_res = client.get_checksum(scope)
        api_count = int(api_res.get("count", -1))
        api_digest = str(api_res.get("checksum", ""))
        api_algo = str(api_res.get("algorithm", ""))
        api_partial = bool(api_res.get("partial", False))

        scope_ok = (
            api_algo == "sha256"
            and not api_partial
            and api_count == ind_count
            and api_digest == ind_digest
            and len(api_digest) == 64
        )
        entry: dict[str, Any] = {
            "scope": label,
            "api_count": api_count,
            "api_checksum": api_digest,
            "independent_count": ind_count,
            "independent_checksum": ind_digest,
            "partial": api_partial,
            "passed": scope_ok,
        }

        if catalog_events is not None:
            cat_scoped = catalog_events if scope is None else [ev for ev in catalog_events if ev.get("source") == scope]
            cat_count, cat_digest = compute_independent_checksum(
                (str(ev["source_ref_id"]) for ev in cat_scoped),
                deduplicate=True,
            )
            entry["catalog_count"] = cat_count
            entry["catalog_checksum"] = cat_digest
            if cat_count != api_count or cat_digest != api_digest:
                scope_ok = False
                entry["passed"] = False
                errors.append(
                    f"Catalog mismatch for {label}: api=({api_count}, {api_digest}) != catalog=({cat_count}, {cat_digest})"
                )

        if not scope_ok and label not in "".join(errors):
            errors.append(
                f"Checksum mismatch for {label}: api=({api_count}, {api_digest}, algo={api_algo}, partial={api_partial}) "
                f"!= independent=({ind_count}, {ind_digest})"
            )

        checksums_report[label] = entry
        checks.append({
            "name": f"checksum_{label}",
            "passed": entry["passed"],
            "count": api_count,
            "checksum": api_digest,
        })

    sampled = _sample_events_evenly(events, sample_locations)
    primary_by_node: dict[str, int] = defaultdict(int)
    replica_by_node: dict[str, int] = defaultdict(int)
    unique_by_node: dict[str, set[str]] = defaultdict(set)
    distinct_units: set[str] = set()
    multi_node_placements = 0

    for ev in sampled:
        hid = str(ev["hazard_id"])
        loc_res = client.get_location(hid)
        locs = loc_res.get("locations") or []
        if not locs:
            errors.append(f"/internal/audit/location returned zero locations for {hid}")
            continue
        primary_nodes: set[str] = set()
        replica_nodes: set[str] = set()
        for loc in locs:
            node = str(loc.get("node", ""))
            unit = str(loc.get("unit", ""))
            role = str(loc.get("role", ""))
            if not node or not unit:
                errors.append(f"Malformed location entry for {hid}: {loc}")
                continue
            distinct_units.add(unit)
            unique_by_node[node].add(hid)
            if role == "primary":
                primary_by_node[node] += 1
                primary_nodes.add(node)
            elif role == "replica":
                replica_by_node[node] += 1
                replica_nodes.add(node)
        if not primary_nodes:
            errors.append(f"No primary storage location reported for {hid}")
        if primary_nodes and replica_nodes and primary_nodes.isdisjoint(replica_nodes):
            multi_node_placements += 1

    observed_nodes = sorted(unique_by_node.keys())
    max_primary = max((primary_by_node[n] for n in observed_nodes), default=0)
    max_unique = max((len(unique_by_node[n]) for n in observed_nodes), default=0)
    no_node_holds_all = (
        len(sampled) == 0
        or len(observed_nodes) < 2
        or (max_primary < len(sampled) and max_unique < len(sampled))
    )
    if sampled and len(observed_nodes) >= 2 and not no_node_holds_all:
        errors.append(
            f"Single node holds all {len(sampled)} sampled records: primary={dict(primary_by_node)} unique={ {k: len(v) for k, v in unique_by_node.items()} }"
        )

    checks.append({
        "name": "location_sharding_and_replication",
        "passed": len(sampled) == 0 or (no_node_holds_all and all("locations" not in e for e in errors)),
        "sampled_events": len(sampled),
        "distinct_nodes": observed_nodes,
        "distinct_units": sorted(distinct_units),
        "distinct_primary_replica_nodes_count": multi_node_placements,
        "no_node_holds_all_records": no_node_holds_all,
    })

    unprocessed = client.list_unprocessed()
    checks.append({
        "name": "unprocessed_dlq_endpoint",
        "passed": True,
        "count": len(unprocessed),
    })

    node_distribution = {
        node: {
            "primary_sampled": primary_by_node[node],
            "replica_sampled": replica_by_node[node],
            "unique_sampled": len(unique_by_node[node]),
        }
        for node in observed_nodes
    }

    return {
        "passed": len(errors) == 0 and all(bool(c["passed"]) for c in checks),
        "enumerated_count": len(events),
        "pages": pages,
        "checks": checks,
        "checksums": checksums_report,
        "node_distribution": node_distribution,
        "unprocessed_count": len(unprocessed),
        "errors": errors,
    }


def _consistent_shard_for_id(hazard_id: str, shard_ids: list[str] | None = None, vnodes: int = 64) -> str:
    """Mirror services/aggregator ConsistentHashRing (sha256 big-endian uint64, 64 vnodes)."""
    shards = shard_ids or ["shard-1", "shard-2", "shard-3"]
    ring: list[tuple[int, str]] = []
    for sid in shards:
        for i in range(vnodes):
            digest = hashlib.sha256(f"{sid}#{i}".encode("utf-8")).digest()
            ring.append((int.from_bytes(digest[:8], "big"), sid))
    ring.sort(key=lambda pair: (pair[0], pair[1]))

    target = int.from_bytes(hashlib.sha256(hazard_id.encode("utf-8")).digest()[:8], "big")
    for h, sid in ring:
        if h >= target:
            return sid
    return ring[0][1]


def start_in_memory_audit_server(
    events: list[dict[str, Any]],
    *,
    token: str = "selftest-audit-token",
    unprocessed: list[dict[str, Any]] | None = None,
) -> tuple[ThreadingHTTPServer, str]:
    """Start an ephemeral HTTP server implementing /internal/audit/* for self-tests and unit tests."""
    ordered = sorted(
        events,
        key=lambda ev: (parse_utc_timestamp(str(ev["ingested_at"])), str(ev["hazard_id"])),
    )
    by_id = {str(ev["hazard_id"]): ev for ev in ordered}
    shard_map = {
        "shard-1": [
            {"node": "aqua", "unit": "shard-1", "role": "primary"},
            {"node": "ruby", "unit": "shard-1-r1", "role": "replica"},
        ],
        "shard-2": [
            {"node": "ruby", "unit": "shard-2", "role": "primary"},
            {"node": "kana", "unit": "shard-2-r1", "role": "replica"},
        ],
        "shard-3": [
            {"node": "kana", "unit": "shard-3", "role": "primary"},
            {"node": "aqua", "unit": "shard-3-r1", "role": "replica"},
        ],
    }
    dlq_items = list(unprocessed or [])

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def _send_json(self, code: int, payload: Any) -> None:
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:  # noqa: N802
            auth = self.headers.get("Authorization", "")
            if auth != f"Bearer {token}":
                self._send_json(401, {"error": {"code": "unauthorized", "message": "token tidak valid"}})
                return

            parsed = urllib.parse.urlsplit(self.path)
            qs = urllib.parse.parse_qs(parsed.query)

            if parsed.path == "/internal/audit/events":
                src = qs.get("source", [None])[0]
                limit = min(1000, max(1, int(qs.get("limit", ["100"])[0])))
                offset = int(qs.get("cursor", ["0"])[0])
                filtered = [ev for ev in ordered if not src or ev.get("source") == src]
                chunk = filtered[offset : offset + limit]
                next_cursor = str(offset + limit) if (offset + limit) < len(filtered) else None
                self._send_json(200, {
                    "items": chunk,
                    "next_cursor": next_cursor,
                    "partial": False,
                    "unavailable_units": [],
                })
                return

            if parsed.path == "/internal/audit/checksum":
                src = qs.get("source", [None])[0]
                filtered = [ev for ev in ordered if not src or ev.get("source") == src]
                cnt, digest = compute_independent_checksum(
                    (str(ev["source_ref_id"]) for ev in filtered),
                    deduplicate=True,
                )
                self._send_json(200, {
                    "source": src or "ALL",
                    "count": cnt,
                    "checksum": digest,
                    "algorithm": "sha256",
                    "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "partial": False,
                    "unavailable_units": [],
                })
                return

            if parsed.path == "/internal/audit/location":
                hid = qs.get("hazard_id", [""])[0]
                if hid not in by_id:
                    self._send_json(404, {"error": {"code": "not_found", "message": "hazard_id tidak ditemukan"}})
                    return
                shard_id = _consistent_shard_for_id(hid)
                self._send_json(200, {
                    "hazard_id": hid,
                    "locations": shard_map[shard_id],
                    "partial": False,
                })
                return

            if parsed.path == "/internal/audit/unprocessed":
                self._send_json(200, {
                    "items": dlq_items,
                    "next_cursor": None,
                })
                return

            self._send_json(404, {"error": {"code": "not_found", "message": "not found"}})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{host}:{port}"


def run_self_test() -> dict[str, Any]:
    """Run Go catalog-loader and verify byte-exact SHA-256 & HTTP Audit API parity in Python."""
    repo_root = Path(__file__).resolve().parents[1]
    aggregator_dir = repo_root / "services" / "aggregator"

    with tempfile.TemporaryDirectory() as tmp_dir:
        export_path = Path(tmp_dir) / "catalog.json"
        proc = subprocess.run(
            [
                "go",
                "run",
                "./cmd/catalog-loader",
                "--in-memory",
                "--baseline-count",
                "120",
                "--spike-count",
                "60",
                "--export-catalog",
                str(export_path),
                "--json",
            ],
            cwd=aggregator_dir,
            capture_output=True,
            text=True,
            check=True,
        )
        go_report = json.loads(proc.stdout)
        catalog_events = load_catalog_events(export_path)

    parity_errors: list[str] = []
    cross_checks: dict[str, Any] = {}
    for scope_key in ("ALL", *CANONICAL_SOURCES):
        scoped = catalog_events if scope_key == "ALL" else [ev for ev in catalog_events if ev.get("source") == scope_key]
        py_count, py_digest = compute_independent_checksum(
            (str(ev["source_ref_id"]) for ev in scoped),
            deduplicate=True,
        )
        go_entry = go_report["checksums"][scope_key]
        matched = (py_count == int(go_entry["count"]) and py_digest == str(go_entry["checksum"]))
        cross_checks[scope_key] = {
            "count": py_count,
            "python_checksum": py_digest,
            "go_checksum": go_entry["checksum"],
            "matched": matched,
        }
        if not matched:
            parity_errors.append(
                f"Go vs Python checksum mismatch for {scope_key}: go={go_entry} py=({py_count}, {py_digest})"
            )

    if not go_report.get("no_node_holds_all_records", False):
        parity_errors.append("Go catalog-loader reported no_node_holds_all_records=false")

    token = "selftest-audit-token"
    server, base_url = start_in_memory_audit_server(catalog_events, token=token)
    try:
        http_report = verify_live_audit_api(
            base_url,
            token,
            catalog_events=catalog_events,
            sample_locations=len(catalog_events),
            page_limit=50,
        )
    finally:
        server.shutdown()
        server.server_close()

    all_errors = parity_errors + list(http_report.get("errors") or [])
    return {
        "mode": "self-test",
        "passed": len(all_errors) == 0 and bool(http_report.get("passed", False)),
        "go_catalog_total": go_report["total_events"],
        "go_no_node_holds_all_records": go_report["no_node_holds_all_records"],
        "go_python_checksum_parity": cross_checks,
        "http_audit_verification": http_report,
        "errors": all_errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.getenv("AGGREGATOR_URL", "http://localhost:8083"), help="Aggregator base URL")
    parser.add_argument("--token", default=os.getenv("AUDIT_TOKEN", "audit-secret-token-2026"), help="Audit Bearer token")
    parser.add_argument("--catalog", type=Path, help="Optional canonical catalog JSON file to cross-check against")
    parser.add_argument("--sample-locations", type=int, default=30, help="Number of events to probe on /internal/audit/location (<=0 probes all)")
    parser.add_argument("--page-limit", type=int, default=100, help="Page size for /internal/audit/events")
    parser.add_argument("--all-sources-label", default=os.getenv("AUDIT_ALL_SOURCES_LABEL", "ALL"))
    parser.add_argument("--self-test", action="store_true", help="Run cross-language Go/Python and ephemeral HTTP self-test")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    args = parser.parse_args(argv)

    if args.self_test:
        report = run_self_test()
    else:
        catalog_events = load_catalog_events(args.catalog) if args.catalog else None
        report = verify_live_audit_api(
            args.url,
            args.token,
            catalog_events=catalog_events,
            sample_locations=args.sample_locations,
            page_limit=args.page_limit,
            all_sources_label=args.all_sources_label,
        )

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        status = "PASSED" if report["passed"] else "FAILED"
        print(f"Audit verification {status}")
        if args.self_test:
            print(f"  Events: {report['go_catalog_total']} | No node holds all: {report['go_no_node_holds_all_records']}")
            for scope, entry in report["go_python_checksum_parity"].items():
                print(f"  Parity {scope}: matched={entry['matched']} count={entry['count']} sha256={entry['python_checksum']}")
        else:
            print(f"  Enumerated events: {report['enumerated_count']} ({report['pages']} pages)")
            for scope, entry in report["checksums"].items():
                print(f"  Checksum {scope}: passed={entry['passed']} count={entry['api_count']} sha256={entry['api_checksum']}")
        if report["errors"]:
            for err in report["errors"]:
                print(f"  ERROR: {err}", file=sys.stderr)

    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
