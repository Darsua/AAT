#!/usr/bin/env python3
"""Unit and cross-language verification tests for scripts/verify_audit_and_catalog.py."""

import hashlib
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import verify_audit_and_catalog as verifier  # noqa: E402


class VerifyAuditAndCatalogTest(unittest.TestCase):
    def test_compute_independent_checksum_raw_byte_order_and_no_trailing_lf(self):
        count, empty_digest = verifier.compute_independent_checksum([])
        self.assertEqual(count, 0)
        self.assertEqual(empty_digest, hashlib.sha256(b"").hexdigest())

        # Uppercase ASCII sorts before lowercase in raw UTF-8 byte order;
        # deduplication removes repeated IDs; no trailing '\n' is appended.
        refs = ["bmkg-2", "BMKG-1", "bmkg-2", "satelit-\u00e9"]
        count, digest = verifier.compute_independent_checksum(refs)
        self.assertEqual(count, 3)
        expected = hashlib.sha256("BMKG-1\nbmkg-2\nsatelit-\u00e9".encode("utf-8")).hexdigest()
        self.assertEqual(digest, expected)

    def test_verify_live_audit_api_reconciles_multi_page_and_detects_tampering(self):
        events = []
        base_hazards = [
            ("haz-1", "BMKG", "BMKG-REF-1", "2026-11-12T01:00:00Z"),
            ("haz-2", "PVMBG", "PVMBG-REF-1", "2026-11-12T01:01:00Z"),
            ("haz-3", "SATELIT", "SAT-REF-1", "2026-11-12T01:02:00Z"),
            ("haz-4", "SENSOR_PANTAI", "PANTAI-REF-1", "2026-11-12T01:03:00Z"),
            ("haz-5", "BMKG", "BMKG-REF-2", "2026-11-12T01:04:00Z"),
            ("haz-6", "BMKG", "BMKG-REF-3", "2026-11-12T01:05:00Z"),
        ]
        for hid, src, ref, ts in base_hazards:
            events.append({
                "hazard_id": hid,
                "source": src,
                "source_ref_id": ref,
                "hazard_type": "SEISMIC",
                "severity": "WASPADA",
                "area_name": "Jawa Barat",
                "occurred_at": ts,
                "ingested_at": ts,
                "attributes": {},
            })

        server, base_url = verifier.start_in_memory_audit_server(events, token="unit-test-token")
        try:
            report = verifier.verify_live_audit_api(
                base_url,
                "unit-test-token",
                catalog_events=events,
                sample_locations=0,
                page_limit=2,
            )
            self.assertTrue(report["passed"], msg=f"unexpected errors: {report['errors']}")
            self.assertEqual(report["enumerated_count"], 6)
            self.assertEqual(report["pages"], 3)
            self.assertEqual(report["checksums"]["BMKG"]["api_count"], 3)

            # A tampered catalog event must trigger mismatch detection.
            tampered_catalog = [dict(ev) for ev in events]
            tampered_catalog[0]["source_ref_id"] = "BMKG-TAMPERED"
            bad_report = verifier.verify_live_audit_api(
                base_url,
                "unit-test-token",
                catalog_events=tampered_catalog,
                sample_locations=6,
                page_limit=2,
            )
            self.assertFalse(bad_report["passed"])
            self.assertTrue(any("Catalog mismatch" in err for err in bad_report["errors"]))
        finally:
            server.shutdown()
            server.server_close()

    def test_cross_language_go_and_python_self_test(self):
        report = verifier.run_self_test()
        self.assertTrue(report["passed"], msg=f"self-test errors: {report['errors']}")
        self.assertEqual(report["go_catalog_total"], 189)
        self.assertTrue(report["go_no_node_holds_all_records"])
        for scope in ("ALL", "BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"):
            self.assertTrue(report["go_python_checksum_parity"][scope]["matched"])


if __name__ == "__main__":
    unittest.main()
