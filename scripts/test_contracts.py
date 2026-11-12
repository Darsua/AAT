"""Structural regression checks for repository contracts, not payload validation."""

import json
from pathlib import Path
import unittest

import yaml


CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
SOURCES = {"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"}
HAZARD_TYPES = {"SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL"}
MODES = {"additive", "rename_field", "type_change", "missing_required_field"}
SUMMARY_KEYS = {
    "hazard_id", "source", "hazard_type", "severity", "area_name",
    "occurred_at", "ingested_at",
}
CANONICAL_KEYS = SUMMARY_KEYS | {"source_ref_id", "attributes"}


class ContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.documents = {}
        for name in (
            "aggregator_internal", "audit_api", "client_api", "coastal_sensor", "pvmbg"
        ):
            cls.documents[name] = yaml.safe_load(
                (CONTRACTS / f"{name}.yaml").read_text(encoding="utf-8")
            )
        cls.canonical = json.loads(
            (CONTRACTS / "hazard_event.json").read_text(encoding="utf-8")
        )

    def schema(self, document, name):
        return self.documents[document]["components"]["schemas"][name]

    def operation(self, document, path, method="get"):
        return self.documents[document]["paths"][path][method]

    def parameter(self, document, path, name):
        matches = [
            parameter
            for parameter in self.operation(document, path)["parameters"]
            if parameter["name"] == name and parameter["in"] == "query"
        ]
        self.assertEqual(len(matches), 1, (document, path, name))
        return matches[0]

    def response_schema(self, document, path, method="get"):
        return self.operation(document, path, method)["responses"]["200"][
            "content"
        ]["application/json"]["schema"]

    def pvmbg_request(self):
        return self.operation("pvmbg", "/admin/schema-version", "post")[
            "requestBody"
        ]["content"]["application/json"]["schema"]

    def representations(self):
        return (
            ("canonical JSON", self.canonical, CANONICAL_KEYS),
            ("aggregator", self.schema("aggregator_internal", "HazardEvent"), CANONICAL_KEYS),
            ("audit", self.schema("audit_api", "CanonicalHazardEvent"), CANONICAL_KEYS),
            ("client full", self.schema("client_api", "HazardEventFull"), CANONICAL_KEYS),
            ("client summary", self.schema("client_api", "HazardEventSummary"), SUMMARY_KEYS),
        )

    def test_canonical_source_and_hazard_type_enums_are_consistent(self):
        for name, schema, _ in self.representations():
            for field, expected in (("source", SOURCES), ("hazard_type", HAZARD_TYPES)):
                with self.subTest(representation=name, field=field):
                    prop = schema["properties"][field]
                    self.assertEqual(prop["type"], "string")
                    self.assertCountEqual(prop["enum"], expected)

    def test_inline_envelopes_preserve_required_keys_and_types(self):
        for name, schema, required in self.representations():
            with self.subTest(representation=name):
                self.assertEqual(schema["type"], "object")
                self.assertCountEqual(schema["required"], required)
                self.assertTrue(required <= schema["properties"].keys())
                for field in required - {"attributes"}:
                    self.assertEqual(schema["properties"][field]["type"], "string", field)
                for field in ("occurred_at", "ingested_at"):
                    self.assertEqual(schema["properties"][field]["format"], "date-time")
                if "attributes" in required:
                    attributes = schema["properties"]["attributes"]
                    self.assertEqual(attributes["type"], "object")
                    self.assertIs(attributes["additionalProperties"], True)

    def test_filter_enums_match_canonical_enums(self):
        for document, path in (
            ("client_api", "/api/v1/hazards"),
            ("aggregator_internal", "/internal/hazards"),
        ):
            with self.subTest(document=document):
                self.assertCountEqual(
                    self.parameter(document, path, "type")["schema"]["enum"], HAZARD_TYPES
                )
        for path in (
            "/internal/audit/events", "/internal/audit/checksum", "/internal/audit/unprocessed"
        ):
            with self.subTest(path=path):
                self.assertCountEqual(
                    self.parameter("audit_api", path, "source")["schema"]["enum"], SOURCES
                )

    def test_audit_checksum_source_is_optional(self):
        source = self.parameter("audit_api", "/internal/audit/checksum", "source")
        self.assertIs(source.get("required", False), False)

    def test_audit_pagination_default_and_maximum(self):
        for path in ("/internal/audit/events", "/internal/audit/unprocessed"):
            with self.subTest(path=path):
                limit = self.parameter("audit_api", path, "limit")
                self.assertIs(limit.get("required", False), False)
                self.assertEqual(limit["schema"]["type"], "integer")
                self.assertEqual(limit["schema"]["minimum"], 1)
                self.assertEqual(limit["schema"]["default"], 100)
                self.assertEqual(limit["schema"]["maximum"], 1000)

    def test_audit_events_reference_canonical_hazard_event(self):
        response = self.schema("audit_api", "AuditEventsResponse")
        self.assertEqual(response["properties"]["items"]["type"], "array")
        self.assertEqual(response["properties"]["items"]["items"], {
            "$ref": "#/components/schemas/CanonicalHazardEvent"
        })

    def test_client_response_unions_use_anyof_with_correct_references(self):
        listing = self.response_schema("client_api", "/api/v1/hazards")
        self.assertEqual(listing["properties"]["data"]["type"], "array")
        schemas = (
            listing["properties"]["data"]["items"],
            self.response_schema("client_api", "/api/v1/hazards/{id}"),
        )
        for location, schema in zip(("list data.items", "detail 200"), schemas):
            with self.subTest(location=location):
                self.assertNotIn("oneOf", schema)
                self.assertCountEqual(schema["anyOf"], [
                    {"$ref": "#/components/schemas/HazardEventSummary"},
                    {"$ref": "#/components/schemas/HazardEventFull"},
                ])
                for branch in schema["anyOf"]:
                    self.assertIn(
                        branch["$ref"].removeprefix("#/components/schemas/"),
                        self.documents["client_api"]["components"]["schemas"],
                    )

    def test_client_projection_schemas_remain_permissive(self):
        for name in ("HazardEventSummary", "HazardEventFull"):
            with self.subTest(schema=name):
                schema = self.schema("client_api", name)
                self.assertIs(schema.get("additionalProperties", True), True)
                for keyword in ("not", "oneOf", "anyOf", "allOf"):
                    self.assertNotIn(keyword, schema)
        summary = self.schema("client_api", "HazardEventSummary")
        self.assertEqual(set(summary["properties"]), SUMMARY_KEYS)

    def test_coastal_quality_is_an_unrestricted_string(self):
        quality = self.schema("coastal_sensor", "Observation")["properties"]["quality"]
        self.assertEqual(quality["type"], "string")
        for keyword in ("enum", "const", "pattern", "minLength", "maxLength", "anyOf", "oneOf", "allOf", "not"):
            with self.subTest(keyword=keyword):
                self.assertNotIn(keyword, quality)

    def test_coastal_limit_has_no_undocumented_default(self):
        limit = self.parameter("coastal_sensor", "/v1/observations", "limit")
        self.assertEqual(limit["schema"]["type"], "integer")
        self.assertIs(limit.get("required", False), False)
        self.assertNotIn("default", limit["schema"])

    def test_pvmbg_request_declares_legacy_boolean_and_mode(self):
        request = self.pvmbg_request()
        self.assertNotIn("$ref", request)
        self.assertEqual(request["type"], "object")
        properties = request["properties"]
        self.assertEqual(properties["include_confidence_level"]["type"], "boolean")
        self.assertEqual(properties["mode"]["type"], "string")
        self.assertCountEqual(properties["mode"]["enum"], MODES)
        self.assertEqual(properties["mode"]["default"], "additive")

    def test_pvmbg_request_anyof_requires_at_least_one_control(self):
        request = self.pvmbg_request()
        self.assertEqual(request.get("required", []), [])
        self.assertNotIn("oneOf", request)
        self.assertCountEqual(request["anyOf"], [
            {"required": ["include_confidence_level"]},
            {"required": ["mode"]},
        ])

    def test_pvmbg_response_mode_is_optional_and_legacy_keys_remain_required(self):
        response = self.response_schema("pvmbg", "/admin/schema-version", "post")
        self.assertEqual(response["type"], "object")
        self.assertCountEqual(response["required"], ["status", "include_confidence_level"])
        self.assertNotIn("mode", response["required"])
        self.assertEqual(response["properties"]["status"]["type"], "string")
        self.assertEqual(response["properties"]["include_confidence_level"]["type"], "boolean")
        self.assertEqual(response["properties"]["mode"]["type"], "string")
        self.assertCountEqual(response["properties"]["mode"]["enum"], MODES)

    def test_pvmbg_breaking_modes_document_affected_fields(self):
        operation = self.operation("pvmbg", "/admin/schema-version", "post")
        request = self.pvmbg_request()
        descriptions = " ".join((
            operation.get("description", ""), request.get("description", ""),
            request["properties"].get("mode", {}).get("description", ""),
        ))
        for term in (
            "rename_field", "report_id", "event_id", "type_change",
            "eruption_count_24h", "string", "missing_required_field", "volcano_id",
        ):
            with self.subTest(term=term):
                self.assertIn(term, descriptions)
        self.assertRegex(descriptions.lower(), r"number|numeric|integer")


if __name__ == "__main__":
    unittest.main()
