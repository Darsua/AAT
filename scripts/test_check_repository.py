"""Regression coverage for the deliberately lightweight repository checks."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import check_repository as checker


class RepositoryChecksTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def file(self, name, text):
        path = Path(name)
        (self.root / path).parent.mkdir(parents=True, exist_ok=True)
        (self.root / path).write_text(text, encoding="utf-8")
        return path

    def test_config_syntax_and_empty_multidocument_secret(self):
        paths = [self.file("ok.json", '{"ok": true}'), self.file(
            "secrets.template.yaml", "kind: ConfigMap\n---\nkind: Secret\ndata: {key: ''}\n"
            "---\nkind: Secret\nstringData: {key: ''}\n")]
        self.assertEqual(checker.check_configs(self.root, paths), [])

    def test_populated_secret_in_later_document_is_rejected_without_value(self):
        path = self.file("secrets.template.yaml", "kind: ConfigMap\n---\nkind: Secret\n"
                         "stringData: {key: do-not-display}\n")
        errors = checker.check_configs(self.root, [path])
        self.assertEqual(len(errors), 1)
        self.assertIn("document 2", errors[0])
        self.assertNotIn("do-not-display", errors[0])

    def test_nonempty_or_malformed_secret_fields(self):
        for value in ("null", "false", "123", "[]", "{key: null}", "{key: 'encoded'}"):
            with self.subTest(value=value):
                path = self.file("secret.template.yaml", f"kind: Secret\ndata: {value}\n")
                self.assertTrue(checker.check_configs(self.root, [path]))

    def test_invalid_yaml_and_json(self):
        for name, content in (("bad.yaml", "kind: ["), ("bad.json", '{"bad":}'),
                              ("nonfinite.json", '{"bad": NaN}')):
            with self.subTest(name=name):
                self.assertTrue(checker.check_configs(self.root, [self.file(name, content)]))

    def test_links_anchors_and_normalized_mermaid(self):
        paths = [self.file("docs/diagrams/flow.mmd", "graph TD\n  A --> B\n"),
                 self.file("README.md", "# Start\n[doc](docs/page.md#hello-world)\n"
                           "```mermaid\ngraph TD\nA --> B\n```\n"),
                 self.file("docs/page.md", "# Hello World\n[back](../README.md#start)\n")]
        self.assertEqual(checker.check_docs(self.root, paths), [])

    def test_bad_link_anchor_fence_and_mermaid(self):
        cases = ("[missing](missing.md)", "# Present\n[missing](#absent)", "```go\nx",
                 "```mermaid\ngraph TD\nA --> C\n```\n")
        for content in cases:
            with self.subTest(content=content):
                path = self.file("README.md", content)
                self.assertTrue(checker.check_docs(self.root, [path]))

    def test_fenced_links_are_ignored_and_reference_links_checked(self):
        path = self.file("README.md", "~~~text\n[example](missing.md)\n~~~\n")
        self.assertEqual(checker.check_docs(self.root, [path]), [])
        self.file("README.md", "[doc][target]\n\n[target]: missing.md\n")
        self.assertTrue(checker.check_docs(self.root, [path]))

    def test_hygiene_filename_allowlist_does_not_read_env_example(self):
        paths = [Path(".env.example"), Path(".env"), Path("sub/.env.prod"),
                 Path("local.kubeconfig"), Path(".kube/config")]
        with patch.object(Path, "read_bytes", side_effect=AssertionError("must not read")):
            self.assertEqual(len(checker.check_hygiene(self.root, paths)), 4)

    def test_hygiene_patterns_and_development_strings(self):
        paths = [self.file("example.txt", "development-fallback"),
                 self.file("kubeconfig.template.yaml", "{}"),
                 self.file("key.txt", "-----BEGIN " + "PRIVATE KEY-----"),
                 self.file("token.txt", "ghp_" + "a" * 36)]
        errors = checker.check_hygiene(self.root, paths)
        self.assertEqual(len(errors), 2)
        self.assertTrue(all("value withheld" in error for error in errors))

    def test_git_discovery_uses_index_and_nonignored_untracked_paths(self):
        result = subprocess.CompletedProcess([], 0, stdout=b"README.md\0new.md\0README.md\0")
        with patch.object(checker.subprocess, "run", return_value=result) as run:
            self.assertEqual(checker.repository_paths(self.root), [Path("README.md"), Path("new.md")])
            self.assertIn("--exclude-standard", run.call_args.args[0])
            self.assertIn("--others", run.call_args.args[0])
            checker.repository_paths(self.root, tracked_only=True)
            self.assertNotIn("--others", run.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
