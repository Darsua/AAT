"""Validation tests for Milestone 2 Kubernetes workload manifests and deploy script."""

from pathlib import Path
import subprocess
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
KUSTOMIZE_DIR = ROOT / "k8s" / "base"

APPENDIX_B_MEMORY = {
    "aggregator": ("256Mi", "512Mi"),
    "store": ("512Mi", "1Gi"),
    "broker": ("512Mi", "1Gi"),
    "consumer": ("128Mi", "256Mi"),
    "api": ("128Mi", "256Mi"),
    "mock": ("64Mi", "128Mi"),
    "sensor": ("128Mi", "256Mi"),
    "other": ("128Mi", "256Mi"),
}


class KubernetesManifestsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        res = subprocess.run(
            ["kubectl", "kustomize", str(KUSTOMIZE_DIR)],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        cls.rendered_text = res.stdout
        cls.docs = [d for d in yaml.safe_load_all(cls.rendered_text) if isinstance(d, dict)]

    def _workloads(self):
        return [d for d in self.docs if d.get("kind") in {"Deployment", "StatefulSet", "Job"}]

    def test_kustomize_renders_workloads_and_excludes_secrets(self):
        self.assertGreaterEqual(len(self.docs), 20)
        kinds = {d.get("kind") for d in self.docs}
        for required_kind in ("Namespace", "NetworkPolicy", "LimitRange", "ConfigMap", "StatefulSet", "Deployment", "Service", "Ingress", "Job"):
            self.assertIn(required_kind, kinds)
        self.assertNotIn("Secret", kinds)

    def test_every_workload_matches_appendix_b_memory_and_role_labels(self):
        workloads = self._workloads()
        self.assertGreaterEqual(len(workloads), 12)
        for doc in workloads:
            name = doc["metadata"]["name"]
            template = doc["spec"]["template"]
            labels = template["metadata"]["labels"]
            role = labels.get("role")
            self.assertIn(role, APPENDIX_B_MEMORY, f"{name} missing valid Appendix B role label")
            expected_req, expected_lim = APPENDIX_B_MEMORY[role]
            containers = template["spec"]["containers"]
            self.assertTrue(containers, f"{name} must define containers")
            for container in containers:
                mem = container.get("resources", {})
                self.assertEqual(mem.get("requests", {}).get("memory"), expected_req, f"{name} memory request mismatch")
                self.assertEqual(mem.get("limits", {}).get("memory"), expected_lim, f"{name} memory limit mismatch")

    def test_three_node_sharded_stores_use_local_hostpath_and_distinct_nodes(self):
        stores = [
            d for d in self.docs
            if d.get("kind") == "StatefulSet"
            and d["spec"]["template"]["metadata"]["labels"].get("role") == "store"
        ]
        self.assertEqual(len(stores), 3)
        hosts = set()
        paths = set()
        for st in stores:
            spec = st["spec"]["template"]["spec"]
            host = spec.get("nodeSelector", {}).get("kubernetes.io/hostname")
            self.assertTrue(host, f"{st['metadata']['name']} missing nodeSelector hostname")
            hosts.add(host)
            host_paths = [v["hostPath"]["path"] for v in spec.get("volumes", []) if "hostPath" in v]
            self.assertEqual(len(host_paths), 1, f"{st['metadata']['name']} must mount node-local hostPath")
            paths.add(host_paths[0])
        self.assertEqual(hosts, {"aqua", "ruby", "akane"})
        self.assertEqual(len(paths), 3)

    def test_ha_aggregator_replicas_probes_and_downward_api(self):
        agg = next(
            (d for d in self.docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == "aggregator"),
            None,
        )
        self.assertIsNotNone(agg)
        self.assertEqual(agg["spec"]["replicas"], 3)
        pod_spec = agg["spec"]["template"]["spec"]
        container = pod_spec["containers"][0]
        self.assertEqual(container["readinessProbe"]["httpGet"]["path"], "/readyz")
        self.assertEqual(container["livenessProbe"]["httpGet"]["path"], "/livez")
        env_map = {e["name"]: e for e in container.get("env", [])}
        self.assertEqual(env_map["INSTANCE_ID"]["valueFrom"]["fieldRef"]["fieldPath"], "metadata.name")
        self.assertEqual(env_map["NODE_NAME"]["valueFrom"]["fieldRef"]["fieldPath"], "spec.nodeName")

    def test_autoscalers_target_stateless_paths_with_cpu_requests(self):
        hpas = {d["metadata"]["name"]: d for d in self.docs if d.get("kind") == "HorizontalPodAutoscaler"}
        self.assertEqual(set(hpas), {"client-api", "aggregator"})
        self.assertEqual(hpas["aggregator"]["spec"]["minReplicas"], 3)
        deployments = {d["metadata"]["name"]: d for d in self.docs if d.get("kind") == "Deployment"}
        for name, hpa in hpas.items():
            self.assertEqual(hpa["spec"]["scaleTargetRef"]["name"], name)
            self.assertGreater(hpa["spec"]["maxReplicas"], hpa["spec"]["minReplicas"])
            resources = deployments[name]["spec"]["template"]["spec"]["containers"][0]["resources"]
            self.assertIn("cpu", resources["requests"], f"{name} needs a CPU request for utilization scaling")

    def test_auth_single_replica_and_redpanda_pinned_hostpath(self):
        auth = next(
            (d for d in self.docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == "auth"),
            None,
        )
        self.assertIsNotNone(auth)
        self.assertEqual(auth["spec"]["replicas"], 1)

        redpanda = next(
            (d for d in self.docs if d.get("kind") == "StatefulSet" and d["metadata"]["name"] == "redpanda"),
            None,
        )
        self.assertIsNotNone(redpanda)
        pod_spec = redpanda["spec"]["template"]["spec"]
        self.assertEqual(pod_spec.get("nodeSelector", {}).get("kubernetes.io/hostname"), "ruby")
        host_paths = [v["hostPath"]["path"] for v in pod_spec.get("volumes", []) if "hostPath" in v]
        self.assertEqual(host_paths, ["/var/lib/bnpb-store/redpanda"])

    def test_deploy_script_dry_run_succeeds(self):
        res = subprocess.run(
            ["bash", str(ROOT / "scripts" / "deploy_k8s.sh"), "--dry-run"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("Dry-run OK", res.stdout)

    def test_least_privilege_secrets_and_network_policies(self):
        for doc in self._workloads():
            name = doc["metadata"]["name"]
            pod_spec = doc["spec"]["template"]["spec"]
            for container in pod_spec.get("containers", []):
                for entry in container.get("envFrom", []):
                    self.assertNotIn(
                        "secretRef",
                        entry,
                        f"{name}/{container['name']} must not mount whole secrets via envFrom",
                    )
                if name == "notifier-consumer":
                    for env in container.get("env", []):
                        self.assertNotIn(
                            "secretKeyRef",
                            env.get("valueFrom", {}),
                            "notifier-consumer must not mount secretKeyRef entries",
                        )

        policies = {d["metadata"]["name"]: d for d in self.docs if d.get("kind") == "NetworkPolicy"}
        self.assertIn("isolate-aggregator-policy", policies)

        mock_policy = policies["allow-mock-admin-policy"]
        mock_roles = next(
            expr["values"]
            for expr in mock_policy["spec"]["podSelector"].get("matchExpressions", [])
            if expr.get("key") == "role"
        )
        self.assertEqual(set(mock_roles), {"mock", "sensor"})
        for rule in mock_policy["spec"].get("ingress", []):
            for peer in rule.get("from", []):
                self.assertNotEqual(peer, {"podSelector": {}}, "allow-mock-admin-policy must not allow arbitrary pods")

        ingress_policy = policies["allow-ingress-controllers"]
        exposed_ports = {
            port["port"]
            for rule in ingress_policy["spec"].get("ingress", [])
            for port in rule.get("ports", [])
        }
        for consumer_port in (8085, 8086, 8087):
            self.assertIn(consumer_port, exposed_ports)


if __name__ == "__main__":
    unittest.main()
