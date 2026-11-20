# Kubernetes operations

**Status:** `base/` defines the M2 foundation policies, ConfigMap/Secret templates, 3-node sharded storage StatefulSets (`store-aqua`, `store-ruby`, `store-akane`), Redpanda broker + topic init job, 3-replica HA Aggregator Deployment, API/consumer/mock workloads, and Traefik Ingress. Provision cluster credentials (`bnpb-credentials`) before running the one-command deployment (`./scripts/deploy_k8s.sh` or `make k8s-deploy`). Live cluster isolation, failover, and load certification still require execution on the 3-node separate-kernel cluster. Use [Compose for the runnable M1 baseline](../docs/development.md).

Infrastructure and application work is tracked in [GitHub issues](https://github.com/Darsua/AAT/issues), including [foundation #22](https://github.com/Darsua/AAT/issues/22), [storage #23](https://github.com/Darsua/AAT/issues/23), and [HA #25](https://github.com/Darsua/AAT/issues/25). Authoring manifests does not certify the running cluster until exercised live under `#31`.

## Documented environment

The cluster runs on three isolated QEMU/KVM virtual machines hosted across separate bare-metal Lab Sister nodes on `10.5.24.0/22` (running Debian 13, K3s `v1.37.2+k3s1`, and `containerd://2.3.7-k3s1`):

| Kubernetes node | Physical host (`10.5.24.x`) | Host hardware & service unit | Guest role & OS/kernel | Guest resources & IP |
| :--- | :--- | :--- | :--- | :--- |
| `aqua` | `nexus` (`10.5.24.111`) | Intel Core i5-10500T (6C/12T), 8 GB RAM, 1 TB HDD (`aat-aqua.service`) | Control plane and worker; Debian 13 (`6.12.117+deb13-cloud-amd64`) | 2 vCPU, 3.5 GB RAM, 40 GB disk (`10.5.27.240`) |
| `ruby` | `runner-1` (`10.5.24.114`) | Intel Core i5-10500T (6C/12T), 8 GB RAM, 1 TB HDD (`aat-ruby.service`) | Non-control-plane worker; Debian 13 (`6.12.117+deb13-cloud-amd64`) | 2 vCPU, 3.5 GB RAM, 40 GB disk (`10.5.27.241`) |
| `akane` | `runner-2` (`10.5.24.115`) | Intel Core i5-10500T (6C/12T), 8 GB RAM, 1 TB HDD (`aat-akane.service`) | Second worker (P1 3-node sharding); Debian 13 (`6.12.115+deb13.1-cloud-amd64`) | 2 vCPU, 3.5 GB RAM (`3.3 GiB` usable), 40 GB disk (`10.5.27.242`) |

Supporting lab infrastructure used by this deployment:

- **`mainframe` (`10.5.24.112`)**: Intel Core i5-10500T, 8 GB RAM, Debian 13; acts as the Cloudflare Zero Trust SSH bastion (`ssh.sister.works`), Tunnel gateway routing `k8s-aat.darsu.dev` (Kubernetes API) and `aat.darsu.dev` / `audit-aat.darsu.dev` (application ingress), and LAN egress proxy (`tinyproxy` `:8888`, providing WARP-backed outbound HTTPS for cluster VMs such as `aggregator`'s satellite feed).
- **`langley` / `imac-gpu` (`10.5.24.125`)**: Intel Core iMac with AMD Radeon Pro 5500 XT (8 GiB VRAM, macOS); runs `llama-server` (`works.sister.llama-server`, Vulkan/MoltenVK) serving `Qwen3.5-9B-Q4_K_M` (`qwen3.5:9b`) on `:11434`, reachable over the `10.5.24.0/22` LAN by `summarizer-consumer` (`#36`) and externally via Cloudflare Access at `https://ai.sister.works`.
- **Golden VM images**: All three guest VMs maintain `vm-<name>-golden.qcow2` base snapshots on their physical hosts for pre-test preparation only; snapshots are never restored during an assessed self-recovery window.

### Sharded storage and one-command deployment

- `storage.yaml` places one `role=store` StatefulSet per node (`store-aqua`, `store-ruby`, `store-akane`), each backed by node-local `hostPath` disk (`/var/lib/bnpb-store/store-<node>`).
- `SHARD_CONFIG` distributes 3 logical shards with cross-node replication (`shard-1` primary `aqua` / replica `ruby`, `shard-2` primary `ruby` / replica `akane`, `shard-3` primary `akane` / replica `aqua`), so no single node stores the entire dataset while surviving a single worker loss. Override `STORE_NODE_1..3` in `bnpb-config` if hostnames change.
- Validate rendered manifests locally with `./scripts/deploy_k8s.sh --dry-run` (or `make k8s-validate`).
- Deploy the complete initial stack in one command (provisioning `bnpb-credentials` from `.env`/environment if absent) with `./scripts/deploy_k8s.sh` (or `make k8s-deploy`).

The documented access path is Cloudflare Tunnel through gateway `mainframe`, with `k8s-aat.darsu.dev` for the Kubernetes API and `aat.darsu.dev` / `audit-aat.darsu.dev` for application ingress. DNS/tunnel availability does not establish that an application is deployed. TLS and bearer authentication are distinct; do not describe bearer-token access as mutual TLS.

### Storage and topology constraints

- Demo nodes require separate kernels; shared-kernel multi-node container clusters are development-only.
- `role=store` data must remain on the pod's node-local disk (local PV or hostPath), not network/external storage that bypasses node-loss testing.
- **In the 3-node cluster, destructive worker scenarios target non-control-plane workers (`ruby` or `akane`), never the control plane (`aqua`).** A selector resolving to an arbitrary DaemonSet pod is not enough to establish a safe target.
- P1 also requires no node to store the whole dataset, lossless worker-wipe recovery, redistribution without downtime, and restored tolerance of a subsequent loss. Two-node full-copy replication is not automatically a valid solution. Resolve placement/failure assumptions under #23 before claiming compliance.
- Check scheduling headroom against the resource table below, including storage replicas, broker, >=3 Aggregators, system components, and the unrestricted Tester Agent. Current VM sizes are not a capacity proof.

## Team access and read-only checks

Keep the real kubeconfig and token outside the repository. From the repository root, create a private copy without replacing an existing config:

```bash
mkdir -p ~/.kube
cp -n k8s/kubeconfig.template.yaml ~/.kube/config-team17.yaml
chmod 600 ~/.kube/config-team17.yaml
export KUBECONFIG=~/.kube/config-team17.yaml
```

Populate the bearer-token placeholder through the team's private credential channel. Do not commit it, disable TLS verification, or print it in shared logs. In Fish, set the environment with `set -x KUBECONFIG ~/.kube/config-team17.yaml` instead.

```bash
kubectl get nodes -o wide
kubectl get pods -A
kubectl get all -n sistem-informasi-kebencanaan-bnpb
kubectl get networkpolicy,limitrange -n sistem-informasi-kebencanaan-bnpb
```

These commands inspect a live cluster; they do not deploy or repair it. This documentation cleanup has not run them.

## Workload conventions

Application pods belong to `sistem-informasi-kebencanaan-bnpb` and carry a `role` label from `aggregator`, `store`, `broker`, `consumer`, `api`, `mock`, `sensor`, or `other`. Use ConfigMaps for configuration and Secrets for credentials. Empty Secret templates are not provisioned credentials, and a ConfigMap key is not proof that a process consumes it.

Declare **memory requests and limits per workload/container according to its component class**. CPU allocation is optional under the course specification; generic LimitRange defaults alone do not satisfy the class-specific memory allocations.

| Component class | Memory request | Memory limit |
| :--- | :--- | :--- |
| Aggregator | `256Mi` | `512Mi` |
| Storage instance | `512Mi` | `1Gi` |
| Broker or stream processor | `512Mi` | `1Gi` |
| Consumer or event processor | `128Mi` | `256Mi` |
| Auth / Client API | `128Mi` | `256Mi` |
| BMKG / PVMBG mock | `64Mi` | `128Mi` |
| Other components | `128Mi` | `256Mi` |

Load generators run outside application component pods. The course table has no separate sensor row; use the assistant's prescribed sensor manifest rather than inventing an image/resource override.

Only Aggregator application pods may access canonical PostgreSQL. Store-to-store replication is separate infrastructure traffic. Verify enforcement from allowed and denied pods with the actual CNI; policy YAML alone is not proof. Preserve Tester Agent access. Broker policies permit `role=aggregator`, `role=consumer`, `role=broker`, and `role=api` (for `POST /field-reports` staging under #27) without opening canonical storage to the gateway.

`kustomization.yaml` excludes `secrets.template.yaml` so `kubectl apply -k k8s/base` cannot overwrite provisioned cluster credentials with empty placeholders. `allow-ingress-controllers` permits port `8084` (`auth`) alongside `8080`, `8083`, and `3000`; `bnpb-ingress` routes `/auth` (`auth:8084`), `/dashboard` (`dashboard-consumer:8085`), `/summaries` (`summarizer-consumer:8087`), and `/` (`client-api:8080`) on `aat.darsu.dev`, plus `/internal/audit` (`aggregator:8083`) on `audit-aat.darsu.dev`. `bnpb-config` and `secrets.template.yaml` include the runtime keys read directly by the Go services (`AUTH_URL`, `TOPIC`, `POLL_INTERVAL_SECS`, `JWT_TTL_SECONDS`, `AUTH_*_CLIENT_*`, `CONSUMER_GROUP_*`, `BMKG_KEY`, `PVMBG_TOKEN`, `AI_API_URL`, `AI_MODEL`, `AI_API_KEY`) alongside M2 keys. `summarizer-consumer` runs on port `8087` (`128Mi`/`256Mi`). Observability scrape/trace paths and M2 multi-topic routing still need integration verification as workloads land.

## Autoscaling (bonus, #37)

[`autoscaling.yaml`](base/autoscaling.yaml) adds CPU-based HorizontalPodAutoscalers for the stateless request paths only: `client-api` (2 to 6 replicas) and `aggregator` (3 to 5, keeping the P2 minimum). Both target 70% of their CPU request (`client-api` 100m, `aggregator` 250m; the CPU limit is the LimitRange default 500m) and scale up at once but down slowly (one pod per 60/90 s after a 120/180 s window) so draining pods finish in-flight work. Auth (process-local refresh state) and the Kafka consumers (3 partitions) are not scaled. The HPAs need the Kubernetes metrics API; k3s ships `metrics-server`, and `deploy_k8s.sh` warns when it is missing.

- Watch decisions: `kubectl get hpa -n sistem-informasi-kebencanaan-bnpb -w` and `kubectl describe hpa aggregator -n sistem-informasi-kebencanaan-bnpb`.
- The P2 drill scales the Aggregator by hand (3 -> 5 -> 2), which the HPA would undo. Pause it first with `kubectl delete hpa aggregator -n sistem-informasi-kebencanaan-bnpb` and restore it afterwards with `kubectl apply -k k8s/base`.
- Re-applying the kustomization resets `spec.replicas` to the manifest value until the HPA's next decision.

Scale-up/scale-down evidence still has to be captured on the cluster under load; these manifests are not proof by themselves.

## Tester Agent and destructive testing

The prescribed image is:

```text
sustuna/tester-agent@sha256:b3143da159cf23a8744ede12b847da6208353754234d53a5a7b46d071cf277ec
```

Installation is a **privileged cluster change**, not a documentation-validation step. When explicitly authorized for the demo cluster, the specified installation command is:

```bash
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
```

Do not change its image, stop it, or impede it through RBAC, NetworkPolicy, admission/Pod Security, taints, or resource restrictions. Verify it runs on every required node; installation/status is not asserted by this repository.

Discover supported scenarios from the installed pinned agent rather than assuming an unverified exam CLI:

```bash
kubectl exec -n tester-agent daemonset/tester-agent -- /app list
kubectl logs -n tester-agent daemonset/tester-agent
```

**Practice scenarios are destructive and require explicit authorization and target verification.** The specified invocation form is `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <name>`; use the installed agent's supported targeting mechanism to ensure two-node node-disruption scenarios affect only the worker. Do not assume the location of the exec session selects the affected node.

Official exam keys, audit credentials, and test resources arrive through the assistants. Follow those instructions; do not invent exam-key flags or run exams without keys. Satellite P4 additionally requires live-feed processing during the recorded demo and full reference comparison, not just an offline clustering benchmark.

VM isolation reduces risk but is not a guarantee of host safety: review disk passthrough, host mounts, networks, and privileges before testing. Backups/snapshots are preparation tools, **not permitted intervention during an assessed self-recovery window**. Neither automatic VM startup nor a snapshot proves application recovery time. Do not restore snapshots, repair data, restart components manually, or change code during that window.

## Verification boundary

A complete M2 deployment must eventually start from a documented initial state with one command, resolve secrets/configuration, install application workloads and ingress, and survive the required recovery/load scenarios. Until implemented and measured, this runbook is not a deployment or bonus certification. Update it from reproducible evidence as work lands; keep pending acceptance criteria in their GitHub issues.
