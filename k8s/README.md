# Kubernetes operations

**Status:** this repository contains M2 **foundation resources**, not a deployable Kubernetes application. `base/` defines a namespace, network policies, a LimitRange, and ConfigMap/Secret templates; it contains no application workloads, database/broker deployments, Ingress, or Tester Agent installation. `kubectl apply -k k8s/base` is therefore **not** the required one-command deployment. Use [Compose for the runnable M1 baseline](../docs/development.md).

Infrastructure and application work is tracked in [GitHub issues](https://github.com/Darsua/AAT/issues), including [foundation #22](https://github.com/Darsua/AAT/issues/22), [storage #23](https://github.com/Darsua/AAT/issues/23), and [HA #25](https://github.com/Darsua/AAT/issues/25). Closing the foundation issue does not certify the running cluster.

## Documented environment

The existing team topology records two KVM VMs on separate physical hosts. This is a configuration record, **not a fresh live-cluster verification**:

| Kubernetes node | Physical host | Node role | VM resources | Node address |
| :--- | :--- | :--- | :--- | :--- |
| `aqua` | `runner-1` | Control plane and worker | 2 vCPU, 3.5 GB RAM | `10.5.27.240` |
| `ruby` | `nexus` | Non-control-plane worker | 2 vCPU, 3.5 GB RAM | `10.5.27.241` |

The physical hosts' CPU/RAM specifications still need to be recorded; VM allocations do not substitute for them. Verify Kubernetes/kernel versions, actual node capacity, storage placement, and reachability from the environment rather than relying on the former unverified version/latency claims.

The documented access path is Cloudflare Tunnel through gateway `mainframe`, with `k8s-aat.darsu.dev` for the Kubernetes API and `aat.darsu.dev` / `*.aat.darsu.dev` for application ingress. DNS/tunnel availability does not establish that an application is deployed. TLS and bearer authentication are distinct; do not describe bearer-token access as mutual TLS.

### Storage and topology constraints

- Demo nodes require separate kernels; shared-kernel multi-node container clusters are development-only.
- `role=store` data must remain on the pod's node-local disk (local PV or hostPath), not network/external storage that bypasses node-loss testing.
- **With two nodes, destructive node scenarios target only `ruby`, not the control plane.** A selector resolving to an arbitrary DaemonSet pod is not enough to establish a safe target.
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

`kustomization.yaml` excludes `secrets.template.yaml` so `kubectl apply -k k8s/base` cannot overwrite provisioned cluster credentials with empty placeholders. `allow-ingress-controllers` permits port `8084` (`auth`) alongside `8080`, `8083`, and `3000`; `bnpb-config` and `secrets.template.yaml` include the runtime keys read directly by the Go services (`AUTH_URL`, `TOPIC`, `POLL_INTERVAL_SECS`, `JWT_TTL_SECONDS`, `AUTH_*_CLIENT_*`, `CONSUMER_GROUP_*`, `BMKG_KEY`, `PVMBG_TOKEN`) alongside M2 keys. Observability scrape/trace paths and M2 multi-topic routing still need integration verification as workloads land.

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
