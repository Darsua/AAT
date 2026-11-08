# Kubernetes Operations & Deployment Runbook (Milestone 2)

This directory contains the Kubernetes manifests, deployment configurations, and operational runbooks for the BNPB Disaster Coordination Platform on multi-node Kubernetes.

---

## 1. Cluster Architecture & Physical Topology

To qualify for the **5-point physical multi-node bonus** in Milestone 2, the cluster runs across two distinct bare-metal server chassis on the local network switch, isolated inside KVM virtual machines with separate Linux 6.12 kernels:

```
[ Public Internet: Aldoy / Jenka / Darrel ]
                   │
                   ▼ (HTTPS 443 / Cloudflare Edge TLS)
       [ Cloudflare Edge: darsu.dev ]
       ├── k8s-aat.darsu.dev (Kubernetes API)
       ├── aat.darsu.dev     (Traefik Web/HTTP)
       └── *.aat.darsu.dev   (Microservice Ingress)
                   │
                   ▼ (Cloudflare Argo Tunnel)
        [ Gateway: mainframe ]
                   │
                   ▼ (Physical LAN Switch / 1.0 ms RTT)
      ┌────────────┴────────────┐
      ▼                         ▼
[ Physical Host 1 ]       [ Physical Host 2 ]
Host: runner-1            Host: nexus
IP: 10.5.24.115           IP: 10.5.24.118
Debian 13 trixie          Debian 13 trixie
      │                         │
      ▼ (KVM Isolation)         ▼ (KVM Isolation)
[ Node: aqua ]            [ Node: ruby ]
IP: 10.5.27.240           IP: 10.5.27.241
Role: Control Plane       Role: Worker Node
2 vCPU | 3.5 GB RAM       2 vCPU | 3.5 GB RAM
      │                         │
┌─────┴─────────────────────────┴─────┐
│       K3s v1.37.2+k3s1 Multi-Node   │
│  CNI: Flannel (10.42.0.0/16)        │
│  Storage: local-path-provisioner    │
│  Ingress: Traefik (Ports 80/443)    │
│  Chaos: sustuna/tester-agent (DS)   │
└─────────────────────────────────────┘
```

### Safety & Sandboxing Guarantees
* **Chaos Containment:** The official `sustuna/tester-agent` runs with `hostPath: /` mounted to `/host`. Because the nodes are KVM virtual machines, node wipes (`wipeAndShutdown`), host reboots, and clock skews (`shiftClock`) are completely contained within `aqua` and `ruby`. Bare-metal hosts and personal workstations remain 100% protected.
* **Auto-Recovery:** Both VMs run under systemd (`aat-aqua.service` and `aat-ruby.service`) and automatically start on physical machine boot.
* **Disaster Recovery:** A golden state snapshot (`golden`) is preserved on both hypervisors for instant 2-second rollback if a chaos test zeroes out partitions.

---

## 2. Team Remote Access (For Aldoy, Jenka & Darrel)

No VPN, WireGuard, or SSH tunnels are required to access the cluster.

### Step 1: Create Your Local Kubeconfig
1. Copy the sanitized template from this repository:
   ```bash
   cp k8s/kubeconfig.template.yaml ~/.kube/config-team17.yaml
   ```
2. Replace `<PASTE_TEAM17_BEARER_TOKEN_HERE>` in your copy with the team bearer token shared over our private channel.

### Step 2: Set Kubeconfig in Your Shell
```bash
# In Bash/Zsh:
export KUBECONFIG=~/.kube/config-team17.yaml

# In Fish:
set -x KUBECONFIG ~/.kube/config-team17.yaml
```
*(Or append `--kubeconfig ~/.kube/config-team17.yaml` to your kubectl commands).*

### Step 3: Verify Connection
```bash
# Check physical nodes
kubectl get nodes -o wide

# Check core system services
kubectl get pods -A

# Check our application namespace
kubectl get all -n sistem-informasi-kebencanaan-bnpb
```

---

## 3. Public Ingress & Endpoints

| Service | Public URL | Upstream Origin | Protocol |
| :--- | :--- | :--- | :--- |
| **Kubernetes API** | `https://k8s-aat.darsu.dev` | `https://10.5.27.240:6443` | HTTPS / mTLS Token |
| **BNPB Hazard Web / API** | `https://aat.darsu.dev` | `http://10.5.27.240:80` | HTTP/2 Traefik Ingress |
| **Wildcard Preview Services** | `https://*.aat.darsu.dev` | `http://10.5.27.240:80` | HTTP/2 Traefik Ingress |

---

## 4. Disaster Simulation & Chaos Testing Runbook

The official tester agent is pre-deployed across both nodes as a privileged DaemonSet in namespace `tester-agent`.

### A. List Supported Chaos Scenarios
```bash
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app list
```
* `clock-skew` — Shifts kernel clock to validate leader lease expiration and token TTLs.
* `kill-leader` — Terminates the active Aggregator leader pod to verify election failover.
* `kill-store` — Terminates primary database instances to verify replica promotion.
* `partition` — Injects `iptables` drop rules between `aqua` and `ruby` to simulate network split.
* `schema` — Tests schema evolution handling and Dead Letter Queue (DLQ) ingestion.
* `wipe-node` — Emulates complete node wipe and triggers VM reboot.

### B. Practice Scenarios
```bash
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app run practice <scenario-name>

# Example: test Aggregator leader failover
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app run practice kill-leader

# Example: test network partition tolerance
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app run practice partition
```

### C. Official Exam Scenarios (Demo & Grading)
```bash
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app run exam <p1|p2|p3|p5>

# Problem 1: Sharding & Node Wipe
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app run exam p1

# Problem 2: Aggregator High Availability & Clock Skew
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app run exam p2

# Problem 3: Dead Letter Queue & Schema Evolution
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app run exam p3

# Problem 5: Coastal Sensor Circuit Breaking
kubectl exec -n tester-agent daemonset/tester-agent -c agent -- /app run exam p5
```
*(Note: Problem 4 is streaming satellite DBSCAN clustering, validated via offline benchmark scripts).*

---

## 5. Milestone 2 Directory Structure

Manifests for the upcoming sprint should be organized under this directory as follows:

```
k8s/
├── README.md                      # This operations runbook
├── kubeconfig.template.yaml       # Sanitized connection template
├── base/                          # Namespaces, RBAC, common ConfigMaps
├── storage/                       # StorageClasses & Local PersistentVolumes
├── database/                      # Sharded PostgreSQL StatefulSets & Services
├── messaging/                     # Redpanda cluster manifests
├── apps/                          # Aggregator (HA deployment), Auth, Client API
└── consumers/                     # Dashboard & Notifier consumers
```
