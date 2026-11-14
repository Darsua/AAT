#!/usr/bin/env bash
# One-command deployment and manifest validation script for k8s/base.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
KUSTOMIZE_DIR="${REPO_ROOT}/k8s/base"
NAMESPACE="sistem-informasi-kebencanaan-bnpb"
DRY_RUN=false

for arg in "$@"; do
  case "${arg}" in
    --dry-run)
      DRY_RUN=true
      ;;
    -h|--help)
      echo "Usage: $0 [--dry-run]"
      echo "  --dry-run  Render and validate k8s/base manifests locally via kubectl kustomize."
      exit 0
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 1
      ;;
  esac
done

command -v kubectl >/dev/null 2>&1 || {
  echo "[k8s-deploy] ERROR: kubectl is required on PATH" >&2
  exit 1
}

RENDERED="$(kubectl kustomize "${KUSTOMIZE_DIR}")"
DOC_COUNT="$(printf '%s\n' "${RENDERED}" | grep -c '^kind: ' || true)"
if [ "${DOC_COUNT}" -lt 20 ]; then
  echo "[k8s-deploy] ERROR: expected >=20 rendered Kubernetes resources, got ${DOC_COUNT}" >&2
  exit 1
fi
if printf '%s\n' "${RENDERED}" | grep -q '^kind: Secret$'; then
  echo "[k8s-deploy] ERROR: rendered kustomization unexpectedly includes a Secret template" >&2
  exit 1
fi

if [ "${DRY_RUN}" = true ]; then
  echo "[k8s-deploy] Dry-run OK: rendered ${DOC_COUNT} resources from ${KUSTOMIZE_DIR}"
  exit 0
fi

# Optionally source local .env without overriding already-exported shell variables.
if [ -f "${REPO_ROOT}/.env" ]; then
  while IFS= read -r line || [ -n "${line}" ]; do
    case "${line}" in
      ''|\#*) continue ;;
    esac
    key="${line%%=*}"
    val="${line#*=}"
    if [ -z "${!key+x}" ]; then
      export "${key}=${val}"
    fi
  done < "${REPO_ROOT}/.env"
fi

echo "[k8s-deploy] Applying Namespace foundation..."
kubectl apply -f "${KUSTOMIZE_DIR}/namespace.yaml" >/dev/null

if ! kubectl get secret bnpb-credentials -n "${NAMESPACE}" >/dev/null 2>&1; then
  required_vars=(
    JWT_SECRET
    AUDIT_TOKEN
    POSTGRES_USER
    POSTGRES_PASSWORD
    POSTGRES_DB
    BMKG_API_KEY
    PVMBG_TOKEN
    AUTH_MEDIA_CLIENT_SECRET
    AUTH_FIELD_CLIENT_SECRET
    AUTH_INTERNAL_CLIENT_SECRET
  )
  for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
      echo "[k8s-deploy] ERROR: secret bnpb-credentials is absent and required env var ${var} is unset" >&2
      exit 1
    fi
  done

  echo "[k8s-deploy] Provisioning bnpb-credentials Secret from environment..."
  kubectl create secret generic bnpb-credentials \
    -n "${NAMESPACE}" \
    --from-literal=JWT_SECRET="${JWT_SECRET}" \
    --from-literal=SATELLITE_GROUP_TOKEN="${SATELLITE_GROUP_TOKEN:-unconfigured-satellite-token}" \
    --from-literal=PANTAI_AUTH_TOKEN="${PANTAI_AUTH_TOKEN:-unconfigured-pantai-token}" \
    --from-literal=AUDIT_TOKEN="${AUDIT_TOKEN}" \
    --from-literal=POSTGRES_USER="${POSTGRES_USER}" \
    --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    --from-literal=POSTGRES_DB="${POSTGRES_DB}" \
    --from-literal=BMKG_API_KEY="${BMKG_API_KEY}" \
    --from-literal=BMKG_KEY="${BMKG_KEY:-${BMKG_API_KEY}}" \
    --from-literal=PVMBG_BEARER_TOKEN="${PVMBG_BEARER_TOKEN:-${PVMBG_TOKEN}}" \
    --from-literal=PVMBG_TOKEN="${PVMBG_TOKEN}" \
    --from-literal=AUTH_MEDIA_CLIENT_SECRET="${AUTH_MEDIA_CLIENT_SECRET}" \
    --from-literal=AUTH_FIELD_CLIENT_SECRET="${AUTH_FIELD_CLIENT_SECRET}" \
    --from-literal=AUTH_INTERNAL_CLIENT_SECRET="${AUTH_INTERNAL_CLIENT_SECRET}" \
    --dry-run=client -o yaml | kubectl apply -f -
else
  echo "[k8s-deploy] Preserving existing bnpb-credentials Secret in ${NAMESPACE}."
fi

kubectl delete job redpanda-topic-init -n "${NAMESPACE}" --ignore-not-found >/dev/null

echo "[k8s-deploy] Applying kustomization ${KUSTOMIZE_DIR} (${DOC_COUNT} resources)..."
kubectl apply -k "${KUSTOMIZE_DIR}"

echo "[k8s-deploy] Waiting for storage and broker StatefulSets..."
kubectl rollout status statefulset/store-aqua -n "${NAMESPACE}" --timeout=180s
kubectl rollout status statefulset/store-ruby -n "${NAMESPACE}" --timeout=180s
kubectl rollout status statefulset/store-kana -n "${NAMESPACE}" --timeout=180s
kubectl rollout status statefulset/redpanda -n "${NAMESPACE}" --timeout=180s
kubectl wait --for=condition=complete job/redpanda-topic-init -n "${NAMESPACE}" --timeout=180s

echo "[k8s-deploy] Waiting for application Deployments..."
for deploy in mock-bmkg mock-pvmbg aggregator auth client-api dashboard-consumer notifier-consumer; do
  kubectl rollout status "deployment/${deploy}" -n "${NAMESPACE}" --timeout=180s
done

echo "[k8s-deploy] Deployment complete in namespace ${NAMESPACE}."
kubectl get pods -n "${NAMESPACE}" -o wide
