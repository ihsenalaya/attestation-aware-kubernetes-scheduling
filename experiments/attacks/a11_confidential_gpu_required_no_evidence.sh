#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# A11 — confidential GPU required, no GPU attestation evidence.
#
# Article 1 does not claim confidential GPU attestation. The safe behavior is
# fail-closed: a policy with requireConfidentialGPU=true must not admit a pod
# until a real GPU/DRA attestation path exists.
set -uo pipefail

ENV_NAME="${ENV_NAME:-kind-live-simulated}"
NS="${NS:-article1-a11-gpu}"
OUT_DIR="${OUT_DIR:-results/raw/${ENV_NAME%%-*}}"
CSV="${CSV:-${OUT_DIR}/a11_gpu_required_no_evidence.csv}"
if [[ "${ENV_NAME}" == aks* ]]; then
  RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-false}"
else
  RUNTIME_CLASS="${RUNTIME_CLASS:-simulated-kata-qemu-snp}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-true}"
fi
mkdir -p "${OUT_DIR}"

echo "timestamp,env,attack_id,scenario,expected,actual,blocked,status,reason,raw_log_path" > "${CSV}"

record() {
  printf '%s,%s,A11,%s,BLOCKED,%s,%s,%s,%s,%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "${ENV_NAME}" \
    "$1" \
    "$2" \
    "$3" \
    "$4" \
    "$5" \
    "$6" >> "${CSV}"
}

if ! kubectl version --client >/dev/null 2>&1; then
  record "gpu-required-no-evidence" "kubectl-unavailable" "no" "NOT_EXECUTED" "kubectl_not_found" ""
  exit 0
fi

raw="${OUT_DIR}/A11-gpu-required-no-evidence.log"
set +e
kubectl get nodes > "${raw}" 2>&1
cluster_rc=$?
set -e
if [ "${cluster_rc}" -ne 0 ]; then
  record "gpu-required-no-evidence" "cluster-unavailable" "no" "NOT_EXECUTED" "no_reachable_cluster" "${raw}"
  exit 0
fi

set +e
kubectl delete namespace "${NS}" --ignore-not-found --wait=true >> "${raw}" 2>&1
cleanup_rc=$?
set -e
if [ "${cleanup_rc}" -ne 0 ]; then
  record "gpu-required-no-evidence" "cleanup-error" "no" "ERROR" "namespace_cleanup_failed" "${raw}"
  exit 1
fi

kubectl create namespace "${NS}" >> "${raw}" 2>&1
kubectl label namespace "${NS}" ai.sovereign.io/sensitivity=high --overwrite >> "${raw}" 2>&1
kubectl apply -f - >> "${raw}" 2>&1 <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: ConfidentialInferencePolicy
metadata:
  name: gpu-required
  namespace: ${NS}
spec:
  target:
    namespaceSelector:
      matchLabels:
        ai.sovereign.io/sensitivity: high
    workloadSelector:
      matchLabels:
        app: gpu-victim
  requiredTEE: ["SEV-SNP"]
  requireConfidentialContainers: ${REQUIRE_CONFIDENTIAL_CONTAINERS}
  allowedRuntimeClasses: ["${RUNTIME_CLASS}"]
  requireConfidentialGPU: true
  gpu:
    vendor: nvidia
    deviceClass: confidential
  maxEvidenceAgeSeconds: 300
  requireModelDigest: true
  enforcementMode: enforce
EOF

set +e
apply_out="$(kubectl apply -f - 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: gpu-victim
  namespace: ${NS}
  labels:
    app: gpu-victim
  annotations:
    ai.sovereign.io/model-digest: "sha256:a11"
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${RUNTIME_CLASS}
  containers:
  - name: app
    image: registry.k8s.io/pause:3.9
EOF
)"
apply_rc=$?
set -e
printf '%s\nrc=%s\n' "${apply_out}" "${apply_rc}" >> "${raw}"

if [ "${apply_rc}" -ne 0 ] && printf '%s\n' "${apply_out}" | grep -qi "confidential GPU attestation is not implemented"; then
  record "gpu-required-no-evidence" "DENIED" "yes" "EXECUTED" "webhook_fail_closed_for_gpu_future_scope" "${raw}"
  exit 0
fi

record "gpu-required-no-evidence" "ADMITTED_OR_WRONG_DENIAL" "no" "FAIL" "gpu_required_workload_not_fail_closed" "${raw}"
exit 1
