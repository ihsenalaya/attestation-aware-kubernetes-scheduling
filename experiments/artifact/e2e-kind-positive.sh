#!/usr/bin/env bash
# Resolve bundled paths independently of the caller working directory.
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ARTIFACT_ROOT}"
# e2e-kind-positive.sh — Article 1 positive end-to-end scenario.
#
# On kind this is SIMULATED and regression-only. With ENV_NAME=aks-real-sevsnp
# it requires real central-verifier evidenceMode=real and writes AKS raw evidence.
#
# Flow:
#   node-agent -> RawAttestationReport -> central-verifier AttestationEvidence
#   -> namespace -> ConfidentialInferencePolicy
#   -> sensitive pod -> ai-attestation-scheduler binds -> AIPlacementDecision
#   -> verify-placement PASS
#
# Requires: a running kind cluster with the confidential platform installed,
# kubectl context set, and the verify-placement binary path in VERIFY_BIN.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_LIB="${SCRIPT_DIR}/../harness/lib.sh"
[ -f "${HARNESS_LIB}" ] && source "${HARNESS_LIB}"

NS="${NS:-article1-e2e}"
PLATFORM_NS="${PLATFORM_NS:-ai-platform}"
VERIFY_BIN="${VERIFY_BIN:-verify-placement}"
OUT_DIR="${OUT_DIR:-results/raw/kind}"
ENV_NAME="${ENV_NAME:-kind-live-simulated}"
if [[ "${ENV_NAME}" == aks* ]]; then
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-real}"
  REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
  RUNTIME_CLASS="${RUNTIME_CLASS:-runc}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-false}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-true}"
  if declare -F detect_verified_node >/dev/null 2>&1; then
    NODE="${NODE:-$(detect_verified_node "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"
    EVIDENCE="${EVIDENCE:-$(detect_verified_evidence_name "${REQUIRED_TEE}" "${EXPECTED_EVIDENCE_MODE}" 2>/dev/null || true)}"
  fi
else
  EXPECTED_EVIDENCE_MODE="${EXPECTED_EVIDENCE_MODE:-simulated}"
  REQUIRED_TEE="${REQUIRED_TEE:-SEV-SNP}"
  RUNTIME_CLASS="${RUNTIME_CLASS:-simulated-kata-qemu-snp}"
  REQUIRE_CONFIDENTIAL_CONTAINERS="${REQUIRE_CONFIDENTIAL_CONTAINERS:-true}"
  TOLERATE_CONFIDENTIAL_NODES="${TOLERATE_CONFIDENTIAL_NODES:-false}"
fi
NODE="${NODE:-ai-platform-control-plane}"
EVIDENCE="${EVIDENCE:-evidence-${NODE}}"
mkdir -p "${OUT_DIR}"

TOLERATIONS_YAML=""
if [ "${TOLERATE_CONFIDENTIAL_NODES}" = "true" ]; then
  TOLERATIONS_YAML='  tolerations:
    - key: ai.sovereign.io/confidential
      operator: Equal
      value: "true"
      effect: NoSchedule'
fi

echo "== [e2e] cleaning any prior namespace"
set +e
kubectl delete namespace "${NS}" --ignore-not-found --wait=true >/dev/null 2>&1
delete_rc=$?
set -e
if [ "${delete_rc}" -ne 0 ]; then
  echo "FAIL: could not delete prior namespace ${NS}" >&2
  exit 1
fi

echo "== [e2e] creating namespace ${NS}"
kubectl create namespace "${NS}"
kubectl label namespace "${NS}" ai.sovereign.io/sensitivity=high --overwrite

echo "== [e2e] applying ConfidentialInferencePolicy"
kubectl apply -f - <<EOF
apiVersion: aiops.imperium.io/v1alpha1
kind: ConfidentialInferencePolicy
metadata:
  name: e2e-policy
  namespace: ${NS}
spec:
  target:
    namespaceSelector:
      matchLabels:
        ai.sovereign.io/sensitivity: high
    workloadSelector:
      matchLabels:
        app: risk-assistant
  requiredTEE: ["${REQUIRED_TEE}"]
  requireConfidentialContainers: ${REQUIRE_CONFIDENTIAL_CONTAINERS}
  allowedRuntimeClasses: ["${RUNTIME_CLASS}"]
  maxEvidenceAgeSeconds: 300
  requireImageDigest: false
  requireModelDigest: true
  enforcementMode: enforce
EOF

echo "== [e2e] waiting for central-verifier evidence (${EXPECTED_EVIDENCE_MODE})"
for i in $(seq 1 30); do
  set +e
  verified=$(kubectl get attestationevidence "${EVIDENCE}" -n "${PLATFORM_NS}" -o jsonpath='{.status.verified}' 2>/dev/null)
  mode=$(kubectl get attestationevidence "${EVIDENCE}" -n "${PLATFORM_NS}" -o jsonpath='{.status.evidenceMode}' 2>/dev/null)
  set -e
  if [ "${verified}" = "true" ] && [ "${mode}" = "${EXPECTED_EVIDENCE_MODE}" ]; then break; fi
  sleep 2
done
kubectl get attestationevidence "${EVIDENCE}" -n "${PLATFORM_NS}" -o yaml > "${OUT_DIR}/e2e-evidence.yaml"
if [ "${verified}" != "true" ] || [ "${mode}" != "${EXPECTED_EVIDENCE_MODE}" ]; then
  echo "FAIL: central-verifier evidence not verified/${EXPECTED_EVIDENCE_MODE}" >&2
  exit 1
fi

echo "== [e2e] creating sensitive pod (evidence annotation set to bypass gate)"
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: risk-assistant
  namespace: ${NS}
  labels:
    app: risk-assistant
  annotations:
    ai.sovereign.io/model-digest: "sha256:model-e2e"
    ai.sovereign.io/attestation-evidence: "${EVIDENCE}"
spec:
  schedulerName: ai-attestation-scheduler
  runtimeClassName: ${RUNTIME_CLASS}
${TOLERATIONS_YAML}
  containers:
    - name: app
      image: registry.k8s.io/pause:3.9
EOF

echo "== [e2e] waiting for pod to be scheduled (bound to a node)"
for i in $(seq 1 30); do
  node=$(kubectl get pod risk-assistant -n "${NS}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo "")
  if [ -n "${node}" ]; then break; fi
  sleep 2
done
kubectl get pod risk-assistant -n "${NS}" -o yaml > "${OUT_DIR}/e2e-pod.yaml"
if [ -z "${node}" ]; then
  echo "FAIL: pod was not bound to any node" >&2
  kubectl describe pod risk-assistant -n "${NS}" >&2
  exit 1
fi
echo "   pod bound to node: ${node}"

echo "== [e2e] waiting for AIPlacementDecision"
DECISION="risk-assistant-${NS}"
DECISION="${DECISION:0:63}"
for i in $(seq 1 30); do
  if kubectl get aiplacementdecision "${DECISION}" -n "${NS}" >/dev/null 2>&1; then break; fi
  sleep 2
done
kubectl get aiplacementdecision "${DECISION}" -n "${NS}" -o yaml > "${OUT_DIR}/e2e-placement-decision.yaml"

echo "== [e2e] extracting placement token and scheduler public key"
TOKEN=$(kubectl get aiplacementdecision "${DECISION}" -n "${NS}" -o jsonpath='{.metadata.annotations.ai\.sovereign\.io/placement-token}')
if [ -z "${TOKEN}" ]; then
  echo "FAIL: placement token annotation missing on decision" >&2
  exit 1
fi
PUBKEY=$(kubectl logs -n "${PLATFORM_NS}" deploy/attestation-scheduler | grep 'signing key ready' | tail -1 | sed -E 's/.*"pubKey":"([0-9a-f]+)".*/\1/')
if [ -z "${PUBKEY}" ]; then
  echo "FAIL: could not read scheduler public key from logs" >&2
  exit 1
fi
echo "${TOKEN}" > "${OUT_DIR}/e2e-placement-token.json"
echo "${PUBKEY}" > "${OUT_DIR}/e2e-scheduler-pubkey.hex"

echo "== [e2e] independent verification with verify-placement"
"${VERIFY_BIN}" -token-file "${OUT_DIR}/e2e-placement-token.json" -pubkey-hex "${PUBKEY}" | tee "${OUT_DIR}/e2e-verify-placement.txt"

echo "== [e2e] PASS — positive scenario complete env=${ENV_NAME} evidenceMode=${EXPECTED_EVIDENCE_MODE}"
